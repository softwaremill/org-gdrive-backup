from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from ..aws.s3 import S3
from src.utils.logger import app_logger as logger
from enum import Enum
from typing import Optional, Dict, Any, TypeAlias
import requests

import json
import os

thread_local = threading.local()

DriveService: TypeAlias = Any
GFile: TypeAlias = Dict[str, Any]


class DRIVE_TYPE(Enum):
    USER = "user"
    SHARED = "shared"


class GDrive:
    def __init__(
        self,
        drive_id: str,
        credentials: Credentials,
        drive_type: DRIVE_TYPE,
        include_shared_with_me: bool = True,
        jit_s3_upload: bool = False,
        s3_role_based_access: bool = False,
        s3_bucket_name: str = None,
        s3_access_key: str = None,
        s3_secret_key: str = None,
    ) -> None:
        self.drive_id = drive_id
        self.credentials = credentials
        self.drive_type = drive_type
        self.include_shared_with_me = include_shared_with_me
        self._files = {}
        self._files_fetched = False
        self._file_export_handlers = None
        self.jit_s3_upload = jit_s3_upload
        self.s3_role_based_access = s3_role_based_access
        self.s3_bucket_name = s3_bucket_name
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self._lock = None
        self._locked_files = None

    @property
    def files(self):
        if self._files is None:
            self._files = {}
        return self._files

    @property
    def locked_files(self):
        if self._locked_files is None:
            self._locked_files = set()
        return self._locked_files

    @property
    def file_export_handlers(self):
        if self._file_export_handlers is None:
            self._file_export_handlers = {
                "application/vnd.google-apps.shortcut": self._handle_shortcut_export,
                "application/vnd.google-apps.document": self._handle_document_export,
                "application/vnd.google-apps.spreadsheet": self._handle_spreadsheet_export,
                "application/vnd.google-apps.presentation": self._handle_presentation_export,
                "application/vnd.google-apps.drawing": self._handle_drawing_export,
                "application/vnd.google-apps.script": self._handle_script_export,
                "application/vnd.google-apps.form": self._handle_zip_export,
            }
        return self._file_export_handlers

    @property
    def lock(self):
        if self._lock is None:
            self._lock = threading.Lock()
        return self._lock

    def __repr__(self) -> str:
        return f"GDrive({self.drive_id}, {self.drive_type})"

    def _get_drive_service(self) -> DriveService:
        if not hasattr(thread_local, "drive_service"):
            thread_local.drive_service = build(
                "drive", "v3", credentials=self.credentials
            )
        return thread_local.drive_service

    def _get_s3_service(self) -> S3:
        if not hasattr(thread_local, "s3"):
            if self.s3_role_based_access:
                s3 = S3(self.s3_bucket_name, None, None, role_based=True)
            else:
                s3 = S3(
                    self.s3_bucket_name,
                    self.s3_access_key,
                    self.s3_secret_key,
                )
            thread_local.s3 = s3
        return thread_local.s3

    def _get_auth_session(self) -> requests.Session:
        if not hasattr(thread_local, "auth_session"):
            thread_local.auth_session = requests.Session()
            thread_local.auth_session.headers.update(
                {"Authorization": f"Bearer {self.credentials.token}"}
            )
        return thread_local.auth_session

    def fetch_file_path(
        self, file_id: str, drive_service: DriveService, supportsAllDrives: bool = False
    ) -> str:
        file_path = []
        f = (
            drive_service.files()
            .get(
                fileId=file_id,
                fields="id, name, parents",
                supportsAllDrives=supportsAllDrives,
            )
            .execute()
        )

        parents = f.get("parents", [])
        while parents:
            parent_id = parents[0]
            parent = (
                drive_service.files()
                .get(
                    fileId=parent_id,
                    fields="name, parents",
                    supportsAllDrives=supportsAllDrives,
                )
                .execute()
            )
            # check if parent is a shared drive
            if "parents" not in parent:
                # get shared drive name
                drive = drive_service.drives().get(driveId=parent_id).execute()
                file_path.append(drive["name"])
                break
            file_path.append(parent["name"])
            parents = parent.get("parents", [])

        return "/".join(reversed(file_path))

    def fetch_file_list(self, page_size: int = 1000) -> None:
        drive_service = build("drive", "v3", credentials=self.credentials)
        self.files.clear()

        if self.drive_type == DRIVE_TYPE.USER:
            self._fetch_file_list_user_drive(drive_service, page_size)
        elif self.drive_type == DRIVE_TYPE.SHARED:
            self._fetch_file_list_shared_drive(drive_service, page_size)

        self._files_fetched = True

        # Update paths after collecting all files
        for file_id, file in self.files.items():
            file["path"] = self.build_file_path(file_id)

    def _fetch_file_list_user_drive(
        self, drive_service: DriveService, page_size: int
    ) -> None:
        fields = "nextPageToken, files(id, name, md5Checksum, parents, mimeType, shortcutDetails, permissions, exportLinks)"
        if self.include_shared_with_me:
            request = drive_service.files().list(
                pageSize=page_size,
                fields=fields,
            )
        else:
            request = drive_service.files().list(
                pageSize=page_size,
                fields=fields,
                q="'me' in owners",
            )
        while request is not None:
            response = request.execute()
            for file in response.get("files", []):
                self.files[file["id"]] = file
            request = drive_service.files().list_next(request, response)

    def _fetch_file_list_shared_drive(
        self, drive_service: DriveService, page_size: int
    ) -> None:
        known_permissions = {}
        request = drive_service.files().list(
            pageSize=page_size,
            fields="nextPageToken, files(id, name, md5Checksum, parents, mimeType, shortcutDetails, permissionIds, exportLinks)",
            corpora="drive",
            driveId=self.drive_id,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        )
        while request is not None:
            response = request.execute()
            for file in response.get("files", []):
                self.files[file["id"]] = file
            request = drive_service.files().list_next(request, response)
        for file in self.files.values():
            file["permissions"] = []
            if "permissionIds" in file:
                for permission_id in file["permissionIds"]:
                    if permission_id in known_permissions:
                        file["permissions"].append(known_permissions[permission_id])
                    else:
                        permission = (
                            drive_service.permissions()
                            .get(
                                fileId=file["id"],
                                permissionId=permission_id,
                                fields="id, displayName, type, kind, emailAddress, role",
                                supportsAllDrives=True,
                            )
                            .execute()
                        )
                        file["permissions"].append(permission)
                        known_permissions[permission_id] = permission

    def find_file_by_id(self, file_id: str) -> Optional[GFile]:
        if not self._files_fetched:
            self.fetch_file_list()
        return self.files.get(file_id)

    def build_file_path(self, file_id: str) -> Optional[str]:
        f = self.find_file_by_id(file_id)
        if f is None:
            return None
        file_path = []
        while "parents" in f:
            parent_id = f["parents"][0]
            parent = self.find_file_by_id(parent_id)
            if parent is None:
                break
            file_path.append(parent["name"])
            f = parent
        return "/".join(reversed(file_path))

    def dump_file_list(self, path: str) -> None:
        if not self._files_fetched:
            self.fetch_file_list()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.files, f, indent=4)

    def download_all_files(self, base_path: str, threads: int = 20) -> None:
        if not self._files_fetched:
            self.fetch_file_list()
        if len(self.files) == 0:
            return
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = []
            for f in self.files.values():
                futures.append(executor.submit(self.download_file, f, base_path))
            for future in as_completed(futures):
                future.result()
                futures.remove(future)
                files_remaining = len(futures)
                if files_remaining % 100 == 0 and files_remaining > 0:
                    logger.info(f"({self.drive_id}) Files remaining: {len(futures)}")

    def _is_cannot_download_error(self, error: Exception) -> bool:
        return (
            isinstance(error, Exception)
            and hasattr(error, "resp")
            and hasattr(error.resp, "status")
            and error.resp.status == 403
            and "This file cannot be downloaded by the user" in str(error)
        )

    def download_file(
        self,
        file: GFile,
        base_path: str,
    ) -> None:
        saved_file_path = None
        try:
            if "md5Checksum" in file:
                saved_file_path = self.download_binary_file(file, base_path)
            else:
                saved_file_path = self.export_file(file, base_path)
        except Exception as e:
            if self._is_cannot_download_error(e):
                logger.warning(
                    f"Skipping file \"{file['name']}\" ({file['id']}) - no download permission"
                )
                os.makedirs(
                    os.path.dirname(f"{base_path}/permission_errors.txt"), exist_ok=True
                )
                with open(f"{base_path}/permission_errors.txt", "a") as f:
                    f.write(f"\"{file['name']}\" | ({file['id']})\n")
                return

            os.makedirs(os.path.dirname(f"{base_path}/errors.txt"), exist_ok=True)
            with open(f"{base_path}/errors.txt", "a") as f:
                logger.error(
                    f"Error downloading file \"{file['name']}\" ({file['id']}, (drive: {self.drive_id})): {e}"
                )
                f.write(
                    f"Error downloading file \"{file['name']}\" ({file['id']}): {e}\n"
                )

        if self.jit_s3_upload and saved_file_path is not None:
            try:
                s3 = self._get_s3_service()
                destination_path = "/".join(saved_file_path.split("/")[1:])
                s3.upload_file(saved_file_path, destination_path)
                logger.trace(f"Removing file: {saved_file_path}")
                os.remove(saved_file_path)
            except Exception as e:
                os.makedirs(os.path.dirname(f"{base_path}/errors.txt"), exist_ok=True)
                with open(f"{base_path}/errors.txt", "a") as f:
                    logger.error(f'Error uploading file "{saved_file_path}" to S3: {e}')
                    f.write(f'Error uploading file "{saved_file_path}" to S3: {e}\n')

    def download_binary_file(self, file: GFile, base_path: str) -> str:
        drive_service = self._get_drive_service()
        request = drive_service.files().get_media(fileId=file["id"])
        if file["path"] == "":
            new_file_path = f"{base_path}/{file['name']}"
        else:
            new_file_path = f"{base_path}/{file['path']}/{file['name']}"
        saved_file_path = self.write_request_to_file(file["id"], request, new_file_path)
        return saved_file_path

    def export_file(self, file: GFile, base_path: str) -> str:
        if file["mimeType"] == "application/vnd.google-apps.folder":
            return

        drive_service = self._get_drive_service()
        if file["path"] == "":
            new_file_path = f"{base_path}/{file['name']}"
        else:
            new_file_path = f"{base_path}/{file['path']}/{file['name']}"

        export_handler = self.file_export_handlers.get(file["mimeType"], None)
        if export_handler is not None:
            saved_file_path = export_handler(file, drive_service, new_file_path)
        else:
            os.makedirs(os.path.dirname(f"{base_path}/errors.txt"), exist_ok=True)
            with open(f"{base_path}/errors.txt", "a") as f:
                logger.warning(f"Unknown file type: {file['mimeType']} ({file['id']})")
                f.write(f"Unknown file type: {file['mimeType']} ({file['id']})\n")
                return None
        return saved_file_path

    def _get_available_path_and_lock_it(self, fileId: str, file_path: str) -> str:
        base_path = os.path.dirname(file_path)
        basename = os.path.basename(file_path)
        name, ext = os.path.splitext(basename)
        path_format = os.path.join(base_path, f"{name}_%s{ext}")

        # Try original name first
        new_path = os.path.join(base_path, basename)

        counter = 1
        while True:
            with self.lock:
                if new_path not in self.locked_files and not os.path.exists(new_path):
                    self.locked_files.add(new_path)
                    logger.trace(f"Locked new path: {new_path}")
                    return new_path

            new_path = path_format % f"{fileId[:5]}_{counter}"
            counter += 1

    def _unlock_file_path(self, file_path: str) -> None:
        with self.lock:
            self.locked_files.remove(file_path)
            logger.trace(f"Unlocked path: {file_path}")

    def download_via_export_link(
        self, fileId: str, export_link: str, new_file_path: str
    ) -> str:
        auth_session = self._get_auth_session()
        os.makedirs(os.path.dirname(new_file_path), exist_ok=True)
        new_file_path = self._get_available_path_and_lock_it(fileId, new_file_path)
        with open(new_file_path, "wb") as f:
            response = auth_session.get(export_link, stream=True)
            for chunk in response.iter_content(chunk_size=104857600):  # 100MB
                if chunk:
                    f.write(chunk)
        return new_file_path

    def write_request_to_file(self, fileId: str, request: Any, file_path: str) -> None:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        logger.debug(f"Downloading file: {file_path}")

        file_path = self._get_available_path_and_lock_it(fileId, file_path)

        with open(file_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        return file_path

    def _handle_shortcut_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> str:
        original_file = file["shortcutDetails"]["targetId"]
        original_file_path = self.fetch_file_path(
            original_file, drive_service, supportsAllDrives=True
        )
        os.makedirs(os.path.dirname(new_file_path), exist_ok=True)
        with open(f"{new_file_path}.lnk.txt", "w") as f:
            f.write(original_file_path)
        return f"{new_file_path}.lnk.txt"

    def _handle_document_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> str:
        desired_mimetype = (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )
        try:
            request = drive_service.files().export_media(
                fileId=file["id"],
                mimeType=desired_mimetype,
            )
            saved_file_path = self.write_request_to_file(
                file["id"], request, f"{new_file_path}.docx"
            )
        except Exception:
            export_link = file["exportLinks"][desired_mimetype]
            saved_file_path = self.download_via_export_link(
                file["id"], export_link, f"{new_file_path}.docx"
            )
        return saved_file_path

    def _handle_spreadsheet_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> str:
        desired_mimetype = (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        try:
            request = drive_service.files().export_media(
                fileId=file["id"],
                mimeType=desired_mimetype,
            )
            saved_file_path = self.write_request_to_file(
                file["id"], request, f"{new_file_path}.xlsx"
            )
        except Exception:
            export_link = file["exportLinks"][desired_mimetype]
            saved_file_path = self.download_via_export_link(
                file["id"], export_link, f"{new_file_path}.xlsx"
            )
        return saved_file_path

    def _handle_presentation_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> str:
        desired_mimetype = (
            "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        )
        try:
            request = drive_service.files().export_media(
                fileId=file["id"],
                mimeType=desired_mimetype,
            )
            saved_file_path = self.write_request_to_file(
                file["id"], request, f"{new_file_path}.pptx"
            )
        except Exception:
            export_link = file["exportLinks"][desired_mimetype]
            saved_file_path = self.download_via_export_link(
                file["id"], export_link, f"{new_file_path}.pptx"
            )
        return saved_file_path

    def _handle_drawing_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> str:
        desired_mimetype = "application/pdf"
        try:
            request = drive_service.files().export_media(
                fileId=file["id"],
                mimeType=desired_mimetype,
            )
            saved_file_path = self.write_request_to_file(
                file["id"], request, f"{new_file_path}.pdf"
            )
        except Exception:
            export_link = file["exportLinks"][desired_mimetype]
            saved_file_path = self.download_via_export_link(
                file["id"], export_link, f"{new_file_path}.pdf"
            )
        return saved_file_path

    def _handle_script_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> str:
        desired_mimetype = "application/vnd.google-apps.script+json"
        try:
            request = drive_service.files().export_media(
                fileId=file["id"],
                mimeType=desired_mimetype,
            )
            saved_file_path = self.write_request_to_file(
                file["id"], request, f"{new_file_path}.json"
            )
        except Exception:
            export_link = file["exportLinks"][desired_mimetype]
            saved_file_path = self.download_via_export_link(
                file["id"], export_link, f"{new_file_path}.json"
            )
        return saved_file_path

    def _handle_zip_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> str:
        desired_mimetype = "application/zip"
        try:
            request = drive_service.files().export_media(
                fileId=file["id"],
                mimeType=desired_mimetype,
            )
            saved_file_path = self.write_request_to_file(
                file["id"], request, f"{new_file_path}.zip"
            )
        except Exception:
            export_link = file["exportLinks"][desired_mimetype]
            saved_file_path = self.download_via_export_link(
                file["id"], export_link, f"{new_file_path}.zip"
            )
        return saved_file_path
