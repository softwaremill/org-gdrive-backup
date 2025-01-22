from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from src.utils.logger import app_logger as logger
from enum import Enum
from typing import Optional, Dict, Any, TypeAlias

import json
import os

thread_local = threading.local()

DriveService: TypeAlias = Any
GFile: TypeAlias = Dict[str, Any]


class DRIVE_TYPE(Enum):
    USER = "user"
    SHARED = "shared"


class GDrive:
    def __init__(self, drive_id: str, credentials: Credentials, drive_type: DRIVE_TYPE):
        self.drive_id = drive_id
        self.credentials = credentials
        self.drive_type = drive_type
        self.files = []
        self._files_fetched = False
        self._file_export_handlers = {
            "application/vnd.google-apps.shortcut": self._handle_shortcut_export,
            "application/vnd.google-apps.document": self._handle_document_export,
            "application/vnd.google-apps.spreadsheet": self._handle_spreadsheet_export,
            "application/vnd.google-apps.presentation": self._handle_presentation_export,
            "application/vnd.google-apps.drawing": self._handle_drawing_export,
            "application/vnd.google-apps.script": self._handle_script_export,
            "application/vnd.google-apps.form": self._handle_zip_export,
        }

    def __repr__(self) -> str:
        return f"GDrive({self.drive_id}, {self.drive_type})"

    def _get_drive_service(self) -> DriveService:
        if not hasattr(thread_local, "drive_service"):
            thread_local.drive_service = build(
                "drive", "v3", credentials=self.credentials
            )
        return thread_local.drive_service

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

        for i, f in enumerate(self.files):
            f["path"] = self.build_file_path(f["id"])
            self.files[i] = f

    def _fetch_file_list_user_drive(
        self, drive_service: DriveService, page_size: int
    ) -> None:
        request = drive_service.files().list(
            pageSize=page_size,
            fields="nextPageToken, files(id, name, md5Checksum, parents, mimeType, shortcutDetails, permissions)",
        )
        while request is not None:
            response = request.execute()
            self.files.extend(response.get("files", []))
            request = drive_service.files().list_next(request, response)

    def _fetch_file_list_shared_drive(
        self, drive_service: DriveService, page_size: int
    ) -> None:
        known_permissions = {}
        request = drive_service.files().list(
            pageSize=page_size,
            fields="nextPageToken, files(id, name, md5Checksum, parents, mimeType, shortcutDetails, permissionIds)",
            corpora="drive",
            driveId=self.drive_id,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        )
        while request is not None:
            response = request.execute()
            self.files.extend(response.get("files", []))
            request = drive_service.files().list_next(request, response)
        for f in self.files:
            f["permissions"] = []
            if "permissionIds" in f:
                for permission_id in f["permissionIds"]:
                    if permission_id in known_permissions:
                        f["permissions"].append(known_permissions[permission_id])
                    else:
                        permission = (
                            drive_service.permissions()
                            .get(
                                fileId=f["id"],
                                permissionId=permission_id,
                                fields="id, displayName, type, kind, emailAddress, role",
                                supportsAllDrives=True,
                            )
                            .execute()
                        )
                        f["permissions"].append(permission)
                        known_permissions[permission_id] = permission

    def find_file_by_id(self, file_id: str) -> Optional[GFile]:
        if not self._files_fetched:
            self.fetch_file_list()
        for f in self.files:
            if f["id"] == file_id:
                return f
        return

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
            for f in self.files:
                futures.append(executor.submit(self.download_file, f, base_path))
            for future in as_completed(futures):
                future.result()
                futures.remove(future)
                files_remaining = len(futures)
                if files_remaining % 100 == 0 and files_remaining > 0:
                    logger.info(f"({self.drive_id}) Files remaining: {len(futures)}")

    def download_file(self, file: GFile, base_path: str) -> None:
        try:
            if "md5Checksum" in file:
                self.download_binary_file(file, base_path)
            else:
                self.export_file(file, base_path)
        except Exception as e:
            os.makedirs(os.path.dirname(f"{base_path}/errors.txt"), exist_ok=True)
            with open(f"{base_path}/errors.txt", "a") as f:
                logger.error(
                    f"Error downloading file {file['name']} ({file['id']}, (drive: {self.drive_id})): {e}"
                )
                f.write(f"Error downloading file {file['name']} ({file['id']}): {e}\n")

    def download_binary_file(self, file: GFile, base_path: str) -> None:
        drive_service = self._get_drive_service()
        request = drive_service.files().get_media(fileId=file["id"])
        new_file_path = f"{base_path}/{file['path']}/{file['name']}"
        self.write_request_to_file(request, new_file_path)

    def export_file(self, file: GFile, base_path: str) -> None:
        if file["mimeType"] == "application/vnd.google-apps.folder":
            return

        drive_service = self._get_drive_service()
        new_file_path = f"{base_path}/{file['path']}/{file['name']}"

        export_handler = self._file_export_handlers.get(file["mimeType"], None)
        if export_handler is not None:
            export_handler(file, drive_service, new_file_path)
        else:
            with open(f"{base_path}/errors.txt", "a") as f:
                logger.warning(f"Unknown file type: {file['mimeType']} ({file['id']})")
                f.write(f"Unknown file type: {file['mimeType']} ({file['id']})\n")

    def write_request_to_file(self, request: Any, file_path: str) -> None:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

    def _handle_shortcut_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> None:
        original_file = file["shortcutDetails"]["targetId"]
        original_file_path = self.fetch_file_path(
            original_file, drive_service, supportsAllDrives=True
        )
        with open(f"{new_file_path}.lnk.txt", "w") as f:
            f.write(original_file_path)

    def _handle_document_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> None:
        request = drive_service.files().export_media(
            fileId=file["id"],
            mimeType="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        self.write_request_to_file(request, f"{new_file_path}.docx")

    def _handle_spreadsheet_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> None:
        request = drive_service.files().export_media(
            fileId=file["id"],
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        self.write_request_to_file(request, f"{new_file_path}.xlsx")

    def _handle_presentation_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> None:
        request = drive_service.files().export_media(
            fileId=file["id"],
            mimeType="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        )
        self.write_request_to_file(request, f"{new_file_path}.pptx")

    def _handle_drawing_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> None:
        request = drive_service.files().export_media(
            fileId=file["id"], mimeType="application/pdf"
        )
        self.write_request_to_file(request, f"{new_file_path}.pdf")

    def _handle_script_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> None:
        request = drive_service.files().export_media(
            fileId=file["id"], mimeType="application/vnd.google-apps.script+json"
        )
        self.write_request_to_file(request, f"{new_file_path}.json")

    def _handle_zip_export(
        self, file: GFile, drive_service: DriveService, new_file_path: str
    ) -> None:
        request = drive_service.files().export_media(
            fileId=file["id"], mimeType="application/zip"
        )
        self.write_request_to_file(request, f"{new_file_path}.zip")
