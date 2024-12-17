from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from src.utils.logger import app_logger as logger


from typing import Literal
import json
import os

thread_local = threading.local()

class GDrive:
    def __init__(self, drive_id: str, credentials: Credentials, type: Literal['user', 'shared']):
        self.drive_id = drive_id
        self.credentials = credentials
        self.type = type
        self.files = []
        self.files_fetched = False

    def __repr__(self) -> str:
        return f"GDrive({self.drive_id}, {self.type})"
    
    def get_drive_service(self):
        if not hasattr(thread_local, "drive_service"):
            thread_local.drive_service = build('drive', 'v3', credentials=self.credentials)
        return thread_local.drive_service


    def get_file_list(self):
        return self.files
    
    def get_file_list_length(self):
        return len(self.files)
    
    def get_drive_id(self):
        return self.drive_id
    
    def fetch_file_path(self, file_id: str, drive_service, supportsAllDrives=False):
        file_path = []
        f = drive_service.files().get(fileId=file_id, fields="id, name, parents", supportsAllDrives=supportsAllDrives).execute()

        parents = f.get('parents', [])
        while parents:
            parent_id = parents[0]
            parent = drive_service.files().get(fileId=parent_id, fields="name, parents", supportsAllDrives=supportsAllDrives).execute()
            #check if parent is a shared drive
            if "parents" not in parent:
                # get shared drive name
                drive = drive_service.drives().get(driveId=parent_id).execute()
                file_path.append(drive['name'])
                break
            file_path.append(parent['name'])
            parents = parent.get('parents', [])

        return "/".join(reversed(file_path))


    def fetch_file_list(self):
        drive_service = build('drive', 'v3', credentials=self.credentials)
        self.files = []

        if self.type == 'user':
            request = drive_service.files().list(pageSize=100, fields="nextPageToken, files(id, name, md5Checksum, parents, mimeType, shortcutDetails, permissions)")
            while request is not None:
                response = request.execute()
                self.files.extend(response.get("files", []))
                request = drive_service.files().list_next(request, response)
        elif self.type == 'shared':
            known_permissions = {}
            request = drive_service.files().list(pageSize=100, fields="nextPageToken, files(id, name, md5Checksum, parents, mimeType, shortcutDetails, permissionIds)", corpora="drive", driveId=self.drive_id, includeItemsFromAllDrives=True, supportsAllDrives=True)
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
                            permission = drive_service.permissions().get(fileId=f["id"], permissionId=permission_id, fields="id, displayName, type, kind, emailAddress, role", supportsAllDrives=True).execute()
                            f["permissions"].append(permission)
                            known_permissions[permission_id] = permission

        for i, f in enumerate(self.files):
            f["path"] = self.build_file_path(f["id"])
            self.files[i] = f
        self.files_fetched = True
        

    def find_file_by_id(self, file_id: str):
        for f in self.files:
            if f["id"] == file_id:
                return f
        return

    def build_file_path(self, file_id: str):
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
                
    def dump_file_list(self, path):
        if not self.files_fetched:
            self.fetch_file_list()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.files, f, indent=4)

    def download_all_files(self, base_path, threads=20):
        if not self.files_fetched:
            self.fetch_file_list()
        if len(self.files) == 0:
            return
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = []
            for f in self.files:
                futures.append(executor.submit(self.download_file, f, base_path))
            for future in as_completed(futures):
                future.result()

    def download_file(self, file, base_path):
        try:
            if 'md5Checksum' in file:
                self.download_binary_file(file, base_path)
            else:
                self.export_file(file, base_path)
        except Exception as e:
            with open(f"{base_path}/errors.txt", "a") as f:
                logger.error(f"Error downloading file {file['name']} ({file['id']}, (drive: {self.drive_id})): {e}")
                f.write(f"Error downloading file {file['name']} ({file['id']}): {e}\n")

    def download_binary_file(self, file, base_path):
        drive_service = self.get_drive_service()
        request = drive_service.files().get_media(fileId=file["id"])
        new_file_path = f"{base_path}/{file['path']}/{file['name']}"
        self.write_request_to_file(request, new_file_path)

    def write_request_to_file(self, request, file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(request.execute())

    def export_file(self, file, base_path):
        if file['mimeType'] == "application/vnd.google-apps.folder":
            return
        
        drive_service = self.get_drive_service()
        new_file_path = f"{base_path}/{file['path']}/{file['name']}"
        
        match file['mimeType']:
            case "application/vnd.google-apps.shortcut":
                original_file = file['shortcutDetails']['targetId']
                original_file_path = self.fetch_file_path(original_file, drive_service, supportsAllDrives=True)
                with open(f"{base_path}/{file['path']}/{file['name']}.lnk.txt", "w") as f:
                    f.write(original_file_path)
            case "application/vnd.google-apps.document":
                request = drive_service.files().export_media(fileId=file['id'], mimeType="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
                self.write_request_to_file(request, f"{new_file_path}.docx")
            case "application/vnd.google-apps.spreadsheet":
                request = drive_service.files().export_media(fileId=file['id'], mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                self.write_request_to_file(request, f"{new_file_path}.xlsx")
            case "application/vnd.google-apps.presentation":
                request = drive_service.files().export_media(fileId=file['id'], mimeType="application/vnd.openxmlformats-officedocument.presentationml.presentation")
                self.write_request_to_file(request, f"{new_file_path}.pptx")
            case "application/vnd.google-apps.drawing":
                request = drive_service.files().export_media(fileId=file['id'], mimeType="application/pdf")
                self.write_request_to_file(request, f"{new_file_path}.pdf")
            case "application/vnd.google-apps.script":
                request = drive_service.files().export_media(fileId=file['id'], mimeType="application/vnd.google-apps.script+json")
                self.write_request_to_file(request, f"{new_file_path}.json")
            case _:
                with open(f"{base_path}/errors.txt", "a") as f:
                    logger.warning(f"Unknown file type: {file['mimeType']} ({file['id']})")
                    f.write(f"Unknown file type: {file['mimeType']} ({file['id']})\n")