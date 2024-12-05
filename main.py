import os.path
import random
import time

from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_API_THREADS = 20

SERVICE_ACCOUNT_FILE = "secrets/sandbox-service-account-key.json"
SCOPES = ["https://www.googleapis.com/auth/admin.directory.user.readonly", 
          "https://www.googleapis.com/auth/drive.metadata.readonly",
          "https://www.googleapis.com/auth/drive.readonly"]
DELEGATED_ADMIN_EMAIL = "xxx"
WORKSPACE_CUSTOMER_ID = "xxx"

random.seed(time.time())


def get_credentials(subject):
    return Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES).with_subject(subject)
  
def get_users(admin_creds):
    users = []
    service = build("admin", "directory_v1", credentials=admin_creds)
    request = service.users().list(customer=WORKSPACE_CUSTOMER_ID, maxResults=100, orderBy="email")
    while request is not None:
        response = request.execute()
        users.extend(response.get("users", []))
        request = service.users().list_next(request, response)
    return users

def get_files(drive_service, drive_id=None):
    files = []
    if drive_id:
        return get_files_shared(drive_service, drive_id)
    else:
        return get_files_user(drive_service)

def get_files_user(drive_service):
    files = []
    request = drive_service.files().list(pageSize=100, fields="nextPageToken, files(id, name, md5Checksum, parents, permissions)")
    while request is not None:
        response = request.execute()
        files.extend(response.get("files", []))
        request = drive_service.files().list_next(request, response)
    return files

def get_files_shared(drive_service, drive_id):
    files = []
    known_permissions = {}
    request = drive_service.files().list(pageSize=100, fields="nextPageToken, files(id, name, md5Checksum, parents, permissionIds)", corpora="drive", driveId=drive_id, includeItemsFromAllDrives=True, supportsAllDrives=True)
    while request is not None:
        response = request.execute()
        files.extend(response.get("files", []))
        request = drive_service.files().list_next(request, response)
    for f in files:
        f["permissions"] = []
        if "permissionIds" in f:
            for permission_id in f["permissionIds"]:
                if permission_id in known_permissions:
                    f["permissions"].append(known_permissions[permission_id])
                else:
                    permission = drive_service.permissions().get(fileId=f["id"], permissionId=permission_id, fields="id, displayName, type, kind, emailAddress, role", supportsAllDrives=True).execute()
                    f["permissions"].append(permission)
                    known_permissions[permission_id] = permission
    return files

def get_file_path(drive_service, file_id):
    file_path = []
    print(f"Processing file {file_id}")

    # Start with the file itself
    file = drive_service.files().get(fileId=file_id, fields="name, parents").execute()
    file_path.append(file['name'])

    # Traverse up the folder hierarchy
    parents = file.get('parents', [])
    while parents:
        # Get the first parent (Google Drive files can have multiple parents)
        parent_id = parents[0]
        parent = drive_service.files().get(fileId=parent_id, fields="name, parents").execute()
        file_path.append(parent['name'])
        parents = parent.get('parents', [])

    # Reverse the path to get it from root to the file
    return "/".join(reversed(file_path))

def update_file_path(file, credentials):
    drive_service = build("drive", "v3", credentials=credentials)
    file["path"] = get_file_path(drive_service, file["id"])
    return file

def find_file_by_id(files, file_id):
    for file in files:
        if file["id"] == file_id:
            return file
    return None

def build_file_path(files, file_id):
    file = find_file_by_id(files, file_id)
    if file is None:
        return None
    file_path = [file["name"]]
    while "parents" in file:
        parent_id = file["parents"][0]
        parent = find_file_by_id(files, parent_id)
        if parent is None:
            break
        file_path.append(parent["name"])
        file = parent
    return "/".join(reversed(file_path))

def save_user_file_list(user_email):
    credentials = get_credentials(user_email)
    drive_service = build("drive", "v3", credentials=credentials)
    files = get_files(drive_service)
    print("Files found:", len(files), f"({user_email})")
    save_file_list(f"u_{user_email}", files)
    

def save_shared_file_list(drive_id):
    credentials = get_credentials(DELEGATED_ADMIN_EMAIL)
    drive_service = build("drive", "v3", credentials=credentials)
    files = get_files(drive_service, drive_id=drive_id)
    print("Files found:", len(files), f"({drive_id})")
    save_file_list(f"s_{drive_id}", files)

def save_file_list(file_name, files):
    if not os.path.exists("output"):
        os.makedirs("output")
    for i, f in enumerate(files):
        f["path"] = build_file_path(files, f["id"])
        files[i] = f
    with open(f"output/{file_name}.json", "w") as f:
        f.write(json.dumps(files, indent=4))

def get_shared_drives(admin_creds):
    drives = []
    service = build("drive", "v3", credentials=admin_creds)
    request = service.drives().list(pageSize=100)
    while request is not None:
        response = request.execute()
        drives.extend(response.get("drives", []))
        request = service.drives().list_next(request, response)
    return drives
    

def main():
    admin_creds = get_credentials(DELEGATED_ADMIN_EMAIL)
    users = [user["primaryEmail"] for user in get_users(admin_creds)]
    shared_drives = [drive["id"] for drive in get_shared_drives(admin_creds)]

    with ThreadPoolExecutor(max_workers=MAX_API_THREADS) as executor:
        futures = [executor.submit(save_user_file_list, user) for user in users]
    with ThreadPoolExecutor(max_workers=MAX_API_THREADS) as executor:
        futures = [executor.submit(save_shared_file_list, drive) for drive in shared_drives]

    
        
    

if __name__ == "__main__":
  main()