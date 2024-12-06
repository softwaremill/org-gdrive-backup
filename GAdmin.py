from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


class GAdmin:
    def __init__(self, workspace_customer_id: str, credentials: Credentials):
        self.workspace_customer_id = workspace_customer_id
        self.credentials = credentials
        self.users_fetched = False
        self.shared_drives_fetched = False

    def fetch_shared_drives(self):
        self.shared_drives = []
        service = build("drive", "v3", credentials=self.credentials)
        request = service.drives().list()
        while request is not None:
            response = request.execute()
            self.shared_drives.extend(response.get("drives", []))
            request = service.drives().list_next(request, response)

    def fetch_user_list(self):
        self.users = []
        service = build("admin", "directory_v1", credentials=self.credentials)
        request = service.users().list(customer=self.workspace_customer_id, maxResults=100, orderBy="email")
        while request is not None:
            response = request.execute()
            self.users.extend(response.get("users", []))
            request = service.users().list_next(request, response)

    def get_user_list(self):
        if not self.users_fetched:
            self.fetch_user_list()
        return self.users
    
    def get_shared_drives(self):
        if not self.shared_drives_fetched:
            self.fetch_shared_drives()
        return self.shared_drives
        