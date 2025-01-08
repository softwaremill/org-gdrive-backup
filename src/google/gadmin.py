from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


class GAdmin:
    def __init__(self, workspace_customer_id: str, credentials: Credentials):
        self._users_fetched = False
        self._shared_drives_fetched = False

        self.users = []
        self.shared_drives = []
        self.workspace_customer_id = workspace_customer_id
        self.credentials = credentials

    def _fetch_shared_drives(self):
        self.shared_drives.clear()
        service = build("drive", "v3", credentials=self.credentials)
        request = service.drives().list()
        while request is not None:
            response = request.execute()
            self.shared_drives.extend(response.get("drives", []))
            request = service.drives().list_next(request, response)

    def _fetch_user_list(self, page_size=100, order_by="email"):
        self.users.clear()
        service = build("admin", "directory_v1", credentials=self.credentials)
        request = service.users().list(
            customer=self.workspace_customer_id, maxResults=page_size, orderBy=order_by
        )
        while request is not None:
            response = request.execute()
            self.users.extend(response.get("users", []))
            request = service.users().list_next(request, response)
        self._users_fetched = True

    def get_user_list(self):
        if not self._users_fetched:
            self._fetch_user_list()
        return self.users

    def get_shared_drives(self):
        if not self._shared_drives_fetched:
            self._fetch_shared_drives()
        return self.shared_drives
