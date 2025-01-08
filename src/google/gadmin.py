from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from functools import cache


class GAdmin:
    def __init__(self, workspace_customer_id: str, credentials: Credentials):
        self.users = []
        self.shared_drives = []
        self.workspace_customer_id = workspace_customer_id
        self.credentials = credentials

    def _fetch_shared_drives(self):
        service = build("drive", "v3", credentials=self.credentials)
        request = service.drives().list()
        while request is not None:
            response = request.execute()
            self.shared_drives.extend(response.get("drives", []))
            request = service.drives().list_next(request, response)
        return self.shared_drives

    def _fetch_user_list(self, page_size=100, order_by="email"):
        service = build("admin", "directory_v1", credentials=self.credentials)
        request = service.users().list(
            customer=self.workspace_customer_id, maxResults=page_size, orderBy=order_by
        )
        while request is not None:
            response = request.execute()
            self.users.extend(response.get("users", []))
            request = service.users().list_next(request, response)
        return self.users

    @cache
    def get_user_list(self):
        return self._fetch_user_list()

    @cache
    def get_shared_drives(self):
        return self._fetch_shared_drives()
