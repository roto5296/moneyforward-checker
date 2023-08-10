import io
import json

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


class Drive:
    def __init__(self, jsontext):
        creds = Credentials.from_service_account_info(json.loads(jsontext))
        scoped = creds.with_scopes(["https://www.googleapis.com/auth/drive"])
        self._service = build("drive", "v3", credentials=scoped)

    def get(self, file_id):
        request = self._service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        ret = fh.getvalue().decode("UTF-8")
        fh.seek(0)
        return ret
