import io

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


class Drive:
    def __init__(self, cred):
        self._service = build("drive", "v3", credentials=cred)

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
