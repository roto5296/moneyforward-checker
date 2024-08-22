import codecs
import pickle

from google.auth.exceptions import GoogleAuthError
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def authenticate(cred_str=None):
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if cred_str:
        creds = pickle.loads(codecs.decode(cred_str.encode(), "base64"))
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        try:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file("client_id.json", SCOPES)
                creds = flow.run_local_server(port=0)
        except GoogleAuthError as err:
            print(f"action=authenticate error={err}")
            raise

        # Save the credentials for the next run
        print("please save below")
        print(codecs.encode(pickle.dumps(creds), "base64").decode())

    return creds


# authenticate(SCOPES)
