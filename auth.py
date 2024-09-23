import codecs
import pickle

from google.auth.exceptions import GoogleAuthError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# from parameter import get_parameter

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]
TOKEN_URI = "https://accounts.google.com/o/oauth2/token"


def authenticate(cred_str, refresh_token=None):
    creds = None
    new_cred_str = cred_str
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if cred_str:
        creds = pickle.loads(codecs.decode(cred_str.encode(), "base64"))
    # creds = Credentials(
    #     get_parameter("GOOGLEAPI_ACCESS_TOKEN"),
    #     refresh_token=refresh_token,
    #     client_id=get_parameter("GOOGLEAPI_CLIENT_ID"),
    #     client_secret=get_parameter("GOOGLEAPI_CLIENT_SECRET"),
    #     token_uri=TOKEN_URI,
    # )
    if creds is None:
        raise
    if refresh_token:
        creds = Credentials(
            creds.token,
            refresh_token=refresh_token,
            client_id=creds.client_id,
            client_secret=creds.client_secret,
            token_uri=creds.token_uri,
            expiry=creds.expiry,
        )
    # If there are no (valid) credentials available, let the user log in.
    if not creds.valid:
        if creds.refresh_token:
            try:
                creds.refresh(Request())
            except GoogleAuthError as err:
                print(f"action=authenticate error={err}")
                raise
        else:
            raise

    # Save the credentials for the next run
    new_cred_str = codecs.encode(pickle.dumps(creds), "base64").decode()
    print(new_cred_str)

    return creds, new_cred_str


# authenticate(SCOPES)
