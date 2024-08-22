import base64

from googleapiclient.discovery import build


def base64_decode(b64_message):
    message = base64.urlsafe_b64decode(b64_message + "=" * (-len(b64_message) % 4)).decode(
        encoding="utf-8"
    )
    return message


class Gmail:
    def __init__(self, cred):
        self._service = build("gmail", "v1", credentials=cred)

    def get_mail_list(self, query):
        try:
            results = (
                self._service.users()
                .messages()
                .list(userId="me", maxResults=10, q=query)
                .execute()
            )
        except BaseException as err:
            print(f"action=get_mail_list error={err}")
            raise
        messages = results.get("messages", [])
        return messages

    def get_subject_message(self, id):
        # Call the Gmail API
        try:
            res = self._service.users().messages().get(userId="me", id=id).execute()
        except BaseException as err:
            print(f"action=get_message error={err}")
            raise

        result = {}

        subject = [
            d.get("value") for d in res["payload"]["headers"] if d.get("name") == "Subject"
        ][0]
        to = [d.get("value") for d in res["payload"]["headers"] if d.get("name") == "To"][0]
        result["subject"] = subject
        result["to"] = to

        # Such as text/plain
        if "data" in res["payload"]["body"]:
            b64_message = res["payload"]["body"]["data"]
        # Such as text/html
        elif res["payload"]["parts"] is not None:
            b64_message = res["payload"]["parts"][0]["body"]["data"]
        message = base64_decode(b64_message)
        result["message"] = message

        return result
