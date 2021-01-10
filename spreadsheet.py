import datetime
import json

import gspread
from oauth2client.service_account import ServiceAccountCredentials


class SpreadSheet:
    def __init__(self, jsontext, sheet_id):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(jsontext), scope)
        gc = gspread.authorize(credentials)
        self._wb = gc.open_by_key(sheet_id)

    @staticmethod
    def dict2ssformat(data):
        convert_data = []
        for d in data:
            amount = abs(d["amount"])
            is_transfer = d["account_from"] and d["account_to"]
            convert_data.append(
                [
                    d["transaction_id"],
                    d["date"].strftime("%Y-%m-%d"),
                    d["content"],
                    amount if d["account_to"] else -amount,
                    d["account_from"] + "," + d["account_to"]
                    if is_transfer
                    else d["account_from"] or d["account_to"],
                    "振替" if is_transfer else d["lcategory"],
                    d["mcategory"],
                    d["memo"],
                ]
            )
        return convert_data

    @staticmethod
    def ssformat2dict(data):
        convert_data = []
        for d in data:
            amount = int(d[3])
            accounts = d[4].split(",")
            is_transfer = d[5] == "振替"
            convert_data.append(
                {
                    "transaction_id": int(d[0]),
                    "date": datetime.date(*map(int, d[1].split("-"))),
                    "content": d[2],
                    "amount": abs(amount),
                    "account_from": accounts[0]
                    if is_transfer
                    else accounts[0]
                    if amount <= 0
                    else None,
                    "account_to": accounts[1]
                    if is_transfer
                    else accounts[0]
                    if amount > 0
                    else None,
                    "lcategory": "" if is_transfer else d[5],
                    "mcategory": d[6],
                    "memo": d[7],
                }
            )
        return convert_data

    def get(self, year, month):
        sname = str(year - 2000).zfill(2) + str(month).zfill(2)
        try:
            ws = self._wb.worksheet(sname)
        except gspread.exceptions.WorksheetNotFound:
            return []
        data = ws.get_all_values()[2:]
        ret = self.ssformat2dict(data)
        ret = sorted(ret, key=lambda x: (x["date"], x["transaction_id"]), reverse=True)
        return ret

    def merge(self, year, month, data, max_num=200):
        convert_data = self.dict2ssformat(data)
        month_b = 12 if month == 1 else month - 1
        year_b = year - 1 if month == 1 else year
        wss = self._wb.worksheets()
        sheet_names = [i.title for i in wss]
        sname = str(year - 2000).zfill(2) + str(month).zfill(2)
        try:
            ws = wss[sheet_names.index(sname)]
        except ValueError:
            snameb = str(year_b - 2000).zfill(2) + str(month_b).zfill(2)
            wsb_index = sheet_names.index(snameb)
            wst_index = sheet_names.index("template")
            ws = self._wb.duplicate_sheet(
                wss[wst_index].id, insert_sheet_index=wsb_index, new_sheet_name=sname
            )
        ws.update("A3", convert_data + [[""] * 8] * (max_num - len(data)))
