import datetime
import json

import gspread
import gspread_asyncio
from google.oauth2.service_account import Credentials


class SpreadSheet:
    def __init__(self, jsontext, sheet_id):
        creds = Credentials.from_service_account_info(json.loads(jsontext))
        self._scoped = creds.with_scopes(
            ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        )
        self._agcm = gspread_asyncio.AsyncioGspreadClientManager(lambda: self._scoped)
        self._sheet_id = sheet_id

    async def login(self):
        agc = await self._agcm.authorize()
        self._wb = await agc.open_by_key(self._sheet_id)

    @staticmethod
    def dict2ssformat(data):
        convert_data = []
        for d in data:
            convert_data.append(
                [
                    str(d["transaction_id"]),
                    d["date"].strftime("%Y-%m-%d"),
                    d["content"],
                    d["amount"],
                    d["account"][0] + "," + d["account"][1] if d["is_transfer"] else d["account"],
                    "振替" if d["is_transfer"] else d["lcategory"],
                    d["mcategory"],
                    d["memo"],
                ]
            )
        return convert_data

    @staticmethod
    def ssformat2dict(data):
        convert_data = []
        for d in data:
            accounts = d[4].split(",")
            is_transfer = d[5] == "振替"
            convert_data.append(
                {
                    "transaction_id": int(d[0]),
                    "date": datetime.date(*map(int, d[1].split("-"))),
                    "content": d[2],
                    "amount": int(d[3]),
                    "account": accounts if is_transfer else accounts[0],
                    "lcategory": "" if is_transfer else d[5],
                    "mcategory": d[6],
                    "memo": d[7],
                    "is_transfer": is_transfer,
                }
            )
        return convert_data

    async def get(self, year, month):
        sname = str(year - 2000).zfill(2) + str(month).zfill(2)
        try:
            ws = await self._wb.worksheet(sname)
        except gspread.exceptions.WorksheetNotFound:
            return []
        data = await ws.get_all_values()
        ret = self.ssformat2dict(data[2:])
        ret = sorted(ret, key=lambda x: (x["date"], x["transaction_id"]), reverse=True)
        return ret

    async def merge(self, year, month, data, max_num=200):
        convert_data = self.dict2ssformat(data)
        month_b = 12 if month == 1 else month - 1
        year_b = year - 1 if month == 1 else year
        wss = await self._wb.worksheets()
        sheet_names = [i.title for i in wss]
        sname = str(year - 2000).zfill(2) + str(month).zfill(2)
        try:
            ws = wss[sheet_names.index(sname)]
        except ValueError:
            snameb = str(year_b - 2000).zfill(2) + str(month_b).zfill(2)
            wsb_index = sheet_names.index(snameb)
            wst_index = sheet_names.index("template")
            ws = await self._wb.duplicate_sheet(
                wss[wst_index].id, insert_sheet_index=wsb_index, new_sheet_name=sname
            )
        await ws.update("A3", convert_data + [[""] * 8] * (max_num - len(data)))
