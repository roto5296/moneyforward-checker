import datetime
import json

import gspread
import gspread_asyncio
from google.oauth2.service_account import Credentials
from mfscraping_asyncio import Account2str, MFTransaction, is_Account, str2Account


class SpreadSheet:
    def __init__(self, jsontext: str, sheet_id: str) -> None:
        creds = Credentials.from_service_account_info(json.loads(jsontext))
        self._scoped = creds.with_scopes(
            ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        )
        self._agcm = gspread_asyncio.AsyncioGspreadClientManager(lambda: self._scoped)
        self._sheet_id = sheet_id

    async def login(self) -> None:
        agc = await self._agcm.authorize()
        self._wb = await agc.open_by_key(self._sheet_id)

    @staticmethod
    def mf2ssformat(data: list[MFTransaction]) -> list[list[str | int]]:
        convert_data = []
        for d in data:
            convert_data.append(
                [
                    str(d.transaction_id),
                    d.date.strftime("%Y-%m-%d"),
                    d.content,
                    d.amount,
                    Account2str(d.account_from()) + "," + Account2str(d.account_to())
                    if d.is_transfer()
                    else Account2str(d.account)
                    if is_Account(d.account)
                    else "",
                    "振替" if d.is_transfer() else d.lcategory,
                    d.mcategory,
                    d.memo,
                ]
            )
        return convert_data

    @staticmethod
    def ssformat2mf(data: list[list[str]]) -> list[MFTransaction]:
        convert_data = []
        for d in data:
            accounts = d[4].split(",")
            tmp = (
                (str2Account(accounts[0]), str2Account(accounts[1]))
                if len(accounts) == 2
                else str2Account(accounts[0])
            )
            is_transfer = d[5] == "振替"
            convert_data.append(
                MFTransaction(
                    int(d[0]),
                    datetime.date(*map(int, d[1].split("-"))),
                    int(d[3]),
                    tmp,
                    "" if is_transfer else d[5],
                    d[6],
                    d[2],
                    d[7],
                )
            )
        return convert_data

    async def get(self, year: int, month: int) -> list[MFTransaction]:
        sname = str(year - 2000).zfill(2) + str(month).zfill(2)
        try:
            ws = await self._wb.worksheet(sname)
        except gspread.exceptions.WorksheetNotFound:
            return []
        data = await ws.get_all_values()
        ret = self.ssformat2mf(data[2:])
        ret = sorted(ret, reverse=True)
        return ret

    async def merge(
        self, year: int, month: int, data: list[MFTransaction], max_num: int = 200
    ) -> None:
        convert_data = self.mf2ssformat(data)
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
                wss[wst_index].id,
                insert_sheet_index=wsb_index,
                new_sheet_name=sname,  # type:ignore
            )
        await ws.update("A3", convert_data + [[""] * 8] * max((max_num - len(data), 0)))
