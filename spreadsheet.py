import asyncio
import datetime
import json

import gspread
import gspread_asyncio
from google.oauth2.service_account import Credentials
from mfscraping_asyncio import Account, Account2str, MFTransaction, is_Account, str2Account


class SpreadSheet:
    def __init__(self, jsontext: str, sheet_id: str) -> None:
        creds = Credentials.from_service_account_info(json.loads(jsontext))
        self._scoped = creds.with_scopes(
            ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        )
        self._agcm = gspread_asyncio.AsyncioGspreadClientManager(lambda: self._scoped)
        self._sheet_id = sheet_id
        self._withdrawal = None

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
                    (
                        Account2str(d.account_from()) + "," + Account2str(d.account_to())
                        if d.is_transfer()
                        else Account2str(d.account) if is_Account(d.account) else ""
                    ),
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

    async def get_withdrawal(
        self,
    ) -> tuple[
        dict[tuple[int, int, Account], dict[str, int | datetime.date]], list[str], list[Account]
    ]:
        async def inner_get_withdrawal(
            self: SpreadSheet,
        ) -> tuple[
            dict[tuple[int, int, Account], dict[str, int | datetime.date]],
            list[str],
            list[Account],
        ]:
            try:
                ws = await self._wb.worksheet("引き落とし")
            except gspread.exceptions.WorksheetNotFound:
                return ({}, [], [])
            data = await ws.get_all_values()
            ret = {}
            col = [str2Account(data[0][i]) for i in range(1, len(data[0]), 2)]
            row = []
            for d in data[1:]:
                date_str = d[0]
                row.append(date_str)
                tmp = date_str.split("/")
                year = int(tmp[0])
                month = int(tmp[1])
                for i in range(len(col)):
                    if d[i * 2 + 1] != "":
                        date_str = d[i * 2 + 2]
                        tmp = date_str.split("/")
                        date = datetime.date(int(tmp[0]), int(tmp[1]), int(tmp[2]))
                        ret.update(
                            {
                                (year, month, col[i]): {
                                    "amount": int(d[i * 2 + 1]),
                                    "date": date,
                                }
                            }
                        )
            return (ret, row, col)

        if not self._withdrawal:
            self._withdrawal = asyncio.create_task(inner_get_withdrawal(self))
        return await self._withdrawal

    async def update_withdrawal(self, data: dict[Account, dict[str, int | datetime.date]]) -> None:
        try:
            ws = await self._wb.worksheet("引き落とし")
        except gspread.exceptions.WorksheetNotFound:
            return
        _, row, col = await self.get_withdrawal()
        tasks = []
        for key, value in data.items():
            if key in col and isinstance(value["date"], datetime.date):
                i = col.index(key)
                j = row.index(value["date"].strftime("%Y/%m"))
                tasks.append(
                    asyncio.create_task(ws.update_cell(j + 2, i * 2 + 2, value["amount"]))
                )
                tasks.append(
                    asyncio.create_task(
                        ws.update_cell(j + 2, i * 2 + 3, value["date"].strftime("%Y/%m/%d"))
                    )
                )
        await asyncio.gather(*tasks)

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
        await ws.update(convert_data + [[""] * 8] * max((max_num - len(data), 0)), "A3")
