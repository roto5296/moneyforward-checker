import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials


class SpreadSheet:
    def __init__(self, jsontext, sheet_id):
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(jsontext), scope
        )
        gc = gspread.authorize(credentials)
        self._wb = gc.open_by_key(sheet_id)

    def select(self, year, month):
        sname = str(year - 2000).zfill(2) + str(month).zfill(2)
        try:
            ws = self._wb.worksheet(sname)
        except gspread.exceptions.WorksheetNotFound:
            return []
        ret = ws.get_all_values()[2:]
        for tmp in ret:
            tmp[0] = int(tmp[0])  # transaction id
            tmp[3] = int(tmp[3])  # price
        return ret

    def merge(self, year, month, data, max_num=200):
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
            wst_index = sheet_names.index('template')
            ws = self._wb.duplicate_sheet(
                wss[wst_index].id,
                insert_sheet_index=wsb_index,
                new_sheet_name=sname
            )
        ws.update('A3', data + [[''] * 8] * (max_num - len(data)))
