import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials


class SpreadSheet:
    def __init__(self, jsontext, sheet_id):
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(jsontext), scope)
        gc = gspread.authorize(credentials)
        self._wb = gc.open_by_key(sheet_id)

    def get(self, year, month):
        sname = str(year - 2000).zfill(2) + str(month).zfill(2)
        try:
            ws = self._wb.worksheet(sname)
        except gspread.exceptions.WorksheetNotFound:
            return []
        ws_data_list = ws.range('A3:G202')
        ret = []
        for i in range(200):
            if ws_data_list[i * 7].value == '':
                break
            tmp = [ws_data_list[j + i * 7].value for j in range(7)]
            tmp[2] = int(tmp[2])
            ret.append(tmp)
        return ret

    def update(self, year, month, data):
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
                wss[wst_index].id, insert_sheet_index=wsb_index, new_sheet_name=sname)
        ws.update('A3:G202', data +
                  [['', '', '', '', '', '', '']] * (200-len(data)))
