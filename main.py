import sys, os
import difflib
from moneyforward import MoneyForward
from spreadsheet import SpreadSheet

mf = MoneyForward(os.environ['MONEYFORWARD_KEYFILE'])
if (not(mf.login())):
    del mf
    sys.exit()
if len(sys.argv) == 2 and sys.argv[1] == "--update":
    mf.update()
ss = SpreadSheet(os.environ['SPREADSHEET_KEYFILE'], os.environ['SPREADSHEET_ID'])
year = 2020
for month in range(7,13):
    print(str(year) + '/' + str(month))
    mfdata = mf.get(year, month)
    sdata = ss.get(year, month)
    if sdata == mfdata:
        print("SAME")
    elif len(mfdata) == 0:
        print("MoneyForward No Data")
    else:
        print("There is diff\nUpdate sheet")
        fname = 'diff' + str(month) + '.html'
        d = difflib.HtmlDiff()
        with open(fname, mode='w') as f:
            f.write(d.make_file([', '.join(map(str, i)) for i in mfdata], [', '.join(map(str, i)) for i in sdata]))
        ss.update(year, month, mfdata)
del mf