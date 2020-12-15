import sys, os
import difflib
from moneyforward import MoneyForward
from spreadsheet import SpreadSheet
import threading

def run(year, month, dict, obj):
    dict[str(year) + '/' + str(month)] = obj.get(year, month)

mf = MoneyForward(os.environ['MONEYFORWARD_KEYFILE'])
if (not(mf.login())):
    del mf
    sys.exit()
if len(sys.argv) == 2 and sys.argv[1] == "--update":
    mf.update()
ss = SpreadSheet(os.environ['SPREADSHEET_KEYFILE'], os.environ['SPREADSHEET_ID'])
year = 2020
mfdata_dict = {}
ssdata_dict = {}
ts1 = [threading.Thread(target=run, args=(year, month, mfdata_dict, mf)) for month in range(7, 13)]
ts2 = [threading.Thread(target=run, args=(year, month, ssdata_dict, ss)) for month in range(7, 13)]
for t in ts1:
    t.start()
for t in ts2:
    t.start()
for t in ts1:
    t.join()
for t in ts2:
    t.join()
for month in range(7,13):
    print(str(year) + '/' + str(month))
    mfdata = mfdata_dict[str(year) + '/' + str(month)]
    sdata = ssdata_dict[str(year) + '/' + str(month)]
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
del ss