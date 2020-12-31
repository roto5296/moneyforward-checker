import sys
import os
import difflib
from moneyforward import MoneyForwardSelenium, MoneyForwardRequests
from spreadsheet import SpreadSheet
import threading
import argparse
import datetime


def run(year, month, dict, obj):
    dict[str(year) + '/' + str(month)] = obj.select(year, month)


dt_now_jst = datetime.datetime.now(
    datetime.timezone(datetime.timedelta(hours=9)))
parser = argparse.ArgumentParser()
parser.add_argument('--update', action='store_true')
parser.add_argument('--year', type=int, default=dt_now_jst.year)
parser.add_argument('--month', type=int, default=dt_now_jst.month)
parser.add_argument('--period', type=int, default=6)
parser.add_argument('--selenium', action='store_true')
args = parser.parse_args()
period = min(args.period, 12)
ym_list = [(
    args.year if args.month - i > 0 else args.year - 1,
    args.month - i if args.month - i > 0 else args.month + 12 - i
) for i in range(period)]

if args.selenium:
    mf = MoneyForwardSelenium(os.environ['MONEYFORWARD_KEYFILE'])
else:
    mf = MoneyForwardRequests(os.environ['MONEYFORWARD_KEYFILE'])
if (not(mf.login())):
    del mf
    sys.exit()
if args.update:
    mf.fetch()
ss = SpreadSheet(
    os.environ['SPREADSHEET_KEYFILE'], os.environ['SPREADSHEET_ID']
)
mfdata_dict = {}
ssdata_dict = {}
ts1 = [threading.Thread(target=run, args=(year, month, ssdata_dict, ss))
       for (year, month) in ym_list]
for t in ts1:
    t.start()
if args.selenium:
    for (year, month) in ym_list:
        run(year, month, mfdata_dict, mf)
else:
    ts2 = [threading.Thread(target=run, args=(
        year, month, mfdata_dict, mf)
    ) for (year, month) in ym_list]
    for t in ts2:
        t.start()
for t in ts1:
    t.join()
if not args.selenium:
    for t in ts2:
        t.join()
for (year, month) in ym_list:
    print(str(year) + '/' + str(month))
    mfdata = mfdata_dict[str(year) + '/' + str(month)]
    sdata = ssdata_dict[str(year) + '/' + str(month)]
    if sdata == mfdata:
        print('SAME')
    elif len(mfdata) == 0:
        print('MoneyForward No Data')
    else:
        print('There is diff\nUpdate sheet')
        fname = 'diff' + str(month) + '.html'
        d = difflib.HtmlDiff()
        with open(fname, mode='w') as f:
            f.write(d.make_file(
                [', '.join(map(str, i)) for i in mfdata],
                [', '.join(map(str, i)) for i in sdata]
            ))
        ss.upsert(year, month, mfdata)
del mf
del ss
