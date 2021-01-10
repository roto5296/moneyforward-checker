import argparse
import datetime
import difflib
import json
import os
import sys
import threading

from mfscraping import MFScraper

from spreadsheet import SpreadSheet


def run(year, month, dict, obj):
    dict[str(year) + "/" + str(month)] = obj.get(year, month)


def run2(year, month, t1, t2, mf_dict, ss_dict, ss, lock):
    t1.join()
    t2.join()
    lock.acquire()
    print(str(year) + "/" + str(month))
    mfdata = mf_dict[str(year) + "/" + str(month)]
    sdata = ss_dict[str(year) + "/" + str(month)]
    if sdata == mfdata:
        print("SAME")
    elif len(mfdata) == 0:
        print("MoneyForward No Data")
    else:
        print("There is diff\nUpdate sheet")
        fname = "diff" + str(month) + ".html"
        d = difflib.HtmlDiff()
        with open(fname, mode="w") as f:
            f.write(
                d.make_file(
                    [", ".join(map(str, i)) for i in SpreadSheet.dict2ssformat(mfdata)],
                    [", ".join(map(str, i)) for i in SpreadSheet.dict2ssformat(sdata)],
                )
            )
        ss.merge(year, month, mfdata)
    lock.release()


SpreadSheet.dict2ssformat

dt_now_jst = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
parser = argparse.ArgumentParser()
parser.add_argument("--update", action="store_true")
parser.add_argument("--year", type=int, default=dt_now_jst.year)
parser.add_argument("--month", type=int, default=dt_now_jst.month)
parser.add_argument("--period", type=int, default=6)
args = parser.parse_args()
period = min(args.period, 12)
ym_list = [
    (
        args.year if args.month - i > 0 else args.year - 1,
        args.month - i if args.month - i > 0 else args.month + 12 - i,
    )
    for i in range(period)
]


mf = MFScraper(**json.loads(os.environ["MONEYFORWARD_KEYFILE"]))
print("login...")
if mf.login():
    print("LOGIN success")
else:
    print("LOGIN fail")
    del mf
    sys.exit()
if args.update:
    print("update...")
    if mf.fetch():
        print("UPDATE success")
    else:
        print("UPDATE fail or timeout")
ss = SpreadSheet(os.environ["SPREADSHEET_KEYFILE"], os.environ["SPREADSHEET_ID"])
mfdata_dict = {}
ssdata_dict = {}
lock = threading.Lock()
ts1 = [
    threading.Thread(target=run, args=(year, month, ssdata_dict, ss)) for (year, month) in ym_list
]
ts2 = [
    threading.Thread(target=run, args=(year, month, mfdata_dict, mf)) for (year, month) in ym_list
]
ts3 = [
    threading.Thread(target=run2, args=(year, month, t1, t2, mfdata_dict, ssdata_dict, ss, lock))
    for (year, month), t1, t2 in zip(ym_list, ts1, ts2)
]
for t1, t2, t3 in zip(ts1, ts2, ts3):
    t1.start()
    t2.start()
    t3.start()
del mf
del ss
