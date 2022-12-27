import difflib
from concurrent.futures import ThreadPoolExecutor

from mfscraping.exceptions import DataDoesNotExist

from spreadsheet import SpreadSheet


def getdata(year, month, obj):
    try:
        return obj.get(year, month)
    except DataDoesNotExist:
        return []


def run(year, month, mf, ss, is_lambda):
    print(str(year) + "/" + str(month) + " start")
    with ThreadPoolExecutor() as executor:
        f_mf = executor.submit(getdata, year, month, mf)
        f_ss = executor.submit(getdata, year, month, ss)
        mfdata = f_mf.result()
        sdata = f_ss.result()
    if sdata == mfdata:
        print(str(year) + "/" + str(month) + " SAME")
    elif len(mfdata) == 0:
        print(str(year) + "/" + str(month) + " MoneyForward No Data")
    else:
        print(str(year) + "/" + str(month) + " There is diff\nUpdate sheet")
        if not is_lambda:
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
    print(str(year) + "/" + str(month) + " end")
