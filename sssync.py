import asyncio
import difflib

from mfscraping.exceptions import DataDoesNotExist

from spreadsheet import SpreadSheet


async def getdata(year, month, obj):
    try:
        return await obj.get(year, month)
    except DataDoesNotExist:
        return []


async def run(year, month, mf, ss, is_lambda):
    print(str(year) + "/" + str(month) + " start")
    mfdata, sdata = await asyncio.gather(getdata(year, month, mf), getdata(year, month, ss))
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
        await ss.merge(year, month, mfdata)
    print(str(year) + "/" + str(month) + " end")
