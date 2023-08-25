import difflib
from asyncio import Task

from mfscraping_asyncio import MFTransaction
from mfscraping_asyncio.exceptions import DataDoesNotExist

from spreadsheet import SpreadSheet


async def getdata(year: int, month: int, obj: SpreadSheet) -> list[MFTransaction]:
    try:
        return await obj.get(year, month)
    except DataDoesNotExist:
        return []


async def run(
    year: int, month: int, t_mfdata: Task[list[MFTransaction]], ss: SpreadSheet, is_lambda: bool
) -> None:
    print("sssync " + str(year) + "/" + str(month) + " start")
    sdata = await getdata(year, month, ss)
    mfdata = await t_mfdata
    if sdata == mfdata:
        print("sssync " + str(year) + "/" + str(month) + " SAME")
    elif len(mfdata) == 0:
        print("sssync " + str(year) + "/" + str(month) + " MoneyForward No Data")
    else:
        print("sssync " + str(year) + "/" + str(month) + " There is diff\nUpdate sheet")
        if not is_lambda:
            fname = "diff" + str(month) + ".html"
            d = difflib.HtmlDiff()
            with open(fname, mode="w") as f:
                f.write(
                    d.make_file(
                        [", ".join(map(str, i)) for i in SpreadSheet.mf2ssformat(mfdata)],
                        [", ".join(map(str, i)) for i in SpreadSheet.mf2ssformat(sdata)],
                    )
                )
        await ss.merge(year, month, mfdata, len(sdata))
    print("sssync " + str(year) + "/" + str(month) + " end")
