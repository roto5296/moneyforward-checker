import asyncio
import datetime

from mfscraping_asyncio import Account, MFScraper

from spreadsheet import SpreadSheet


async def run(
    mf_main: MFScraper,
    mf_subs: list[MFScraper],
    ss: SpreadSheet,
) -> None:
    ret_: dict[Account, dict[str, int | datetime.date]] = {}
    tasks = [asyncio.create_task(mf_main.get_withdrawal())] + [
        asyncio.create_task(mf_sub.get_withdrawal()) for mf_sub in mf_subs
    ]
    t_ = asyncio.create_task(ss.get_withdrawal())
    ret: list[dict[Account, dict[str, int | datetime.date]]] = await asyncio.gather(*tasks)
    [ret_.update(x) for x in ret]
    ssret, _, col = await t_
    tasks = []
    tmp = {}
    for key, value in ret_.items():
        if key in col:
            if isinstance(date_ := value.get("date", None), datetime.date) and (
                not (data_ := ssret.get((date_.year, date_.month, key), None))
                or data_["amount"] != value["amount"]
                or data_["date"] != value["date"]
            ):
                tmp |= {key: value}
    await ss.update_withdrawal(tmp)
    print("wupdate " + str(len(tmp)))
