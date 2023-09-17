import asyncio
import datetime  # noqa
from asyncio import Task
from typing import TypedDict

from mfscraping_asyncio import Account, MFScraper, MFTransaction, str2Account  # noqa

from spreadsheet import SpreadSheet


async def run(
    year: int,
    month: int,
    mf_main: MFScraper,
    ss: SpreadSheet,
    tdata: Task,
    auto_transfer_list: list[dict],
    auto_withdrawal_list: list[dict[str, str]],
) -> list[MFTransaction]:
    print("auto transfer " + str(year) + "/" + str(month) + " start")
    num1 = await auto_withdrawal(year, month, tdata, ss, mf_main, auto_withdrawal_list)
    if num1:
        tdata = asyncio.create_task(mf_main.get(year, month))
    num2 = await auto_transfer(mf_main, tdata, auto_transfer_list)
    if num2:
        data = await mf_main.get(year, month)
    else:
        data = await tdata
    print(
        "auto transfer "
        + str(year)
        + "/"
        + str(month)
        + " withdrawal:"
        + str(num1)
        + " transfer:"
        + str(num2)
    )
    print("auto transfer " + str(year) + "/" + str(month) + " end")
    return data


async def auto_transfer(mf_main: MFScraper, tdata: Task, auto_transfer_list: list[dict]) -> int:
    class AutoTransfer(TypedDict, total=False):
        account_from: list[Account]
        search_from: list[str]
        account_to: list[Account]
        search_to: list[str]
        no_search_account: Account

    new_data = await tdata
    data_in = list(filter(lambda a: not a.is_transfer() and a.amount > 0, new_data))
    data_out = list(filter(lambda a: not a.is_transfer() and a.amount < 0, new_data))
    transfer_list: list[tuple[MFTransaction, MFTransaction | None, Account | None]] = []
    new_auto_transfer_list: list[AutoTransfer] = []

    for auto_transfer in auto_transfer_list:
        if auto_transfer["search_from"] is None and auto_transfer["search_to"] is None:
            raise
        nat: AutoTransfer = {}
        for ft in ["from", "to"]:
            keys = ["account_" + ft, "search_" + ft]

            if auto_transfer[keys[1]] is None:
                if not isinstance(auto_transfer[keys[0]], str):
                    raise
                nat["no_search_account"] = str2Account(auto_transfer[keys[0]])
            else:
                tmp = tuple(
                    len(auto_transfer[x]) if isinstance(auto_transfer[x], list) else 0
                    for x in keys
                )
                l_ = max(tmp)
                match tmp:
                    case (0, 0):
                        nat[keys[0]] = [str2Account(auto_transfer[keys[0]])]
                        nat[keys[1]] = [auto_transfer[keys[1]]]
                    case (0, _):
                        nat[keys[0]] = [str2Account(x) for x in [auto_transfer[keys[0]]] * l_]
                        nat[keys[1]] = auto_transfer[keys[1]]
                    case (_, 0):
                        nat[keys[0]] = [str2Account(x) for x in auto_transfer[keys[0]]]
                        nat[keys[1]] = [auto_transfer[keys[1]]] * l_
                    case (_, _):
                        if any(x != l_ for x in filter(lambda x: x > 0, tmp)):
                            raise
                        nat[keys[0]] = [str2Account(x) for x in auto_transfer[keys[0]]]
                        nat[keys[1]] = auto_transfer[keys[1]]
        new_auto_transfer_list.append(nat)

    for new_auto_transfer in new_auto_transfer_list:

        def filter_out(x: MFTransaction) -> bool:
            if "account_from" in new_auto_transfer and "search_from" in new_auto_transfer:
                return any(
                    x.account == y and z in x.content
                    for y, z in zip(
                        new_auto_transfer["account_from"], new_auto_transfer["search_from"]
                    )
                )
            else:
                return False

        def filter_in(x: MFTransaction) -> bool:
            if "account_to" in new_auto_transfer and "search_to" in new_auto_transfer:
                return any(
                    x.account == y and z in x.content
                    for y, z in zip(
                        new_auto_transfer["account_to"], new_auto_transfer["search_to"]
                    )
                )
            else:
                return False

        if "search_to" not in new_auto_transfer and "no_search_account" in new_auto_transfer:
            data_out_filter = list(filter(filter_out, data_out))
            for do in data_out_filter:
                transfer_list.append((do, None, new_auto_transfer["no_search_account"]))
                data_out.remove(do)
        elif "search_from" not in new_auto_transfer and "no_search_account" in new_auto_transfer:
            data_in_filter = list(filter(filter_in, data_in))
            for di in data_in_filter:
                transfer_list.append((di, None, new_auto_transfer["no_search_account"]))
                data_in.remove(di)
        else:
            data_out_filter = list(filter(filter_out, data_out))
            data_in_filter = list(filter(filter_in, data_in))
            for do in data_out_filter[:]:
                for di in data_in_filter[:]:
                    if (
                        do.account[0] != di.account[0]
                        and abs(do.amount) == abs(di.amount)
                        and do.date == di.date
                    ):
                        transfer_list.append((do, di, None))
                        data_out_filter.remove(do)
                        data_in_filter.remove(di)
                        data_out.remove(do)
                        data_in.remove(di)
                        break

    if transfer_list:
        await asyncio.gather(*[mf_main.transfer(*transfer) for transfer in transfer_list])
    return len(transfer_list)


async def auto_withdrawal(
    year: int,
    month: int,
    tdata: Task,
    ss: SpreadSheet,
    mf_main: MFScraper,
    auto_withdrawal_list: list[dict[str, str]],
) -> int:
    new_auto_withdrawal_list = [
        {"ac": str2Account(x["ac"]), "condition": x["condition"], "process": x["process"]}
        for x in auto_withdrawal_list
    ]
    wdata, _, _ = await ss.get_withdrawal()
    count = 0
    for new_auto_withdrawal in new_auto_withdrawal_list:
        ac = new_auto_withdrawal["ac"]
        if wd := wdata.get((year, month, ac), None):
            data = await tdata
            tf = eval(new_auto_withdrawal["condition"], globals() | {"wd": wd}, locals())
            if tf:
                count += 1
                await eval(new_auto_withdrawal["process"], globals() | {"wd": wd}, locals())
    return count
