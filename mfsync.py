import asyncio
import dataclasses
from asyncio import Task
from collections.abc import Iterable

from mfscraping_asyncio import Account, MFScraper, MFTransaction, str2Account
from mfscraping_asyncio.exceptions import DataDoesNotExist


async def get_data(
    year: int, month: int, obj: MFScraper, transfer_disable: bool = False
) -> list[MFTransaction]:
    try:
        data = await obj.get(year, month)
        if transfer_disable:
            is_transfer_ = False
            tlist = []
            for d in data:
                if d.is_transfer():
                    is_transfer_ = True
                    tlist.append(obj.disable_transfer(d))
            if is_transfer_:
                await asyncio.gather(*tlist)
                return await obj.get(year, month)
            else:
                return data
        else:
            return data
    except DataDoesNotExist:
        return []


async def match_data(
    ac: tuple[Account, Account | list[Account]],
    t_main: Task[list[MFTransaction]],
    t_sub: Task[list[MFTransaction]],
) -> tuple[
    list[MFTransaction],
    list[MFTransaction],
    list[tuple[MFTransaction, MFTransaction]],
    list[MFTransaction],
]:
    tmp = await t_main
    tmp2 = await t_sub
    mmdl = list(ac_filter(tmp, ac[0]))
    msdl = list(ac_filter(tmp2, ac[1]))
    main_update_list: list[MFTransaction] = []
    main_add_list: list[tuple[MFTransaction, MFTransaction]] = []
    sub_update_list: list[MFTransaction] = []
    for mmd in mmdl[:]:
        for msd in msdl[:]:
            if msd.memo != "" and mmd.transaction_id == int(msd.memo):
                if not mmd.is_transfer():
                    if (
                        mmd.content.replace("’", "'") != msd.content.replace("’", "'")
                        or mmd.amount != msd.amount
                        or mmd.date != msd.date
                    ):
                        mmd.amount = msd.amount
                        mmd.content = msd.content
                        mmd.date = msd.date
                        main_update_list.append(mmd)
                mmdl.remove(mmd)
                msdl.remove(msd)
                break
    for mmd in mmdl[:]:
        for msd in msdl[:]:
            if (
                mmd.content.replace("’", "'") == msd.content.replace("’", "'")
                and mmd.amount == msd.amount
                and mmd.date == msd.date
            ):
                msd.memo = str(mmd.transaction_id)
                sub_update_list.append(msd)
                mmdl.remove(mmd)
                msdl.remove(msd)
                break
    for mmd in mmdl[:]:
        if mmd.is_transfer():
            for msd in msdl[:]:
                if mmd.date == msd.date and abs(mmd.amount) == abs(msd.amount):
                    msd.memo = str(mmd.transaction_id)
                    sub_update_list.append(msd)
                    msdl.remove(msd)
                    break
            mmdl.remove(mmd)
    main_delete_list = list(mmdl)
    for msd in msdl:
        data = MFTransaction(**dataclasses.asdict(msd))
        data.account = ac[0]
        data.memo = ""
        data.transaction_id = 0
        data.lcategory = "未分類"
        data.mcategory = "未分類"
        main_add_list.append((data, msd))
    return (main_update_list, main_delete_list, main_add_list, sub_update_list)


def ac_filter(data: list[MFTransaction], ac: Account | list[Account]) -> Iterable[MFTransaction]:
    def func(x: MFTransaction, ac: Account) -> bool:
        if len(ac) == 1:
            return (
                any([ac[0] == a[0] for a in x.account])
                if x.is_transfer()
                else ac[0] == x.account[0]
            )
        else:
            return any([ac == a for a in x.account]) if x.is_transfer() else ac == x.account

    if isinstance(ac, list):
        ac_: list[Account] = ac
        return filter(lambda x: any([func(x, y) for y in ac_]), data)
    else:
        return filter(lambda x: func(x, ac), data)


async def add_data(
    mf_main: MFScraper, main_add_all_list: list[list[tuple[MFTransaction, MFTransaction]]]
) -> None:
    for main_add_list in main_add_all_list:
        for data, _ in reversed(main_add_list):
            await mf_main.save(data)


async def update_add_sub_data(
    mf_subs: list[MFScraper],
    main_add_all_list: list[list[tuple[MFTransaction, MFTransaction]]],
    new_data: list[MFTransaction],
) -> None:
    t = []
    new_data_copy = sorted(new_data, key=lambda x: x.transaction_id, reverse=True)
    for mf_sub, main_add_list in zip(reversed(mf_subs), reversed(main_add_all_list)):
        for mdata, sdata in main_add_list:
            for ndata in new_data_copy:
                if (
                    ndata.content.replace("’", "'") == mdata.content.replace("’", "'")
                    and ndata.amount == mdata.amount
                    and ndata.date == mdata.date
                    and ndata.account == mdata.account
                ):
                    sdata.memo = str(ndata.transaction_id)
                    t.append(asyncio.create_task(mf_sub.update(sdata)))
                    break
    await asyncio.gather(*t)


async def update_data(
    mf_main: MFScraper,
    t_data: Task[
        tuple[
            list[MFTransaction],
            list[MFTransaction],
            list[tuple[MFTransaction, MFTransaction]],
            list[MFTransaction],
        ]
    ],
) -> None:
    data = await t_data
    await asyncio.gather(*[mf_main.update(x) for x in data[0]])


async def delete_data(
    mf_main: MFScraper,
    t_data: Task[
        tuple[
            list[MFTransaction],
            list[MFTransaction],
            list[tuple[MFTransaction, MFTransaction]],
            list[MFTransaction],
        ]
    ],
) -> None:
    data = await t_data
    await asyncio.gather(*[mf_main.delete(x) for x in data[1]])


async def update_sub_data(
    mf_sub: MFScraper,
    t_data: Task[
        tuple[
            list[MFTransaction],
            list[MFTransaction],
            list[tuple[MFTransaction, MFTransaction]],
            list[MFTransaction],
        ]
    ],
):
    data = await t_data
    await asyncio.gather(*[mf_sub.update(x) for x in data[3]])


async def run(
    year: int,
    month: int,
    mf_main: MFScraper,
    mf_subs: list[MFScraper],
    account_lists: list[list[str | list[str] | list[list[str]]]],
) -> list[MFTransaction]:
    print("mfsync " + str(year) + "/" + str(month) + " start")
    new_account_lists: list[list[tuple[Account, Account | list[Account]]]] = []
    for account_list in account_lists:
        new_account_list: list[tuple[Account, Account | list[Account]]] = []
        for account in account_list:
            if isinstance(account, str):
                x = str2Account(account)
                new_account_list.append((x, x))
            elif isinstance(account[0], str):
                if isinstance(account[1], str):
                    new_account_list.append((str2Account(account[0]), str2Account(account[1])))
                else:
                    new_account_list.append(
                        (str2Account(account[0]), [str2Account(x) for x in account[1]])
                    )
            else:
                raise ValueError()
        new_account_lists.append(new_account_list)

    t_main = asyncio.create_task(get_data(year, month, mf_main))
    t_subs = [asyncio.create_task(get_data(year, month, mf_sub, True)) for mf_sub in mf_subs]
    t_match_data = [
        [asyncio.create_task(match_data(ac, t_main, t_sub)) for ac in new_account_list]
        for new_account_list, t_sub in zip(new_account_lists, t_subs)
    ]
    t_update_data = [
        [asyncio.create_task(update_data(mf_main, x)) for x in tmp] for tmp in t_match_data
    ]
    t_delete_data = [
        [asyncio.create_task(delete_data(mf_main, x)) for x in tmp] for tmp in t_match_data
    ]
    [
        [asyncio.create_task(update_sub_data(mf_sub, x)) for x in tmp]
        for mf_sub, tmp in zip(mf_subs, t_match_data)
    ]
    rets_list: list[
        list[
            tuple[
                list[MFTransaction],
                list[MFTransaction],
                list[tuple[MFTransaction, MFTransaction]],
                list[MFTransaction],
            ]
        ]
    ] = await asyncio.gather(*[asyncio.gather(*tmp) for tmp in t_match_data])
    main_update_all_list = [[y for x in rets for y in x[0]] for rets in rets_list]
    main_delete_all_list = [[y for x in rets for y in x[1]] for rets in rets_list]
    main_add_all_list = [[y for x in rets for y in x[2]] for rets in rets_list]
    sub_update_all_list = [[y for x in rets for y in x[3]] for rets in rets_list]
    update_sum = sum([len(x) for x in main_update_all_list])
    delete_sum = sum([len(x) for x in main_delete_all_list])
    add_sum = sum([len(x) for x in main_add_all_list])
    sub_update_sum = sum([len(x) for x in sub_update_all_list])
    await add_data(mf_main, main_add_all_list)
    if add_sum:
        await asyncio.gather(*[asyncio.gather(*tmp) for tmp in t_update_data])
        await asyncio.gather(*[asyncio.gather(*tmp) for tmp in t_delete_data])
        new_data = await get_data(year, month, mf_main)
        asyncio.create_task(update_add_sub_data(mf_subs, main_add_all_list, new_data))
    elif update_sum or delete_sum:
        data = await t_main
        data2 = [x for x in data if x not in sum(main_delete_all_list, [])]
        for main_update in sum(main_update_all_list, []):
            ret = next(
                filter(lambda x: x.transaction_id == main_update.transaction_id, data2), None
            )
            if ret:
                for field in dataclasses.fields(MFTransaction):
                    setattr(ret, field.name, getattr(main_update, field.name))
        new_data = data2
    else:
        new_data = await t_main
    print(
        "mfsync "
        + str(year)
        + "/"
        + str(month)
        + " main update:"
        + str(update_sum)
        + " main delete:"
        + str(delete_sum)
        + " main add:"
        + str(add_sum)
        + " sub update:"
        + str(sub_update_sum)
    )
    print("mfsync " + str(year) + "/" + str(month) + " end")
    return new_data
