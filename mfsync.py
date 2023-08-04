import asyncio

from mfscraping.exceptions import DataDoesNotExist


async def get_data(year, month, obj, transfer_disable=False):
    try:
        data = await obj.get(year, month)
        if transfer_disable:
            is_transfer = False
            tlist = []
            for d in data:
                if d["is_transfer"]:
                    is_transfer = True
                    tlist.append(obj.disable_transfer(d["transaction_id"]))
            if is_transfer:
                await asyncio.gather(*tlist)
                return await obj.get(year, month)
            else:
                return data
        else:
            return data
    except DataDoesNotExist:
        return []


async def match_data(ac, t_main, t_sub):
    tmp = await t_main
    tmp2 = await t_sub
    mmdl = list(ac_filter(tmp, ac))
    msdl = list(ac_filter(tmp2, ac))
    main_update_list = []
    main_delete_list = []
    main_add_list = []
    sub_update_list = []
    for mmd in mmdl[:]:
        for msd in msdl[:]:
            if msd["memo"] != "" and mmd["transaction_id"] == int(msd["memo"]):
                if not mmd["is_transfer"]:
                    if (
                        mmd["content"].replace("’", "'") != msd["content"].replace("’", "'")
                        or mmd["amount"] != msd["amount"]
                        or mmd["date"] != msd["date"]
                    ):
                        main_update_list.append(
                            {
                                "transaction_id": mmd["transaction_id"],
                                "amount": msd["amount"],
                                "content": msd["content"],
                                "date": msd["date"],
                            }
                        )
                mmdl.remove(mmd)
                msdl.remove(msd)
                break
    for mmd in mmdl[:]:
        for msd in msdl[:]:
            if (
                mmd["content"].replace("’", "'") == msd["content"].replace("’", "'")
                and abs(mmd["amount"]) == abs(msd["amount"])
                and mmd["date"] == msd["date"]
            ):
                sub_update_list.append(
                    {
                        "transaction_id": msd["transaction_id"],
                        "amount": msd["amount"],
                        "memo": mmd["transaction_id"],
                    }
                )
                mmdl.remove(mmd)
                msdl.remove(msd)
                break
    for mmd in mmdl[:]:
        if mmd["is_transfer"]:
            for msd in msdl[:]:
                if (
                    mmd["date"] == msd["date"]
                    and abs(mmd["amount"]) == abs(msd["amount"])
                    and msd["memo"] != ""
                ):
                    sub_update_list.append(
                        {
                            "transaction_id": msd["transaction_id"],
                            "amount": msd["amount"],
                            "memo": mmd["transaction_id"],
                        }
                    )
                    msdl.remove(msd)
                    break
            mmdl.remove(mmd)
    main_delete_list.extend([x["transaction_id"] for x in mmdl])
    for msd in msdl:
        data = {**msd}
        data.pop("memo")
        data.pop("transaction_id")
        data["lcategory"] = "未分類"
        data["mcategory"] = "未分類"
        main_add_list.append((msd["transaction_id"], data))
    return (main_update_list, main_delete_list, main_add_list, sub_update_list)


def ac_filter(data, ac):
    return filter(
        lambda x: any([ac == a for a in x["account"]])
        if isinstance(x["account"], list)
        else ac == x["account"],
        data,
    )


async def add_data(mf_main, main_add_all_list):
    for main_add_list in main_add_all_list:
        for id, data in reversed(main_add_list):
            await mf_main.save(**data)


async def update_add_sub_date(mf_subs, main_add_all_list, new_data):
    new_data.sort(key=lambda x: x["transaction_id"], reverse=True)
    for mf_sub, main_add_list in zip(reversed(mf_subs), reversed(main_add_all_list)):
        for i, (id, data) in enumerate(main_add_list):
            asyncio.create_task(
                mf_sub.update(id, new_data[i]["amount"], memo=new_data[i]["transaction_id"])
            )


async def update_data(mf_main, t_data):
    data = await t_data
    await asyncio.gather(*[mf_main.update(**x) for x in data[0]])


async def delete_data(mf_main, t_data):
    data = await t_data
    await asyncio.gather(*[mf_main.delete(x) for x in data[1]])


async def update_sub_data(mf_sub, t_data):
    data = await t_data
    await asyncio.gather(*[mf_sub.update(**x) for x in data[3]])


async def run(year, month, mf_main, mf_subs, account_lists, auto_transfer_list):
    print("mfsync " + str(year) + "/" + str(month) + " start")
    t_main = asyncio.create_task(get_data(year, month, mf_main))
    t_subs = [asyncio.create_task(get_data(year, month, mf_sub, True)) for mf_sub in mf_subs]
    t_match_data = [
        [asyncio.create_task(match_data(ac, t_main, t_sub)) for ac in account_list]
        for account_list, t_sub in zip(account_lists, t_subs)
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
    rets_list = await asyncio.gather(*[asyncio.gather(*tmp) for tmp in t_match_data])
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
        await update_add_sub_date(mf_subs, main_add_all_list, new_data)
    elif update_sum or delete_sum:
        data = await t_main
        data2 = [x for x in data if x["transaction_id"] not in sum(main_delete_all_list, [])]
        for main_update in sum(main_update_all_list, []):
            ret = next(
                filter(lambda x: x["transaction_id"] == main_update["transaction_id"], data2), None
            )
            ret.update(main_update)
        new_data = data2
    else:
        new_data = await t_main
    transfer_sum = await auto_transfer(year, month, mf_main, new_data, auto_transfer_list)
    if transfer_sum:
        new_data = await get_data(year, month, mf_main)
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
        + " transfer:"
        + str(transfer_sum)
    )
    print("mfsync " + str(year) + "/" + str(month) + " end")
    return new_data


async def auto_transfer(year, month, mf_main, new_data, auto_transfer_list):
    data_in = list(filter(lambda a: not a["is_transfer"] and a["amount"] > 0, new_data))
    data_out = list(filter(lambda a: not a["is_transfer"] and a["amount"] < 0, new_data))
    transfer_list = []
    for auto_transfer in auto_transfer_list:
        if not auto_transfer["search_to"]:
            for do in data_out[:]:
                if (
                    do["account"] == auto_transfer["account_from"]
                    and auto_transfer["search_from"] in do["content"]
                ):
                    transfer_list.append(
                        [
                            do["transaction_id"],
                            auto_transfer["account_to"],
                            auto_transfer["sub_account_to"],
                        ]
                    )
                    data_out.remove(do)
        elif not auto_transfer["search_from"]:
            for di in data_in[:]:
                if (
                    di["account"] == auto_transfer["account_to"]
                    and auto_transfer["search_to"] in di["content"]
                ):
                    transfer_list.append(
                        [
                            di["transaction_id"],
                            auto_transfer["account_from"],
                            auto_transfer["sub_account_from"],
                        ]
                    )
                    data_in.remove(di)
        else:
            for do in data_out[:]:
                if (
                    do["account"] == auto_transfer["account_from"]
                    and auto_transfer["search_from"] in do["content"]
                ):
                    for di in data_in[:]:
                        if (
                            di["account"] == auto_transfer["account_to"]
                            and auto_transfer["search_to"] in di["content"]
                            and abs(do["amount"]) == abs(di["amount"])
                            and do["date"] == di["date"]
                        ):
                            transfer_list.append(
                                [
                                    do["transaction_id"],
                                    auto_transfer["account_to"],
                                    auto_transfer["sub_account_to"],
                                    di["transaction_id"],
                                ]
                            )
                            data_out.remove(do)
                            data_in.remove(di)
                            break
    await asyncio.gather(*[mf_main.transfer(*transfer) for transfer in transfer_list])
    return len(transfer_list)
