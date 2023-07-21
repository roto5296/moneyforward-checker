from concurrent.futures import ThreadPoolExecutor
from functools import reduce

from mfscraping.exceptions import DataDoesNotExist


def getdata(year, month, obj):
    try:
        return obj.get(year, month)
    except DataDoesNotExist:
        return []


def run2(mmdl, msdl, auto_transfer_list):
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
    main_delete_list.extend([{"transaction_id": x["transaction_id"]} for x in mmdl])
    for msd in msdl:
        data = {**msd}
        data.pop("memo")
        data.pop("transaction_id")
        data["lcategory"] = "未分類"
        data["mcategory"] = "未分類"
        for at in auto_transfer_list:
            if at["search"] in data["content"]:
                data["is_transfer"] = True
                data["account"] = [at["account_from"], at["account_to"]]
        main_add_list.append((msd["transaction_id"], data))
    return (main_update_list, main_delete_list, main_add_list, sub_update_list)


def ac_filter(data, ac):
    return filter(
        lambda x: any([ac == a for a in x["account"]])
        if isinstance(x["account"], list)
        else ac == x["account"],
        data,
    )


def run3(year, month, obj, data):
    is_transfer = False
    with ThreadPoolExecutor() as executor:
        for d in data:
            if d["is_transfer"]:
                is_transfer = True
                executor.submit(obj.disable_transfer, d["transaction_id"])
    if is_transfer:
        return getdata(year, month, obj)
    else:
        return data


def run(year, month, mf_main, mf_subs, account_lists, auto_transfer_list):
    print(str(year) + "/" + str(month) + " start")
    mfmaindata = []
    mfsubdata_ = []
    with ThreadPoolExecutor() as executor:
        f_main = executor.submit(getdata, year, month, mf_main)
        f_subs = []
        f_subs2 = []
        for mf_sub in mf_subs:
            f_subs.append(executor.submit(getdata, year, month, mf_sub))
        mfmaindata = f_main.result()
        for mf_sub, f_sub in zip(mf_subs, f_subs):
            f_subs2.append(executor.submit(run3, year, month, mf_sub, f_sub.result()))
        for f_sub in f_subs2:
            mfsubdata_.append(f_sub.result())
    main_add_all_list = []
    with ThreadPoolExecutor() as executor:
        rets_list = []
        for account_list, mfsubdata in zip(account_lists, mfsubdata_):
            rets_list.append(
                executor.map(
                    run2,
                    [list(ac_filter(mfmaindata, ac)) for ac in account_list],
                    [list(ac_filter(mfsubdata, ac)) for ac in account_list],
                    [list(ac_filter(auto_transfer_list, ac)) for ac in account_list],
                )
            )
        rets_list = [list(ret) for ret in rets_list]
        main_update_all_list = [[y for x in rets for y in x[0]] for rets in rets_list]
        main_delete_all_list = [[y for x in rets for y in x[1]] for rets in rets_list]
        main_add_all_list = [[y for x in rets for y in x[2]] for rets in rets_list]
        sub_update_all_list = [[y for x in rets for y in x[3]] for rets in rets_list]
        print(
            str(year)
            + "/"
            + str(month)
            + " main update:"
            + str(reduce(lambda a, b: a + len(b), main_update_all_list, 0))
            + " main delete:"
            + str(reduce(lambda a, b: a + len(b), main_delete_all_list, 0))
            + " main add:"
            + str(reduce(lambda a, b: a + len(b), main_add_all_list, 0))
            + " sub update:"
            + str(reduce(lambda a, b: a + len(b), sub_update_all_list, 0))
        )
        for main_update_list, main_delete_list, sub_update_list, mf_sub in zip(
            main_update_all_list, main_delete_all_list, sub_update_all_list, mf_subs
        ):
            rets1 = executor.map(lambda x: mf_main.update(**x), main_update_list)
            rets2 = executor.map(lambda x: mf_main.delete(**x), main_delete_list)
            rets3 = executor.map(
                lambda x, y: y.update(**x), sub_update_list, [mf_sub] * len(sub_update_list)
            )
        list(rets1)
        list(rets2)
        list(rets3)
    is_add = False
    for main_add_list in main_add_all_list:
        for id, data in reversed(main_add_list):
            mf_main.save(**data)
            is_add = True
    if is_add:
        new_data = mf_main.get(year, month)
        new_data.sort(key=lambda x: x["transaction_id"], reverse=True)
        with ThreadPoolExecutor() as executor:
            for mf_sub, main_add_list in zip(reversed(mf_subs), reversed(main_add_all_list)):
                for i, (id, data) in enumerate(main_add_list):
                    executor.submit(
                        lambda a, b: mf_sub.update(
                            a, new_data[b]["amount"], memo=new_data[b]["transaction_id"]
                        ),
                        id,
                        i,
                    )
    print(str(year) + "/" + str(month) + " end")
