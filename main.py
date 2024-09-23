import argparse
import asyncio
import datetime
import json
import re
import sys
import time
from contextlib import AsyncExitStack

from mfscraping_asyncio import MFScraper
from mfscraping_asyncio.exceptions import FetchTimeout, LoginFailed

import auth
import mfsync
import sssync
import transfer
import withdrawal
from drive import Drive
from gmail import Gmail
from parameter import get_parameter, put_parameter
from spreadsheet import SpreadSheet


async def main(
    ym_list: list[tuple[int, int]],
    is_update: bool,
    is_wupdate: bool,
    is_mfsync: bool,
    is_transfer: bool,
    is_sssync: bool,
    is_lambda: bool,
    update_maxtime: int,
    aclist: list[list[str | list[str] | list[list[str]]]] | None = None,
    auto_transfer_list: list[dict[str, str]] | None = None,
    auto_withdrawal_list: list[dict[str, str]] | None = None,
    timeout: int | None = None,
) -> None:
    cred_str = get_parameter("GOOGLEAPI_CRED")
    refresh_token = get_parameter("GOOGLEAPI_REFRESH_TOKEN")
    cred, new_cred_str = auth.authenticate(cred_str, refresh_token)
    if cred_str != new_cred_str:
        put_parameter("GOOGLEAPI_CRED", new_cred_str)
    drive = Drive(cred)
    gmail = Gmail(cred)
    mf_keyfile = json.loads(get_parameter("MONEYFORWARD_KEYFILE"))
    async with AsyncExitStack() as stack:
        t = {}
        if timeout:
            t.update({"timeout": timeout})
        main_task = asyncio.current_task()
        mf_main = await stack.enter_async_context(MFScraper(**mf_keyfile["main"], **t))
        mf_subs = await asyncio.gather(
            *[stack.enter_async_context(MFScraper(**x, **t)) for x in mf_keyfile["sub"]]
        )
        print("login...")
        ret = await asyncio.gather(
            mf_main.login(), *[x.login() for x in mf_subs], return_exceptions=True
        )
        time.sleep(10)
        otps = ["000000"] * 8
        query = (
            "from:do_not_reply@moneyforward.com subject:マネーフォワード ID メールによる追加認証"
        )
        messages = gmail.get_mail_list(query)
        time.sleep(1)
        message_data = [gmail.get_subject_message(message["id"]) for message in messages]
        for i, mail in enumerate(
            [mf_keyfile["main"]["id"]] + [a["id"] for a in mf_keyfile["sub"]]
        ):
            for data in message_data:
                if data["to"] == mail:
                    otp = re.search(r"\d\d\d\d\d\d", data["message"])
                    if otp is not None:
                        otps[i] = otp.group()
                        break
        try:
            ret = await asyncio.gather(
                mf_main.login_otp(otps[0]),
                *[x.login_otp(otp) for x, otp in zip(mf_subs, otps[1:])]
            )
            print("LOGIN success")
            print(ret)
        except LoginFailed:
            print("LOGIN fail")
            sys.exit()
        if is_update:
            print("update...")
            results = await asyncio.gather(
                mf_main.fetch(maxwaiting=update_maxtime),
                *[x.fetch(maxwaiting=update_maxtime) for x in mf_subs],
                return_exceptions=True
            )
            is_success = True
            for result in results:
                if isinstance(result, FetchTimeout):
                    print("UPDATE timeout")
                    is_success = False
                    break
            if is_success:
                print("UPDATE success")

        ss = None
        if is_transfer or is_sssync or is_wupdate:
            ss = SpreadSheet(cred, get_parameter("SPREADSHEET_ID"))
            await ss.login()

        if is_wupdate and ss is not None:
            print("wupdate...")
            await withdrawal.run(mf_main, mf_subs, ss)

        if is_mfsync:
            print("mfsync task start")
            tasks = main_mfsync(mf_main, mf_subs, drive, aclist, ym_list)
        else:
            tasks = [asyncio.create_task(mf_main.get(year, month)) for year, month in ym_list]
        await asyncio.sleep(0)

        if is_transfer and ss is not None:
            print("transfer task start")
            tasks = transfer_mfsync(
                mf_main, ss, drive, auto_transfer_list, auto_withdrawal_list, ym_list, tasks
            )
            await asyncio.sleep(0)

        if is_sssync and ss is not None:
            print("sssync task start")
            [
                asyncio.create_task(sssync.run(year, month, task, ss, is_lambda))
                for (year, month), task in zip(ym_list, tasks)
            ]
            await asyncio.sleep(0)

        while True:
            all_tasks = list(asyncio.all_tasks())
            if len(all_tasks) == 1:
                break
            if main_task:
                all_tasks.remove(main_task)
            try:
                await asyncio.gather(*all_tasks)
            except asyncio.CancelledError:
                pass


def main_mfsync(mf_main, mf_subs, drive, aclist, ym_list):
    if aclist is None:
        try:
            aclist = json.loads(get_parameter("ACCOUNT_LIST"))
        except BaseException:
            aclist = json.loads(drive.get(get_parameter("ACCOUNT_LIST_ID")))
    if aclist is not None:
        tasks = [
            asyncio.create_task(mfsync.run(year, month, mf_main, mf_subs, aclist))
            for year, month in ym_list
        ]
    else:
        tasks = []
    return tasks


def transfer_mfsync(mf_main, ss, drive, auto_transfer_list, auto_withdrawal_list, ym_list, tasks):
    if auto_transfer_list is None:
        try:
            auto_transfer_list = json.loads(get_parameter("AUTO_TRANSFER_LIST"))
        except BaseException:
            auto_transfer_list = json.loads(drive.get(get_parameter("AUTO_TRANSFER_LIST_ID")))
    if auto_withdrawal_list is None:
        try:
            auto_withdrawal_list = json.loads(get_parameter("AUTO_WITHDRAWAL_LIST"))
        except BaseException:
            auto_withdrawal_list = json.loads(drive.get(get_parameter("AUTO_WITHDRAWAL_LIST_ID")))
    if auto_transfer_list is not None and auto_withdrawal_list is not None:
        tasks = [
            asyncio.create_task(
                transfer.run(
                    year,
                    month,
                    mf_main,
                    ss,
                    task,
                    auto_transfer_list,
                    auto_withdrawal_list,
                )
            )
            for (year, month), task in zip(ym_list, tasks)
        ]
    else:
        tasks = []
    return tasks


def lambda_handler(event, context):
    dt_now_jst = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    ym_list = [
        (
            dt_now_jst.year if dt_now_jst.month - i > 0 else dt_now_jst.year - 1,
            dt_now_jst.month - i if dt_now_jst.month - i > 0 else dt_now_jst.month + 12 - i,
        )
        for i in range(event.get("period", 6))
    ]
    asyncio.run(
        main(
            ym_list,
            event["update"],
            event["wupdate"],
            event["mfsync"],
            event["transfer"],
            event["sssync"],
            True,
            0,
            event.get("aclist"),
            event.get("auto_transfer_list"),
            event.get("auto_withdrawal_list"),
            event.get("timeout"),
        )
    )


if __name__ == "__main__":
    dt_now_jst = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    parser = argparse.ArgumentParser()
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--wupdate", action="store_true")
    parser.add_argument("--mfsync", action="store_true")
    parser.add_argument("--transfer", action="store_true")
    parser.add_argument("--year", type=int, default=dt_now_jst.year)
    parser.add_argument("--month", type=int, default=dt_now_jst.month)
    parser.add_argument("--period", type=int, default=6)
    args = parser.parse_args()
    ym_list = [
        (
            args.year if args.month - i > 0 else args.year - 1,
            args.month - i if args.month - i > 0 else args.month + 12 - i,
        )
        for i in range(args.period)
    ]
    asyncio.run(
        main(ym_list, args.update, args.wupdate, args.mfsync, args.transfer, True, False, 300)
    )
