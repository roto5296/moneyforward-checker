import argparse
import asyncio
import datetime
import json
import os
import sys
from contextlib import AsyncExitStack

from mfscraping_asyncio import MFScraper
from mfscraping_asyncio.exceptions import FetchTimeout, LoginFailed

import mfsync
import sssync
from spreadsheet import SpreadSheet


async def main(ym_list, is_update, is_mfsync, is_sssync, is_lambda, update_maxtime, aclist=None):
    async with AsyncExitStack() as stack:
        mf_main = await stack.enter_async_context(
            MFScraper(**json.loads(os.environ["MONEYFORWARD_KEYFILE"])["main"], timeout=20)
        )
        mf_subs = await asyncio.gather(
            *[
                stack.enter_async_context(MFScraper(**x, timeout=20))
                for x in json.loads(os.environ["MONEYFORWARD_KEYFILE"])["sub"]
            ]
        )
        print("login...")
        try:
            await asyncio.gather(mf_main.login(), *[x.login() for x in mf_subs])
            print("LOGIN success")
        except LoginFailed:
            print("LOGIN fail")
            sys.exit()
        if is_update:
            print("update...")
            try:
                await asyncio.gather(
                    mf_main.fetch(maxwaiting=update_maxtime),
                    *[x.fetch(maxwaiting=update_maxtime) for x in mf_subs]
                )
                print("UPDATE success")
            except FetchTimeout:
                print("UPDATE timeout")
        if is_mfsync:
            print("mfsync...")
            if not isinstance(aclist, list):
                aclist = json.loads(os.environ["ACLIST"])
            auto_transfer_list = json.loads(os.environ["AUTO_TRANSFER_LIST"])

            await asyncio.gather(
                *[
                    mfsync.run(year, month, mf_main, mf_subs, aclist, auto_transfer_list)
                    for year, month in ym_list
                ]
            )
        if is_sssync:
            print("sssync...")
            ss = SpreadSheet(os.environ["SPREADSHEET_KEYFILE"], os.environ["SPREADSHEET_ID"])
            await ss.login()
            await asyncio.gather(
                *[sssync.run(year, month, mf_main, ss, is_lambda) for year, month in ym_list]
            )


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
            event["mfsync"],
            event["sssync"],
            True,
            0,
            event.get("aclist"),
        )
    )


if __name__ == "__main__":
    dt_now_jst = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    parser = argparse.ArgumentParser()
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--mfsync", action="store_true")
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
    asyncio.run(main(ym_list, args.update, args.mfsync, True, False, 300))
