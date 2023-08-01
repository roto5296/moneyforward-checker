import argparse
import datetime
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

from mfscraping import MFScraper
from mfscraping.exceptions import FetchTimeout, LoginFailed

import mfsync
import sssync
from spreadsheet import SpreadSheet


def main(ym_list, is_update, is_mfsync, is_sssync, is_lambda, update_maxtime, aclist=None):
    mf_main = MFScraper(**json.loads(os.environ["MONEYFORWARD_KEYFILE"])["main"])
    mf_subs = list(
        map(lambda x: MFScraper(**x), json.loads(os.environ["MONEYFORWARD_KEYFILE"])["sub"])
    )
    print("login...")
    try:
        with ThreadPoolExecutor() as executor:
            ret = executor.submit(mf_main.login)
            rets = executor.map(lambda x: x.login(), mf_subs)
            ret.result()
            list(rets)
        print("LOGIN success")
    except LoginFailed:
        print("LOGIN fail")
        sys.exit()
    if is_update:
        print("update...")
        try:
            with ThreadPoolExecutor() as executor:
                ret = executor.submit(mf_main.fetch, maxwaiting=update_maxtime)
                rets = executor.map(lambda x: x.fetch(maxwaiting=update_maxtime), mf_subs)
                ret.result()
                list(rets)
            print("UPDATE success")
        except FetchTimeout:
            print("UPDATE timeout")
    if is_mfsync:
        print("mfsync...")
        if not isinstance(aclist, list):
            aclist = json.loads(os.environ["ACLIST"])
        auto_transfer_list = json.loads(os.environ["AUTO_TRANSFER_LIST"])
        with ThreadPoolExecutor() as executor:
            rets = []
            for year, month in ym_list:
                rets.append(
                    executor.submit(
                        mfsync.run, year, month, mf_main, mf_subs, aclist, auto_transfer_list
                    )
                )
            [ret.result() for ret in rets]
    if is_sssync:
        print("sssync...")
        ss = SpreadSheet(os.environ["SPREADSHEET_KEYFILE"], os.environ["SPREADSHEET_ID"])
        with ThreadPoolExecutor() as executor:
            rets = []
            for year, month in ym_list:
                rets.append(executor.submit(sssync.run, year, month, mf_main, ss, is_lambda))
            [ret.result() for ret in rets]


def lambda_handler(event, context):
    dt_now_jst = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    ym_list = [
        (
            dt_now_jst.year if dt_now_jst.month - i > 0 else dt_now_jst.year - 1,
            dt_now_jst.month - i if dt_now_jst.month - i > 0 else dt_now_jst.month + 12 - i,
        )
        for i in range(event.get("period", 6))
    ]
    main(ym_list, event["update"], event["mfsync"], event["sssync"], True, 0, event.get("aclist"))


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
    main(ym_list, args.update, args.mfsync, True, False, 300)
