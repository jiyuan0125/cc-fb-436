#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""接着采集对公账户对账单，产出 txns.csv / daily.csv / rejects.csv / crawl_summary.txt。

规则要点：
  * tasks.csv 的 已抓/已冻结 是上一轮已完成的凭据；已冻结一律不再调 fetch。
  * 落盘完好（页连续、非末页整 20 行、末页不足 20 行）的任务不再调 fetch。
  * 待抓任务里已有落盘的是上一轮中断留下的旧时点半截数据，整任务重抓并计数。
  * crawl_state.json 记录已采任务与缺件登记，重复执行时不再重抓、数值稳定。
"""

import csv
import json
import os
import re
import sys
from decimal import Decimal, ROUND_HALF_UP

ROOT = os.path.dirname(os.path.abspath(__file__))
for _p in (ROOT, os.getcwd()):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import api

RAW_DIR = os.path.join(ROOT, "raw")
# 采集状态放在 raw 之外，raw 目录保持一任务一 json 的原样。
MANIFEST = os.path.join(ROOT, "crawl_state.json")
TXNS = os.path.join(ROOT, "txns.csv")
DAILY = os.path.join(ROOT, "daily.csv")
REJECTS = os.path.join(ROOT, "rejects.csv")
SUMMARY = os.path.join(ROOT, "crawl_summary.txt")

PAGE_SIZE = 20
OK_STATUS = ("已清算", "待清算")
REVOKED = "撤销"

_AMT_RE = re.compile(r"^\d{1,12}\.\d{1,3}$")
_TIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")

NOTES = (
    "现势状态取同一流水号在可见清单中按交易日归并、以最晚一日日终定格的记录；"
    "撤销行银行未记账整笔剔除并登记rejects.csv；"
    "金额段坏的行另日有正常行的以正常行入账、坏行仍记拒绝；"
    "厘位金额四舍五入折为分；"
    "冻结缺件的日不查询、期初期末仍按balances.csv链；"
    "整任务重抓与落盘缺件的任务登记在crawl_state.json"
)


def read_csv_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return [[c.strip() for c in row] for row in csv.reader(f) if row]


def amount_cents(text):
    """金额段 -> 分（整数）；认不出返回 None。"""
    if not isinstance(text, str) or not _AMT_RE.match(text):
        return None
    return int((Decimal(text) * 100).to_integral_value(rounding=ROUND_HALF_UP))


def amount_reason(text):
    if "," in text or "，" in text:
        return "amount_comma"
    return "amount_malformed"


def pages_complete(pages):
    """落盘结构是否完好：页连续，非末页整页 20 行，末页 1..20 行，每行五段文本。"""
    if not isinstance(pages, list) or not pages:
        return False
    for i, page in enumerate(pages):
        if not isinstance(page, list):
            return False
        if i < len(pages) - 1:
            if len(page) != PAGE_SIZE:
                return False
        elif not 1 <= len(page) <= PAGE_SIZE:
            return False
        for row in page:
            if not (isinstance(row, list) and len(row) == 5
                    and all(isinstance(x, str) for x in row)):
                return False
    return True


def load_pages(path):
    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
    except (OSError, ValueError):
        return None
    return obj if pages_complete(obj) else None


def save_pages(path, pages):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(pages, ensure_ascii=False))


def load_manifest():
    try:
        with open(MANIFEST, encoding="utf-8") as f:
            obj = json.load(f)
    except (OSError, ValueError):
        return {}
    return obj if isinstance(obj, dict) else {}


def save_manifest(manifest):
    with open(MANIFEST, "w", encoding="utf-8") as f:
        f.write(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=1))


def crawl(acct, day):
    """按页翻到 more 为 false。"""
    pages = []
    page_no = 1
    while True:
        try:
            res = api.fetch(acct, day, page_no)
        except ValueError as exc:
            print("fetch 失败 %s %s 第%d页: %s" % (acct, day, page_no, exc),
                  file=sys.stderr)
            return None
        rows = [list(r) for r in res.get("rows") or []]
        pages.append(rows)
        if not res.get("more") or not rows:
            break
        page_no += 1
    return pages


def collect():
    tasks = read_csv_rows(os.path.join(ROOT, "tasks.csv"))
    manifest = load_manifest()

    dataset = []          # [(acct, day, pages)]
    frozen_missing = 0
    redone_keys = set(k for k, v in manifest.items()
                      if isinstance(v, dict) and v.get("redone"))

    for acct, day, state in tasks:
        key = "%s|%s" % (acct, day)
        path = os.path.join(RAW_DIR, "%s_%s.json" % (acct, day))
        pages = load_pages(path)

        if state == "已冻结":
            # 对过账的任务一律不再查询；没有落盘就算缺件。
            if pages is None:
                frozen_missing += 1
            else:
                dataset.append((acct, day, pages))
            continue

        if pages is not None and (state == "已抓" or key in manifest):
            dataset.append((acct, day, pages))
            continue

        had_capture = pages is not None or key in manifest or state == "已抓"
        fresh = crawl(acct, day)
        if fresh is None:
            if pages is not None:
                dataset.append((acct, day, pages))
            continue
        save_pages(path, fresh)
        entry = {"pages": len(fresh),
                 "rows": sum(len(p) for p in fresh),
                 "redone": bool(had_capture)}
        manifest[key] = entry
        if entry["redone"]:
            redone_keys.add(key)
        dataset.append((acct, day, fresh))

    save_manifest(manifest)
    # 只统计清单里仍存在的任务，重复执行数值稳定。
    task_keys = {"%s|%s" % (a, d) for a, d, _s in tasks}
    redone = len(redone_keys & task_keys)
    return dataset, frozen_missing, redone


def classify(dataset):
    valid = []
    rejects = []
    for acct, day, pages in dataset:
        for pi, page in enumerate(pages, 1):
            for li, row in enumerate(page, 1):
                txn, direction, amt, status, ts = row
                reason = None
                bad_field = amt
                if status == REVOKED:
                    reason, bad_field = "status_revoked", status
                elif status not in OK_STATUS:
                    reason, bad_field = "status_unknown", status
                elif amount_cents(amt) is None:
                    reason = amount_reason(amt)
                elif not _TIME_RE.match(ts):
                    reason, bad_field = "time_malformed", ts
                if reason:
                    rejects.append([acct, day, pi, li, bad_field, reason])
                    continue
                valid.append({"txn": txn, "acct": acct, "stmt_day": day,
                              "dir": direction, "amount": amount_cents(amt),
                              "status": status, "trade_day": ts[:10].replace("-", ""),
                              "page": pi, "line": li})
    rejects.sort(key=lambda r: (r[0], r[1], r[2], r[3], r[5]))
    return valid, rejects


def build_txns(valid):
    groups = {}
    for v in valid:
        groups.setdefault(v["txn"], []).append(v)

    txns = []
    for txn, items in groups.items():
        # 跨日同号：取最晚一日日终的那一行作为现势状态。
        items.sort(key=lambda x: (x["stmt_day"], x["page"], x["line"]), reverse=True)
        cur = items[0]
        if len({(x["amount"], x["dir"]) for x in items}) > 1:
            print("流水号 %s 各日金额或方向不一致，取最晚一日" % txn, file=sys.stderr)
        txns.append([txn, cur["acct"], cur["trade_day"], cur["dir"],
                     cur["amount"], cur["status"]])
    txns.sort(key=lambda r: (r[1], r[2], r[0]))
    return txns


def build_daily(dataset):
    opening = {a: int(b) for a, b in read_csv_rows(os.path.join(ROOT, "opening.csv"))}
    eod = {}
    for a, d, v in read_csv_rows(os.path.join(ROOT, "balances.csv")):
        eod[(a, d)] = int(v)

    days = {}
    for acct, day, _pages in dataset:
        days.setdefault(acct, set()).add(day)
    for acct, day in eod:
        days.setdefault(acct, set()).add(day)

    out = []
    for acct in sorted(days):
        prev = opening.get(acct)
        for day in sorted(days[acct]):
            close = eod.get((acct, day))
            if close is None:
                print("balances 缺 %s %s，期末沿用上一日" % (acct, day), file=sys.stderr)
                close = prev
            out.append([acct, day, prev, close])
            prev = close
    return out


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def main():
    dataset, frozen_missing, redone = collect()
    valid, rejects = classify(dataset)
    txns = build_txns(valid)
    daily = build_daily(dataset)

    write_csv(TXNS, ["txn_id", "acct", "day", "dir", "amount", "status"], txns)
    write_csv(DAILY, ["acct", "day", "open", "close"], daily)
    write_csv(REJECTS, ["acct", "day", "page", "line", "raw", "reason"], rejects)

    dups_merged = len(valid) - len(txns)
    lines = [
        "rows_out %d" % len(txns),
        "dups_merged %d" % dups_merged,
        "rows_rejected %d" % len(rejects),
        "frozen_missing %d" % frozen_missing,
        "tasks_redone %d" % redone,
        "notes %s" % NOTES,
    ]
    with open(SUMMARY, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
