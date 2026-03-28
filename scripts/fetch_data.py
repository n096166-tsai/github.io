#!/usr/bin/env python3
"""
個股期貨資料爬蟲 v3
- 改用台灣期交所（TAIFEX）官方公開 API
- 支援補抓歷史資料：設定 BACKFILL_START / BACKFILL_END 環境變數
- 每日計算成交量遞增前 20 名
"""

import json
import os
import sys
import time
import datetime
import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
TOP_N    = 20

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.taifex.com.tw/',
}

TAIFEX_JSON_API = (
    'https://opendata.taifex.com.tw/v1/DailyFuturesDate'
    '?MarketCode=0&CommodityID=SF&Date={date_nodash}'
)
TAIFEX_CSV_API = (
    'https://www.taifex.com.tw/cht/3/futDataDown'
    '?down_type=1&queryStartDate={date}&queryEndDate={date}'
    '&commodity_id=SF'
)


def ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def tw_now():
    tz = datetime.timezone(datetime.timedelta(hours=8))
    return datetime.datetime.now(tz)


def today_str():
    return tw_now().strftime('%Y-%m-%d')


def now_str():
    return tw_now().strftime('%Y-%m-%d %H:%M')


def load_json(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def safe_float(s):
    if s is None:
        return None
    try:
        return float(str(s).replace(',', '').replace('+', '').strip())
    except Exception:
        return None


# ── 取得日期範圍內所有工作日 ──────────────────────────────
def get_weekdays(start_str, end_str):
    """回傳 start ~ end 之間所有週一至週五的日期字串清單"""
    tz = datetime.timezone(datetime.timedelta(hours=8))
    start = datetime.datetime.strptime(start_str, '%Y-%m-%d').replace(tzinfo=tz)
    end   = datetime.datetime.strptime(end_str,   '%Y-%m-%d').replace(tzinfo=tz)
    days  = []
    cur   = start
    while cur <= end:
        if cur.weekday() < 5:  # 0=Monday, 4=Friday
            days.append(cur.strftime('%Y-%m-%d'))
        cur += datetime.timedelta(days=1)
    return days


# ── API 方法一：期交所 JSON 開放資料 ─────────────────────
def fetch_taifex_opendata(date_str):
    date_nodash = date_str.replace('-', '')
    url = TAIFEX_JSON_API.format(date_nodash=date_nodash)
    print(f'  [API-1] {url}')
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list) or len(data) == 0:
            return []
        records = []
        for row in data:
            vol  = None
            for k in row:
                if any(x in k.lower() for x in ['vol', 'volume', '成交量']):
                    vol = safe_float(row[k])
                    if vol is not None:
                        break
            name = (row.get('ContractName') or row.get('Name') or
                    row.get('CommodityName') or '')
            code = (row.get('ContractCode') or row.get('Code') or
                    row.get('CommodityID') or '')
            price = safe_float(row.get('SettlementPrice') or
                               row.get('Close') or row.get('close'))
            oi    = safe_float(row.get('OpenInterest') or row.get('openInterest'))
            if not name or vol is None:
                continue
            records.append({'code': code, 'name': name, 'volume': vol,
                            'price': price, 'open_interest': oi})
        return records
    except Exception as e:
        print(f'  [API-1] 失敗：{e}')
        return []


# ── API 方法二：期交所 CSV 下載 ───────────────────────────
def fetch_taifex_csv(date_str):
    date_slash = date_str.replace('-', '/')
    url = TAIFEX_CSV_API.format(date=date_slash)
    print(f'  [API-2] {url}')
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        for enc in ('big5', 'utf-8-sig', 'utf-8'):
            try:
                text = resp.content.decode(enc)
                break
            except Exception:
                continue
        else:
            text = resp.text
        records = []
        for line in text.strip().split('\n')[1:]:
            cols = [c.strip().strip('"') for c in line.split(',')]
            if len(cols) < 10:
                continue
            name = cols[1] if len(cols) > 1 else ''
            vol  = safe_float(cols[9]) if len(cols) > 9 else None
            price = safe_float(cols[6]) if len(cols) > 6 else None
            oi   = safe_float(cols[11]) if len(cols) > 11 else None
            if not name or vol is None:
                continue
            records.append({'code': cols[1], 'name': name, 'volume': vol,
                            'price': price, 'open_interest': oi})
        return records
    except Exception as e:
        print(f'  [API-2] 失敗：{e}')
        return []


def fetch_one_day(date_str):
    """嘗試所有 API 取得單日資料，失敗回傳空清單"""
    records = fetch_taifex_opendata(date_str)
    if not records:
        records = fetch_taifex_csv(date_str)
    return records


# ── 計算量增排行 ──────────────────────────────────────────
def calc_ranking(today_records, yesterday_records):
    yest_map = {}
    for r in (yesterday_records or []):
        key = r.get('code') or r.get('name')
        if key:
            yest_map[key] = r

    result = []
    for r in today_records:
        key   = r.get('code') or r.get('name')
        yest  = yest_map.get(key)
        vol_t = r.get('volume') or 0
        vol_y = (yest.get('volume') or 0) if yest else 0
        vol_chg = round((vol_t - vol_y) / vol_y * 100, 2) if vol_y > 0 else None
        result.append({**r, 'volume_change_pct': vol_chg, 'price_change_pct': None})

    has = sorted([r for r in result if r['volume_change_pct'] is not None],
                 key=lambda x: x['volume_change_pct'], reverse=True)
    no  = sorted([r for r in result if r['volume_change_pct'] is None],
                 key=lambda x: x.get('volume') or 0, reverse=True)
    return (has + no)[:TOP_N]


def update_index(date):
    idx_path = os.path.join(DATA_DIR, 'index.json')
    idx   = load_json(idx_path) or {'dates': []}
    dates = idx.get('dates', [])
    if date not in dates:
        dates.append(date)
        dates.sort(reverse=True)
    idx['dates']        = dates
    idx['last_updated'] = now_str()
    save_json(idx_path, idx)


# ── 處理單一日期 ──────────────────────────────────────────
def process_date(date_str, prev_date_str=None):
    print(f'\n── {date_str} ──────────────────────')
    records = fetch_one_day(date_str)

    if not records:
        print(f'  → 無資料（非交易日或 API 未更新），跳過')
        return False

    # 儲存原始
    raw_path = os.path.join(DATA_DIR, f'raw_{date_str}.json')
    save_json(raw_path, {'date': date_str, 'fetched': now_str(), 'records': records})
    print(f'  → 原始資料 {len(records)} 筆')

    # 讀前一日
    yest_records = []
    if prev_date_str:
        yest_data = load_json(os.path.join(DATA_DIR, f'raw_{prev_date_str}.json'))
        if yest_data:
            yest_records = yest_data.get('records', [])

    # 計算排行
    ranking = calc_ranking(records, yest_records)
    rank_path = os.path.join(DATA_DIR, f'ranking_{date_str}.json')
    save_json(rank_path, {'date': date_str, 'fetched': now_str(), 'ranking': ranking})

    print(f'  → 排行前3：', end='')
    for r in ranking[:3]:
        p = f"{r['volume_change_pct']:+.1f}%" if r['volume_change_pct'] is not None else 'N/A'
        print(f'{r["name"]}({p})', end='  ')
    print()

    update_index(date_str)
    return True


# ── 主程式 ────────────────────────────────────────────────
def main():
    ensure_dir()
    print(f'\n{"="*50}')
    print(f' 個股期貨爬蟲 v3  {now_str()}')
    print(f'{"="*50}')

    # 讀取環境變數決定模式
    backfill_start = os.environ.get('BACKFILL_START', '').strip()
    backfill_end   = os.environ.get('BACKFILL_END', '').strip()

    if backfill_start and backfill_end:
        # ── 補抓模式 ──
        print(f'[補抓模式] {backfill_start} ~ {backfill_end}')
        dates = get_weekdays(backfill_start, backfill_end)
        print(f'[補抓模式] 共 {len(dates)} 個工作日：{dates}')
        for i, date_str in enumerate(dates):
            prev = dates[i-1] if i > 0 else None
            process_date(date_str, prev)
            time.sleep(1)  # 避免 API 請求太快
    else:
        # ── 每日模式（今天）──
        today = today_str()
        tz    = datetime.timezone(datetime.timedelta(hours=8))
        yest  = (datetime.datetime.now(tz) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        print(f'[每日模式] 擷取 {today}')
        process_date(today, yest)

    print(f'\n✅ 全部完成！\n')


if __name__ == '__main__':
    main()
