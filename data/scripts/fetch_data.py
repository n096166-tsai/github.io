#!/usr/bin/env python3
"""
個股期貨資料爬蟲 v2
改用台灣期交所（TAIFEX）官方公開 API
每日計算成交量遞增前 20 名
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

# 期交所個股期貨日交易資料 API
# queryType=2 = 個股期貨, queryDate = YYYY/MM/DD
TAIFEX_API = (
    'https://www.taifex.com.tw/cht/3/futDataDown'
    '?down_type=1&queryStartDate={date}&queryEndDate={date}'
    '&commodity_id=SF'
)

# 備用：期交所開放資料 JSON API
TAIFEX_JSON_API = (
    'https://opendata.taifex.com.tw/v1/DailyFuturesDate'
    '?MarketCode=0&CommodityID=SF&Date={date_nodash}'
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


# ── 方法一：期交所開放資料 JSON API ──────────────────────
def fetch_taifex_opendata(date_str):
    """
    date_str: '2025-01-15'
    回傳 list of dict，每筆為一個個股期貨契約
    """
    date_nodash = date_str.replace('-', '')
    url = TAIFEX_JSON_API.format(date_nodash=date_nodash)
    print(f'[API-1] 嘗試期交所開放資料：{url}')
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        print(f'[API-1] 回傳 {len(data)} 筆')
        records = []
        for row in data:
            vol = safe_float(row.get('Volume') or row.get('TradingVolume') or row.get('volume'))
            name = (row.get('ContractName') or row.get('Name') or
                    row.get('CommodityName') or row.get('name') or '')
            code = (row.get('ContractCode') or row.get('Code') or
                    row.get('CommodityID') or row.get('code') or '')
            price = safe_float(row.get('SettlementPrice') or row.get('Close') or
                               row.get('close') or row.get('price'))
            oi = safe_float(row.get('OpenInterest') or row.get('openInterest'))
            if not name or vol is None:
                continue
            records.append({
                'code':          code,
                'name':          name,
                'volume':        vol,
                'price':         price,
                'open_interest': oi,
            })
        return records
    except Exception as e:
        print(f'[API-1] 失敗：{e}')
        return []


# ── 方法二：期交所每日行情下載（CSV 格式）─────────────────
def fetch_taifex_csv(date_str):
    """備用方案：下載期交所 CSV 日行情"""
    date_slash = date_str.replace('-', '/')
    url = TAIFEX_API.format(date=date_slash)
    print(f'[API-2] 嘗試期交所 CSV：{url}')
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        # 嘗試用 big5 / utf-8 解碼
        for enc in ('big5', 'utf-8-sig', 'utf-8'):
            try:
                text = resp.content.decode(enc)
                break
            except Exception:
                continue
        else:
            text = resp.text

        records = []
        lines = text.strip().split('\n')
        print(f'[API-2] 取得 {len(lines)} 行')
        # 跳過標頭，解析資料行
        for line in lines[1:]:
            cols = [c.strip().strip('"') for c in line.split(',')]
            if len(cols) < 6:
                continue
            try:
                # 欄位順序依期交所格式：日期,契約,到期月,開盤,最高,最低,收盤,漲跌,漲跌%,成交量,結算價,未平倉
                name = cols[1] if len(cols) > 1 else ''
                vol  = safe_float(cols[9]) if len(cols) > 9 else None
                price = safe_float(cols[6]) if len(cols) > 6 else None
                oi   = safe_float(cols[11]) if len(cols) > 11 else None
                if not name or vol is None:
                    continue
                records.append({
                    'code':          cols[1],
                    'name':          name,
                    'volume':        vol,
                    'price':         price,
                    'open_interest': oi,
                })
            except Exception:
                continue
        print(f'[API-2] 解析 {len(records)} 筆')
        return records
    except Exception as e:
        print(f'[API-2] 失敗：{e}')
        return []


# ── 方法三：期交所另一個公開JSON端點 ──────────────────────
def fetch_taifex_alt(date_str):
    """第三備用：期交所 JSON 格式日行情"""
    date_nodash = date_str.replace('-', '')
    urls = [
        f'https://opendata.taifex.com.tw/v1/DailyFuturesDate?MarketCode=0&Date={date_nodash}',
        f'https://opendata.taifex.com.tw/v1/DailyFutures?Date={date_nodash}',
    ]
    for url in urls:
        print(f'[API-3] 嘗試：{url}')
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    print(f'[API-3] 取得 {len(data)} 筆，第一筆欄位：{list(data[0].keys())}')
                    records = []
                    for row in data:
                        # 動態找成交量欄位
                        vol = None
                        for k in row:
                            if 'vol' in k.lower() or '成交量' in k or 'volume' in k.lower():
                                vol = safe_float(row[k])
                                if vol is not None:
                                    break
                        name = ''
                        for k in row:
                            if 'name' in k.lower() or '名稱' in k or '契約' in k:
                                name = str(row[k]).strip()
                                if name:
                                    break
                        if not name or vol is None:
                            continue
                        records.append({
                            'code':          row.get('CommodityID', ''),
                            'name':          name,
                            'volume':        vol,
                            'price':         None,
                            'open_interest': None,
                        })
                    if records:
                        return records
        except Exception as e:
            print(f'[API-3] {url} 失敗：{e}')
    return []


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
    print(f'[索引] 已更新，共 {len(dates)} 天記錄')


def main():
    ensure_dir()
    today = today_str()
    print(f'\n{"="*50}')
    print(f' 個股期貨爬蟲 v2  {now_str()}')
    print(f'{"="*50}')

    # 依序嘗試三個方法
    records = fetch_taifex_opendata(today)
    if not records:
        records = fetch_taifex_csv(today)
    if not records:
        records = fetch_taifex_alt(today)

    if not records:
        print('[中止] 三個 API 都無法取得資料')
        print('可能原因：今日為非交易日，或 API 尚未更新（盤後約 17:30 更新）')
        # 非交易日不算失敗，正常結束
        sys.exit(0)

    # 儲存原始資料
    raw_path = os.path.join(DATA_DIR, f'raw_{today}.json')
    save_json(raw_path, {'date': today, 'fetched': now_str(), 'records': records})
    print(f'[儲存] 原始資料 {len(records)} 筆 → {raw_path}')

    # 讀昨日資料
    tz = datetime.timezone(datetime.timedelta(hours=8))
    yest = (datetime.datetime.now(tz) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    yest_data = load_json(os.path.join(DATA_DIR, f'raw_{yest}.json'))
    yest_records = yest_data.get('records', []) if yest_data else []

    # 計算排行
    ranking = calc_ranking(records, yest_records)
    print(f'[排行] 前 {len(ranking)} 名：')
    for i, r in enumerate(ranking[:5], 1):
        p = f"{r['volume_change_pct']:+.1f}%" if r['volume_change_pct'] is not None else 'N/A'
        print(f'  {i}. {r["name"]}  量增{p}  成交{r.get("volume")}')

    rank_path = os.path.join(DATA_DIR, f'ranking_{today}.json')
    save_json(rank_path, {'date': today, 'fetched': now_str(), 'ranking': ranking})
    print(f'[儲存] 排行 → {rank_path}')

    update_index(today)
    print(f'\n✅ 完成！\n')


if __name__ == '__main__':
    main()
