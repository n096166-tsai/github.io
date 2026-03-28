#!/usr/bin/env python3
"""
個股期貨資料爬蟲
每日下午 6:00 從 wantgoo.com 擷取個股期貨交易資訊
並計算成交量遞增前 20 名
"""

import json
import os
import sys
import time
import datetime
import requests
from bs4 import BeautifulSoup

# ── 設定 ────────────────────────────────────────────────
DATA_DIR    = os.path.join(os.path.dirname(__file__), 'data')
TARGET_URL  = 'https://www.wantgoo.com/futures/stock-futures-ranking'
TOP_N       = 20
HEADERS     = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8',
    'Referer': 'https://www.wantgoo.com/',
}


def ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def today_str():
    tz = datetime.timezone(datetime.timedelta(hours=8))  # 台灣時區
    return datetime.datetime.now(tz).strftime('%Y-%m-%d')


def now_str():
    tz = datetime.timezone(datetime.timedelta(hours=8))
    return datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M')


def load_json(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ── 爬取資料 ─────────────────────────────────────────────
def fetch_data():
    print(f'[爬蟲] 開始擷取 {TARGET_URL}')
    try:
        resp = requests.get(TARGET_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        print(f'[爬蟲] HTTP {resp.status_code}，取得 {len(resp.text)} 字元')
        return resp.text
    except requests.RequestException as e:
        print(f'[錯誤] 擷取失敗：{e}')
        return None


def parse_data(html):
    """解析 wantgoo 個股期貨排行頁面"""
    soup = BeautifulSoup(html, 'html.parser')
    records = []

    # wantgoo 的資料通常在 table 中，嘗試多種選擇器
    table = (
        soup.select_one('table.table-stock') or
        soup.select_one('table.ranking-table') or
        soup.select_one('div.futures-ranking table') or
        soup.select_one('table')
    )

    if not table:
        print('[解析] 找不到表格，嘗試 JSON API')
        return parse_from_json_api()

    rows = table.select('tbody tr')
    print(f'[解析] 找到 {len(rows)} 筆資料列')

    for row in rows:
        cols = row.select('td')
        if len(cols) < 4:
            continue
        try:
            # 依 wantgoo 實際欄位順序解析（可能需依網頁更新調整）
            code_name = cols[0].get_text(strip=True)
            # 嘗試分離代碼與名稱
            code, name = '', code_name
            for td in cols:
                a = td.select_one('a')
                if a:
                    name = a.get_text(strip=True)
                    href = a.get('href', '')
                    # 從連結取代碼
                    parts = [p for p in href.split('/') if p]
                    if parts:
                        code = parts[-1]
                    break

            def safe_float(s):
                try:
                    return float(s.replace(',', '').replace('%', '').replace('+', '').strip())
                except Exception:
                    return None

            volume        = safe_float(cols[1].get_text(strip=True)) if len(cols) > 1 else None
            price         = safe_float(cols[2].get_text(strip=True)) if len(cols) > 2 else None
            price_change  = safe_float(cols[3].get_text(strip=True)) if len(cols) > 3 else None
            open_interest = safe_float(cols[4].get_text(strip=True)) if len(cols) > 4 else None

            if not name or volume is None:
                continue

            records.append({
                'code':         code,
                'name':         name,
                'volume':       volume,
                'price':        price,
                'price_change': price_change,
                'open_interest': open_interest,
            })
        except Exception as e:
            print(f'[解析] 某列解析失敗：{e}')
            continue

    print(f'[解析] 成功解析 {len(records)} 筆')
    return records


def parse_from_json_api():
    """嘗試直接取 wantgoo 的 JSON API（備用方案）"""
    api_urls = [
        'https://www.wantgoo.com/futures/api/stock-futures-ranking',
        'https://www.wantgoo.com/api/futures/stock-futures-ranking',
    ]
    for url in api_urls:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                print(f'[API] 成功從 {url} 取得資料')
                # 依實際 API 格式解析，此為通用處理
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    for key in ('data', 'list', 'items', 'result'):
                        if key in data and isinstance(data[key], list):
                            return data[key]
        except Exception as e:
            print(f'[API] {url} 失敗：{e}')
    return []


# ── 計算量增排行 ──────────────────────────────────────────
def calc_volume_growth_ranking(today_records, yesterday_records):
    """
    計算成交量遞增排行
    - 比較今日與昨日成交量，計算成長率
    - 若無昨日資料，則以今日成交量排序
    """
    # 建立昨日索引（以 name 或 code 對應）
    yest_map = {}
    if yesterday_records:
        for r in yesterday_records:
            key = r.get('code') or r.get('name')
            if key:
                yest_map[key] = r

    result = []
    for r in today_records:
        key = r.get('code') or r.get('name')
        yest = yest_map.get(key)

        vol_today = r.get('volume') or 0
        vol_yest  = (yest.get('volume') or 0) if yest else 0

        if vol_yest > 0:
            vol_change_pct = round((vol_today - vol_yest) / vol_yest * 100, 2)
        else:
            vol_change_pct = None  # 無前日資料

        price_chg = r.get('price_change')
        price_val = r.get('price')
        price_chg_pct = None
        if price_val and price_chg is not None:
            base = price_val - price_chg
            if base != 0:
                price_chg_pct = round(price_chg / base * 100, 2)

        result.append({
            **r,
            'volume_change_pct': vol_change_pct,
            'price_change_pct':  price_chg_pct,
        })

    # 排序：有昨日資料的以量增幅降序，無昨日資料的以今日量降序排在後面
    has_yest    = [r for r in result if r['volume_change_pct'] is not None]
    no_yest     = [r for r in result if r['volume_change_pct'] is None]
    has_yest.sort(key=lambda x: x['volume_change_pct'], reverse=True)
    no_yest.sort(key=lambda x: x.get('volume') or 0, reverse=True)

    ranked = (has_yest + no_yest)[:TOP_N]
    return ranked


# ── 更新 index.json ───────────────────────────────────────
def update_index(date):
    idx_path = os.path.join(DATA_DIR, 'index.json')
    idx = load_json(idx_path) or {'dates': []}
    dates = idx.get('dates', [])
    if date not in dates:
        dates.append(date)
        dates.sort(reverse=True)
    idx['dates'] = dates
    idx['last_updated'] = now_str()
    save_json(idx_path, idx)
    print(f'[索引] index.json 已更新，共 {len(dates)} 筆記錄')


# ── 主程式 ────────────────────────────────────────────────
def main():
    ensure_dir()
    today = today_str()
    print(f'\n{"="*50}')
    print(f' 個股期貨爬蟲  {now_str()}')
    print(f'{"="*50}')

    # 爬取今日資料
    html = fetch_data()
    if not html:
        print('[中止] 無法取得網頁內容')
        sys.exit(1)

    today_records = parse_data(html)
    if not today_records:
        print('[中止] 解析結果為空，請確認網頁結構是否更新')
        sys.exit(1)

    # 儲存今日原始資料
    raw_path = os.path.join(DATA_DIR, f'raw_{today}.json')
    save_json(raw_path, {
        'date':    today,
        'fetched': now_str(),
        'records': today_records,
    })
    print(f'[儲存] 原始資料 → {raw_path}')

    # 讀取昨日資料
    tz = datetime.timezone(datetime.timedelta(hours=8))
    yesterday = (datetime.datetime.now(tz) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    yest_path = os.path.join(DATA_DIR, f'raw_{yesterday}.json')
    yest_data = load_json(yest_path)
    yesterday_records = yest_data.get('records', []) if yest_data else []
    print(f'[前日] {yesterday} 資料：{len(yesterday_records)} 筆')

    # 計算量增排行
    ranking = calc_volume_growth_ranking(today_records, yesterday_records)
    print(f'[排行] 計算完成，前 {len(ranking)} 名：')
    for i, r in enumerate(ranking[:5], 1):
        pct_str = f"{r['volume_change_pct']:+.1f}%" if r['volume_change_pct'] is not None else 'N/A'
        print(f'  {i}. {r["name"]} 量增 {pct_str}  成交量 {r.get("volume")}')

    # 儲存排行資料
    rank_path = os.path.join(DATA_DIR, f'ranking_{today}.json')
    save_json(rank_path, {
        'date':    today,
        'fetched': now_str(),
        'ranking': ranking,
    })
    print(f'[儲存] 排行資料 → {rank_path}')

    # 更新索引
    update_index(today)
    print(f'\n✅ 完成！今日排行已儲存。\n')


if __name__ == '__main__':
    main()
