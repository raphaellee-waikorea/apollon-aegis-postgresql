# -*- coding: utf-8 -*-
import pendulum
from typing import Any, Dict, List, Optional

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import bs4
import json
import os
import pandas as pd
import re
import time
import cloudscraper

import U0001_functions as module_UTIL
import U0002_datasets as module_DATA

# ─────────────────────────────────────────────────────────────────────────────
# 공통 상수/설정
# ─────────────────────────────────────────────────────────────────────────────
KST = "Asia/Seoul"

# 작업(Task) 실행 정보를 DB(dag_job_log)에 저장할 때 사용하는 작업 구분 키.
# 추후 다른 작업(A0004, A0005, ...)을 추가하면 각 DAG 파일에서 이 값만 다르게 지정하면 된다.
JOB_ID = "I0001"

# HTTP 요청 사이 최소 대기 시간(초). investing.com 서버에 연속 요청이 몰리지 않도록
# 매 요청 뒤에 이 시간만큼 idle time을 둔다.
COLLECT_SLEEP_SEC = 2

BASE_URL = "https://www.investing.com"
WORK_PATH = "/opt/apollon-data/finance-data/work/investing/"

SEARCH_KEYWORDS: List[str] = [
    "MAL",        # Aluminium Futures
    "MCU",        # Copper Futures
    "TIOc1",      # Iron ore
    "HRCc3",      # US Steel Coil
    "NICKEL",     # Nickel
    "US10YT=X",   # US 10Y Yield
    "XLE", "XLI", "XLY", "XLP", "XLV", "XLF", "XLK", "XLC", "XLU", "XLRE",  # ETFs
    "BADI",       # Baltic Dry
]

CODE_TO_REMOVE = {"BPNI", "BACI"}
NAME_EXCEPT = {
    "Energy_Select_Sector_SPDR_Fund_DRC",
    "Invesco_Energy_SnP_US_Select_Sector_UCITS_ETF",
    "Leverage_Shares_3x_Long_Oil_n_Gas_ETP_Securities",
    "magellan-aerospace-corporation",
}

# ─────────────────────────────────────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────────────────────────────────────
def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _scraper() -> cloudscraper.CloudScraper:
    return cloudscraper.create_scraper()

def _norm_name(name: str) -> str:
    return (name or "").strip().replace("®", "").replace(" ", "_").replace("/", "").lower()

def _extract_results_json(html_text: str) -> list[dict]:
    m = re.search(r"window\.allResultsQuotesDataArray\s*=\s*(\[[\s\S]*?\]);", html_text)
    if not m:
        return []
    try:
        arr = json.loads(m.group(1))
        return arr if isinstance(arr, list) else []
    except Exception:
        return []

def _parse_hist_table(html_text: str) -> pd.DataFrame:
    soup = bs4.BeautifulSoup(html_text, "html.parser")
    tables = soup.find_all("table", {"class": "freeze-column-w-1 w-full overflow-x-auto text-xs leading-4"})
    if not tables:
        return pd.DataFrame()
    tbody = tables[0].find("tbody")
    if not tbody:
        return pd.DataFrame()

    rows = []
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        try:
            eod_date = pd.to_datetime(tds[0].get_text(strip=True), format="%b %d, %Y").strftime("%Y%m%d")
            rows.append({
                "eod_date": int(eod_date),
                "price_close": float(tds[1].get_text(strip=True).replace(",", "")),
                "price_open":  float(tds[2].get_text(strip=True).replace(",", "")),
                "price_high":  float(tds[3].get_text(strip=True).replace(",", "")),
                "price_low":   float(tds[4].get_text(strip=True).replace(",", "")),
            })
        except Exception:
            continue
    return pd.DataFrame(rows)

def _top_search_result(
    html_text: str,
    exclude_names: set[str] | None = None,
    preferred_name_substrings: Optional[List[str]] = None,
    preferred_types: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    exclude_names = exclude_names or set()
    pref_subs = [s.lower() for s in (preferred_name_substrings or [])]
    pref_types = set((preferred_types or []))

    json_rows = _extract_results_json(html_text)
    candidates: list[dict] = []

    if json_rows:
        for row in json_rows:
            code = (row.get("symbol") or "").strip()
            name = (row.get("name") or "").strip()
            link = row.get("link") or ""
            pair_type = (row.get("pair_type") or "").strip().lower()
            url = BASE_URL + link if link else ""
            if code in CODE_TO_REMOVE:
                continue
            if _norm_name(name) in {_norm_name(x) for x in exclude_names}:
                continue
            candidates.append({"code": code, "name": name, "url": url, "pair_type": pair_type})
    else:
        soup = bs4.BeautifulSoup(html_text, "html.parser")
        anchors = soup.select(".js-inner-all-results-quotes-wrapper a")
        for a in anchors:
            code_tag = a.select_one("span.second")
            name_tag = a.select_one("span.third")
            if not code_tag or not name_tag:
                continue
            code = code_tag.get_text(strip=True)
            name = name_tag.get_text(strip=True)
            url = BASE_URL + a.get("href", "")
            if code in CODE_TO_REMOVE:
                continue
            if _norm_name(name) in {_norm_name(x) for x in exclude_names}:
                continue
            candidates.append({"code": code, "name": name, "url": url, "pair_type": ""})

    if not candidates:
        return []

    if pref_subs:
        for c in candidates:
            n = _norm_name(c.get("name", ""))
            if any(sub in n for sub in pref_subs):
                return [c]

    if pref_types:
        for c in candidates:
            if c.get("pair_type", "").lower() in pref_types:
                return [c]

    return [candidates[0]]

# ─────────────────────────────────────────────────────────────────────────────
# DAG (순차 실행)
# ─────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="I0001_investing",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[module_DATA.DS_A0003],   # 달력 세팅 완료 후 실행
    catchup=False,
    tags=["PBCI_000100", "Investing", "Commodities", "Sectors"],
)
def I0001_investing():

    @task.short_circuit(inlets=[module_DATA.DS_A0003])
    def check_available(p_dummy=None) -> bool:
        """사이트 접근 가능/최소 파싱 확인"""
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'check_available', 'STARTED', 'check_available is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            s = _scraper()
            res = s.get(f"{BASE_URL}/search/?q=MCU", timeout=20)
            time.sleep(COLLECT_SLEEP_SEC)
            ok = (res.status_code == 200) and bool(_top_search_result(res.text, NAME_EXCEPT))

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_available', 'SUCCESS', f'check_available is finished. (ok={ok})',
                p_section_count=1 if ok else 0, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
            )
            return ok
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_available', 'FAILED', f'check_available failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            # 이 Task는 short_circuit 게이트이므로 예외를 올리지 않고 False로 단락시킨다.
            return False

    @task
    def search_top_items(keywords: List[str]) -> List[Dict[str, str]]:
        """키워드별 대표 1건만 선별"""
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'search_top_items', 'STARTED', f'search_top_items is started. (keywords={len(keywords)})',
            p_context=ctx, p_start_time=t_start,
            p_extra={"keywords": keywords},
        )

        _ensure_dir(WORK_PATH)
        s = _scraper()
        items: List[Dict[str, str]] = []

        PREFER: dict[str, dict] = {
            "MAL":    {"preferred_name_substrings": ["aluminium", "aluminum"], "preferred_types": ["commodity"]},
            "NICKEL": {"preferred_name_substrings": ["nickel"], "preferred_types": ["commodity"]},
            "MCU":    {"preferred_name_substrings": ["copper"], "preferred_types": ["commodity"]},
            "TIOc1":  {"preferred_name_substrings": ["iron", "iron_ore"], "preferred_types": ["commodity"]},
            "HRCc3":  {"preferred_name_substrings": ["hot", "steel"], "preferred_types": ["commodity"]},
            "BADI":   {"preferred_name_substrings": ["baltic", "baltic_dry"], "preferred_types": []},
            "US10YT=X": {"preferred_name_substrings": ["10-year", "10_year"], "preferred_types": []},
            "XLE": {"preferred_types": ["etf"]}, "XLI": {"preferred_types": ["etf"]},
            "XLY": {"preferred_types": ["etf"]}, "XLP": {"preferred_types": ["etf"]},
            "XLV": {"preferred_types": ["etf"]}, "XLF": {"preferred_types": ["etf"]},
            "XLK": {"preferred_types": ["etf"]}, "XLC": {"preferred_types": ["etf"]},
            "XLU": {"preferred_types": ["etf"]}, "XLRE": {"preferred_types": ["etf"]},
        }

        try:
            for kw in keywords:
                try:
                    url = f"{BASE_URL}/search/?q={kw}"
                    res = s.get(url, timeout=25)
                    time.sleep(COLLECT_SLEEP_SEC)
                    pref = PREFER.get(kw, {})
                    top1 = _top_search_result(
                        res.text,
                        exclude_names=NAME_EXCEPT,
                        preferred_name_substrings=pref.get("preferred_name_substrings"),
                        preferred_types=pref.get("preferred_types"),
                    )
                    if not top1:
                        items.append({"code": kw, "name": kw, "url": "", "not_found": True})
                    else:
                        one = top1[0]
                        one["not_found"] = False
                        items.append(one)
                except Exception:
                    items.append({"code": kw, "name": kw, "url": "", "not_found": True})

            found = sum(1 for it in items if not it.get("not_found"))
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'search_top_items', 'SUCCESS', f'search_top_items is finished. (found={found}/{len(items)})',
                p_section_count=found, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
            )
            return items
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'search_top_items', 'FAILED', f'search_top_items failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task
    def collect_all_sequential(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        순차 실행 수집기:
        - 각 지수(아이템)별로 히스토리 데이터를 수집하여 개별 CSV로 저장
        - 항목별 수집 결과(코드/명칭/행수/기준일자 범위/파일경로)를 리스트로 반환하여
          이후 task_to_close 에서 dag_collect_result 테이블에 항목 단위로 적재한다.
        """
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_all_sequential', 'STARTED',
            f'collect_all_sequential is started. (items={len(items or [])})',
            p_context=ctx, p_start_time=t_start,
        )

        _ensure_dir(WORK_PATH)
        s = _scraper()
        results: List[Dict[str, Any]] = []

        try:
            for item in (items or []):
                try:
                    code_raw = item.get("code", "")
                    name_raw = item.get("name", "")
                    not_found = item.get("not_found", False)

                    # 건너뛰기 케이스 (진짜 미존재 항목은 시작/종료 로그도 남기지 않음)
                    if not code_raw or not name_raw or not item.get("url") or not_found or code_raw in CODE_TO_REMOVE:
                        continue

                    symbol = code_raw.replace(" ", "_").replace("/", "")
                    commod_name = name_raw.replace(" ", "_").replace("/", "").replace("®", "")
                    url = item["url"]
                    hist_url = (url + "-historical-data") if ("?" not in url) else url.replace("?", "-historical-data?")

                    # ── [시작 로그] (각 지수별 1회)
                    start_msg = f"{commod_name}|{symbol}"
                    module_UTIL.U0001_logging("PBCI_000100", "ITEM_START", start_msg)

                    # 데이터 수집/파싱(중간 로그 없음)
                    try:
                        res = s.get(hist_url, timeout=30)
                        time.sleep(COLLECT_SLEEP_SEC)
                        df_new = _parse_hist_table(res.text)
                    except Exception:
                        df_new = pd.DataFrame()

                    out_csv = os.path.join(WORK_PATH, f"{commod_name}_{symbol}_hist_price.csv")

                    if not df_new.empty:
                        # 기존 파일과 병합(끝부분 4행 보호 로직 유지)
                        if os.path.exists(out_csv):
                            try:
                                old_df = pd.read_csv(out_csv)
                                old_df = old_df.sort_values(by=["eod_date"], ascending=True)
                                if old_df.shape[0] > 4:
                                    old_df = old_df.head(old_df.shape[0] - 4)
                                df_new = pd.concat([old_df, df_new], ignore_index=True)
                            except Exception:
                                pass

                        df_new = df_new.drop_duplicates(subset=["eod_date"]).sort_values(by=["eod_date"], ascending=True)
                        df_new.to_csv(out_csv, index=False)
                        rows = int(df_new.shape[0])
                        eod_min = int(df_new["eod_date"].min())
                        eod_max = int(df_new["eod_date"].max())
                    else:
                        rows = 0
                        eod_min = None
                        eod_max = None
                        out_csv = None

                    # ── [종료 로그] (각 지수별 1회)
                    finish_msg = f"{commod_name}|{symbol} rows={rows} path={out_csv if rows > 0 else 'EMPTY'}"
                    module_UTIL.U0001_logging("PBCI_000100", "ITEM_FINISH", finish_msg)

                    results.append({
                        "code": symbol,
                        "name": commod_name,
                        "rows": rows,
                        "eod_date_min": eod_min,
                        "eod_date_max": eod_max,
                        "file_path": out_csv,
                    })

                except Exception as e:
                    # 아이템 단위 예외도 종료 로그 1회로 취급하고 다음 아이템으로 계속 진행
                    err_msg = f"{item.get('name','?')}|{item.get('code','?')} ERROR: {e}"
                    module_UTIL.U0001_logging("PBCI_000100", "ITEM_FINISH", err_msg)
                    results.append({
                        "code": item.get("code", "?"),
                        "name": item.get("name", "?"),
                        "rows": 0,
                        "eod_date_min": None,
                        "eod_date_max": None,
                        "file_path": None,
                        "error": str(e),
                    })

            total_rows = sum(int(r.get("rows", 0) or 0) for r in results)
            module_UTIL.U0001_logging("PBCI_000100", "PIPELINE_SUMMARY", f"total_rows={total_rows}")

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_all_sequential', 'SUCCESS',
                f'collect_all_sequential is finished. (total_rows={total_rows})',
                p_section_count=total_rows, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
            )
            return results
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_all_sequential', 'FAILED', f'collect_all_sequential failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task(outlets=[module_DATA.DS_I0001])
    def task_to_close(_results: List[Dict[str, Any]]):
        """
        파이프라인 종료 처리:
        - 항목별 수집 결과를 dag_collect_result 테이블에 한 행씩 적재(Superset 등 BI 조회용)
        - Task 실행 로그(dag_job_log)도 함께 기록
        """
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'task_to_close', 'STARTED', 'task_to_close is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            results = _results or []
            total_rows = 0
            for r in results:
                row_count = int(r.get("rows", 0) or 0)
                total_rows += row_count
                module_UTIL.U0001_db_save_collect_result(
                    p_job_id=JOB_ID,
                    p_category="investing",
                    p_item_code=r.get("code", ""),
                    p_item_name=r.get("name", ""),
                    p_row_count=row_count,
                    p_eod_date_min=r.get("eod_date_min"),
                    p_eod_date_max=r.get("eod_date_max"),
                    p_file_path=r.get("file_path"),
                    p_message=r.get("error"),
                    p_context=ctx,
                )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'task_to_close', 'SUCCESS', f'task_to_close is finished. (total_rows={total_rows})',
                p_section_count=total_rows, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
            )
            return True
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'task_to_close', 'FAILED', f'task_to_close failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # ── 파이프라인(완전 순차) ────────────────────────────────────────────────
    ok = check_available(module_DATA.DS_A0003)
    items = search_top_items(SEARCH_KEYWORDS)
    # ShortCircuitTask가 False면 이후 자동 중단
    collect_results = collect_all_sequential(items)
    close_task = task_to_close(collect_results)

    # 의존성 (check_available -> search_top_items -> collect_all_sequential -> task_to_close)
    # (주의) ok >> check_available 형태는 사이클을 만드니 사용하지 않는다.
    ok >> items
    items >> collect_results
    collect_results >> close_task

I0001_investing()
