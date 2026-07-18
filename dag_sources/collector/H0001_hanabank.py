import pendulum
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

from dataclasses import dataclass
from sqlalchemy import create_engine, text

import bs4
import numpy as np
import os
import pandas as pd
from typing import List

import U0001_functions as module_UTIL
import U0002_datasets as module_DATA

KST = "Asia/Seoul"

JOB_ID = "H0001"

COLLECT_SLEEP_SEC = 2  # 서버 부하 방지를 위한 요청 간 유휴 시간(초)


@dataclass
class PgConfig:
    host: str
    port: int
    db: str
    user: str
    password: str
    schema: str = "public"
    table: str = "m_calendar"   # 달력 테이블 (기준일 조회용)


def build_pg_config(p_schema: str = "public", p_table: str = "m_calendar") -> PgConfig:
    """
    PostgreSQL 접속 정보를 Airflow Variable 로부터 조회하여 PgConfig 를 생성한다.
    """
    conn_info = module_UTIL.U0001_get_pg_conn_info()
    return PgConfig(
        host=conn_info["host"],
        port=int(conn_info["port"]),
        db=conn_info["db"],
        user=conn_info["user"],
        password=conn_info["password"],
        schema=conn_info.get("schema") or p_schema,
        table=p_table,
    )


BASE_URL = "https://www.kebhana.com/cms/rate/wpfxd651_07i_01.do"
CURRENCIES: List[str] = "USD-EUR-JPY-GBP-AUD-CAD-CHF-CNY-SEK-MXN-NZD-SGD-HKD-NOK".split("-")
WORK_PATH = "/opt/apollon-data/finance-data/work/fx/"

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _param_for(eod_date: str, currency: str) -> dict:
    return {
        "inqType": 0,
        "tmpInqStrDt_d": eod_date,
        "tmpInqStrDtY_m": eod_date[0:4],
        "tmpInqStrDtM_m": eod_date[4:6],
        "tmpInqStrDt_p": eod_date,
        "tmpInqEndDt_p": eod_date,
        "curCd": currency,
        "tmpPbldDvCd": "1",
        "inqDt": eod_date,
        "inqDvCd": "1",
        "requestTarget": "searchContentDiv",
    }

def _parse_html_to_rows(html_content: bytes, eod_date: str, currency: str) -> List[dict]:
    soup = bs4.BeautifulSoup(html_content, "html.parser")
    tbodys = soup.find_all("tbody")
    if not tbodys:
        return list()
    price_trs = bs4.BeautifulSoup(str(tbodys[0]), "html.parser").find_all("tr")
    rows = list()
    for tr in price_trs:
        tds = bs4.BeautifulSoup(str(tr), "html.parser").find_all("td")
        if len(tds) > 10:
            try:
                rows.append({
                    "eod_date": eod_date,
                    "item_code": currency.lower(),
                    "seq": int(tds[0].text),
                    "time": tds[1].text.strip(),
                    "price_buy_cash": float(tds[2].text.replace(",", "")),
                    "price_sell_cash": float(tds[3].text.replace(",", "")),
                    "price_buy_send": float(tds[4].text.replace(",", "")),
                    "price_sell_send": float(tds[5].text.replace(",", "")),
                    "price_sell_cheque": float(tds[6].text.replace(",", "")),
                    "price_std_ratio": float(tds[7].text.replace(",", "")),
                    "price_updown_ratio": float(tds[8].text.replace(",", "")),
                    "price_exchange_ratio": float(tds[9].text.replace(",", "")),
                    "price_usd_dollar": float(tds[10].text.replace(",", "")),
                })
            except Exception:
                continue
    return rows

def _pivot_daily_ohlc(df_one_ccy: pd.DataFrame, fx_code: str) -> pd.DataFrame:
    if df_one_ccy.empty:
        return pd.DataFrame()
    df = df_one_ccy.copy()
    df["seq"] = df["seq"].astype(int)
    df["eod_date"] = df["eod_date"].astype(int)
    g = df.groupby(["eod_date"]).agg(
        seq=("seq", np.max),
        price_high=("price_std_ratio", np.max),
        price_low=("price_std_ratio", np.min),
    ).reset_index()
    df_last = df.set_index(["eod_date", "seq"])
    g2 = g.set_index(["eod_date", "seq"]).join(df_last, how="left").reset_index()
    df_open = df[df["seq"] == 1][["eod_date", "price_std_ratio"]].rename(
        columns={"price_std_ratio": "price_open"}
    )
    g2 = g2.merge(df_open, on="eod_date", how="left")
    out = g2.set_index("eod_date")[[
        "price_open", "price_high", "price_low", "price_std_ratio",
        "price_buy_cash", "price_sell_cash", "price_buy_send", "price_sell_send",
        "price_sell_cheque", "price_updown_ratio", "price_exchange_ratio", "price_usd_dollar",
    ]].copy()
    out.columns = [
        f"{fx_code}_price_open", f"{fx_code}_price_high", f"{fx_code}_price_low", f"{fx_code}_price_close",
        f"{fx_code}_price_buy_cash", f"{fx_code}_price_sell_cash", f"{fx_code}_price_buy_send", f"{fx_code}_price_sell_send",
        f"{fx_code}_price_sell_cheque", f"{fx_code}_price_updown_ratio", f"{fx_code}_price_exchange_ratio", f"{fx_code}_price_usd_dollar",
    ]
    return out


@dag(
    dag_id="H0001_hanabank",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[module_DATA.DS_A0003],
    catchup=False,
    tags=["H0001_hanabank", "Hana Bank", "FX"],
)
def H0001_hanabank():

    @task.short_circuit(inlets=[module_DATA.DS_A0003])
    def check_available(p_dummy=None) -> bool:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        base_date = ctx["ds_nodash"]
        currency = "USD"
        module_UTIL.U0001_db_logging(
            JOB_ID, 'check_available', 'STARTED', 'check_available is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"base_date": base_date, "currency": currency},
        )
        try:
            res = module_UTIL.U0001_collect(BASE_URL, _param_for(base_date, currency), p_sleep_time=COLLECT_SLEEP_SEC)
            rows = _parse_html_to_rows(res.content, base_date, currency)
            ok = len(rows) > 0
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_available', 'SUCCESS', f'check_available is finished(ok={ok}).',
                p_section_count=len(rows), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"base_date": base_date, "currency": currency, "ok": ok},
            )
            return ok
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_available', 'FAILED', f'check_available failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"base_date": base_date, "currency": currency, "error": str(e), "error_type": type(e).__name__},
            )
            return False

    @task
    def get_collect_dates(cfg: PgConfig) -> list[int]:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        base_date = int(ctx["ds_nodash"])
        module_UTIL.U0001_db_logging(
            JOB_ID, 'get_collect_dates', 'STARTED', 'get_collect_dates is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"base_date": base_date, "schema": cfg.schema, "table": cfg.table},
        )
        try:
            engine = create_engine(
                f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.db}"
            )
            sql = """
                SELECT eod_date::integer AS eod_date, off_yn
                FROM public.m_calendar
                WHERE off_yn = 'N'
                  AND eod_date::integer >= 20010101
                  AND eod_date::integer <= :base_date
                ORDER BY eod_date ASC
            """
            with engine.begin() as conn:
                df = pd.read_sql(text(sql), conn, params={"base_date": base_date})
            if df.empty:
                t_end = pendulum.now(KST)
                module_UTIL.U0001_db_logging(
                    JOB_ID, 'get_collect_dates', 'SUCCESS', 'get_collect_dates: no trading dates.',
                    p_section_count=0, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                    p_extra={"base_date": base_date},
                )
                return list()
            dates = list(df["eod_date"].unique())
            dates.reverse()
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'get_collect_dates', 'SUCCESS', f'get_collect_dates is finished(count={len(dates)}, desc).',
                p_section_count=len(dates), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"base_date": base_date},
            )
            return dates
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'get_collect_dates', 'FAILED', f'get_collect_dates failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"base_date": base_date, "error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task
    def fetch_and_pivot_currency(currency: str, dates_desc: List[int]) -> str:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        _ensure_dir(WORK_PATH)
        raw_csv = os.path.join(WORK_PATH, f"hanabank-{currency}.csv")
        pivot_csv = os.path.join(WORK_PATH, f"pivot-{currency}.csv")
        module_UTIL.U0001_db_logging(
            JOB_ID, 'fetch_and_pivot_currency', 'STARTED', f'fetch_and_pivot_currency is started for {currency}.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"currency": currency, "raw_csv": raw_csv, "pivot_csv": pivot_csv},
        )
        try:
            if os.path.exists(raw_csv):
                df_cur = pd.read_csv(raw_csv, dtype={"eod_date": str}).drop_duplicates()
                existing_dates = sorted(df_cur["eod_date"].unique()) if not df_cur.empty else []
                keep_until = max(0, len(existing_dates) - 10)
                keep_set = set(existing_dates[:keep_until])
                df_cur = df_cur[df_cur["eod_date"].isin(keep_set)]
                set_collected = keep_set
            else:
                df_cur = pd.DataFrame()
                set_collected = set()

            pages = 0
            for int_date in dates_desc:
                eod_date = str(int_date)
                if eod_date in set_collected:
                    continue
                try:
                    res = module_UTIL.U0001_collect(BASE_URL, _param_for(eod_date, currency), p_sleep_time=COLLECT_SLEEP_SEC)
                    rows = _parse_html_to_rows(res.content, eod_date, currency)
                    if not rows:
                        continue
                    df_part = pd.DataFrame(rows)
                    df_cur = pd.concat([df_cur, df_part], ignore_index=True)
                    pages += 1
                except Exception:
                    continue

            raw_rows = 0
            pivot_rows = 0
            eod_min = None
            eod_max = None

            if not df_cur.empty:
                df_cur["seq"] = df_cur["seq"].astype(int)
                df_cur["eod_date"] = df_cur["eod_date"].astype(str)
                df_cur = df_cur.sort_values(by=["eod_date", "seq"], ascending=[True, True])
                df_cur.to_csv(raw_csv, index=False)
                raw_rows = int(df_cur.shape[0])

                fx_code = currency.upper()
                df_cur2 = df_cur.copy()
                df_cur2["eod_date"] = df_cur2["eod_date"].astype(int)
                eod_min = int(df_cur2["eod_date"].min())
                eod_max = int(df_cur2["eod_date"].max())

                df_pivot = _pivot_daily_ohlc(df_cur2, fx_code)
                df_pivot.to_csv(pivot_csv)
                pivot_rows = int(df_pivot.shape[0])
            else:
                pd.DataFrame().to_csv(pivot_csv)

            module_UTIL.U0001_db_save_collect_result(
                JOB_ID, p_category='fx', p_item_code=currency, p_item_name=currency,
                p_row_count=pivot_rows or raw_rows, p_eod_date_min=eod_min, p_eod_date_max=eod_max,
                p_file_path=pivot_csv, p_context=ctx,
            )
            module_UTIL.U0001_logging(
                f"[{JOB_ID}][fetch_and_pivot_currency] currency={currency} pages={pages} "
                f"raw_rows={raw_rows} pivot_rows={pivot_rows} range=[{eod_min},{eod_max}]"
            )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'fetch_and_pivot_currency', 'SUCCESS',
                f'fetch_and_pivot_currency is finished for {currency}.',
                p_section_count=pivot_rows or raw_rows, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"currency": currency, "pages": pages, "raw_rows": raw_rows, "pivot_rows": pivot_rows,
                         "eod_min": eod_min, "eod_max": eod_max},
            )
            return pivot_csv
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'fetch_and_pivot_currency', 'FAILED', f'fetch_and_pivot_currency failed for {currency}: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"currency": currency, "error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task(outlets=[module_DATA.DS_H0001])
    def task_to_close(p_dummy=None):
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'task_to_close', 'STARTED', 'task_to_close is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            module_UTIL.U0001_logging(
                f"[{JOB_ID}][task_to_close] H0001_hanabank pipeline finished. currencies={len(CURRENCIES)}"
            )
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'task_to_close', 'SUCCESS', 'task_to_close is finished. pipeline finished.',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"currencies": len(CURRENCIES)},
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

    flag_available = check_available(module_DATA.DS_A0003)
    dates_desc = get_collect_dates(build_pg_config(p_schema="public", p_table="m_calendar"))

    fetch_tasks = []
    for ccy in CURRENCIES:
        t = fetch_and_pivot_currency.override(task_id=f"fetch_pivot_{ccy}")(ccy, dates_desc)
        fetch_tasks.append(t)

    batch_size = 3
    batches = [fetch_tasks[i:i+batch_size] for i in range(0, len(fetch_tasks), batch_size)]

    batch_barriers = []
    if batches:
        for t in batches[0]:
            flag_available >> t

        for idx, batch in enumerate(batches):
            barrier = EmptyOperator(
                task_id=f"batch_{idx}_done",
                trigger_rule="all_done",
            )
            for t in batch:
                t >> barrier
            batch_barriers.append(barrier)

        for idx in range(1, len(batches)):
            prev_barrier = batch_barriers[idx - 1]
            for t in batches[idx]:
                prev_barrier >> t

        t_close = task_to_close.override(trigger_rule="all_done")()
        batch_barriers[-1] >> t_close
    else:
        t_close = task_to_close.override(trigger_rule="all_done")()
        flag_available >> t_close

H0001_hanabank()
