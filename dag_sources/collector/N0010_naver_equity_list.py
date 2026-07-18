# -*- coding: utf-8 -*-
"""
N0010_naver_equity_list
- NAVER 시가총액 페이지에서 KOSPI/KOSDAQ 종목 목록을 수집한다.
- 기존 WAI_POC.py 의 POCBR_000311(종목 목록 수집 - NAVER) 로직을 Airflow TaskFlow로 변환.
- 수집 결과는 input_KOSPI-YYYYMMDD.csv / input_KOSDAQ-YYYYMMDD.csv 로 저장되며,
  N0011/N0012/N0013 등 하위 파이프라인이 이 파일에서 종목코드 목록을 읽어 사용한다.
- 공용 함수(U0001_collect/U0001_logging/U0001_db_logging 등)와 Dataset 정의는
  U0001_functions.py / U0002_datasets.py 를 그대로 사용하며, 이 파일에서는 중복 정의하지 않는다.
  (원본 POCBR_000001 HTML 수집 함수 → module_UTIL.U0001_collect 로 대체)
- A0003_build_calendar.py 와 동일한 방식으로 DB 접속정보(Airflow Variable)와
  Task 실행 로그(dag_job_log, Task별 STARTED/SUCCESS/FAILED)를 사용한다.
- NAVER 서버 부하 방지를 위해 페이지 수집 사이에 유휴 시간(sleep)을 둔다
  (module_UTIL.U0001_collect 의 p_sleep_time 파라미터를 통해 매 요청 후 대기).
"""
import os
import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import U0001_functions as module_UTIL
import U0002_datasets as module_DATA

KST = "Asia/Seoul"

# 작업(Task) 실행 정보를 DB(dag_job_log)에 저장할 때 사용하는 작업 구분 키.
JOB_ID = "N0010"

# 종목 목록 저장 경로 (N0011/N0012/N0013 등이 참조하는 디렉터리와 동일해야 함)
BASE_WORK_DIR = "/opt/apollon-data/finance-data/work/equities/"
os.makedirs(BASE_WORK_DIR, exist_ok=True)

# NAVER 시가총액 목록 페이지
NAVER_MARKET_LIST_URL = "https://finance.naver.com/sise/sise_market_sum.nhn"

# 수집 대상 마켓 (원본 POCBR_000311 의 sosok 코드: 0=코스피, 1=코스닥)
LIST_MARKET = [
    {"market": "KOSPI", "sosok": "0"},
    {"market": "KOSDAQ", "sosok": "1"},
]

# 페이지 수집 상한 및 서버 부하 방지용 유휴 시간(초)
PAGE_MAX = 9999
COLLECT_SLEEP_SEC = 2


@dag(
    dag_id="N0010_naver_equity_list",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[module_DATA.DS_A0003],  # 달력(영업일) 데이터 준비 후 실행
    catchup=False,
    tags=["N0010_naver_equity_list", "NAVER", "equity list"],
)
def N0010_naver_equity_list():
    """
    파이프라인 개요
    1) NAVER 시가총액 페이지 가용성 점검 (ShortCircuit)
    2) 마켓(KOSPI/KOSDAQ)별로 페이지를 순회하며 종목 목록 수집 → input_<MARKET>-YYYYMMDD.csv 저장
    3) 마감: 수집 결과 요약을 Airflow 로그로 출력 + dag_job_log 기록 + DS_N0010 방출
    """

    @task.short_circuit(inlets=[module_DATA.DS_A0003])
    def check_available(p_dummy=None) -> bool:
        import bs4

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'check_available', 'STARTED', 'check_available is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            param = {"sosok": "0", "page": 1}
            results = module_UTIL.U0001_collect(NAVER_MARKET_LIST_URL, param, p_sleep_time=COLLECT_SLEEP_SEC)
            soup = bs4.BeautifulSoup(results.content, "html.parser")
            tables = soup.find_all("table", {"class": "type_2"})
            ok = len(tables) > 0

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_available', 'SUCCESS' if ok else 'FAILED',
                f'check_available is finished. ok={ok}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
            )
            return ok
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_available', 'FAILED', f'check_available failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task
    def collect_market_list(market_info: dict) -> dict:
        """
        market_info: {"market": "KOSPI"|"KOSDAQ", "sosok": "0"|"1"}
        NAVER 시가총액 페이지를 페이지 단위로 순회하며 종목 목록을 수집해
        input_<MARKET>-YYYYMMDD.csv 로 저장한다. (원본 POCBR_000311 로직)
        """
        import bs4
        import pandas

        ctx = get_current_context()
        market = market_info["market"]
        sosok = market_info["sosok"]
        yyyymmdd = ctx["ds_nodash"]

        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, f'collect_{market}', 'STARTED', f'collect_market_list({market}) is started.',
            p_context=ctx, p_start_time=t_start, p_extra={"market": market, "sosok": sosok},
        )

        def _num(text_value: str) -> float:
            text_value = str(text_value).strip().replace(",", "").replace("N/A", "0")
            return float(text_value) if text_value != "" else 0.0

        try:
            df_market = pandas.DataFrame()
            for page_no in range(1, PAGE_MAX):
                list_rows = []
                param = {"sosok": sosok, "page": page_no}
                results = module_UTIL.U0001_collect(NAVER_MARKET_LIST_URL, param, p_sleep_time=COLLECT_SLEEP_SEC)
                equity_table = bs4.BeautifulSoup(results.content, "html.parser").find_all("table", {"class": "type_2"})
                price_trs = bs4.BeautifulSoup(str(equity_table), "html.parser").find_all("tr")
                for price_tr in price_trs:
                    row = bs4.BeautifulSoup(str(price_tr), "html.parser").find_all("td")
                    if len(row) > 4:
                        try:
                            list_rows.append({
                                "market": market,
                                "equity_code": str(row[1].find("a")["href"]).split("=")[1],
                                "equity_name": row[1].text,
                                "face_value": _num(row[5].text),
                                "market_capitalization": _num(row[6].text),
                                "number_of_listed_shares": _num(row[7].text),
                                "ratio_of_foreigners": _num(row[8].text),
                                "volume": _num(row[9].text),
                                "price": _num(row[2].text),
                                "per": _num(row[10].text),
                                "roe": _num(row[11].text),
                            })
                        except Exception as e:
                            module_UTIL.U0001_logging(market, page_no, "row_parse_error", str(e))

                df_page = pandas.DataFrame(list_rows)
                module_UTIL.U0001_logging(
                    JOB_ID, market, "page", page_no,
                    f"df_page={tuple(df_page.shape)}", f"df_market={tuple(df_market.shape)}"
                )

                if df_page.shape[0] == 0:
                    # 더 이상 종목이 없는 페이지에 도달 → 수집 종료
                    module_UTIL.U0001_logging(JOB_ID, market, page_no, "empty_page -> stop paging")
                    break

                df_market = pandas.concat([df_market, df_page], sort=False)

            out_path = os.path.join(BASE_WORK_DIR, f"input_{market}-{yyyymmdd}.csv")
            df_market.to_csv(out_path, index=None)

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, f'collect_{market}', 'SUCCESS', f'collect_market_list({market}) is finished.',
                p_section_count=int(df_market.shape[0]), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"market": market, "output_path": out_path},
            )
            return {"market": market, "path": out_path, "count": int(df_market.shape[0])}
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, f'collect_{market}', 'FAILED', f'collect_market_list({market}) failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"market": market, "error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task(outlets=[module_DATA.DS_N0010])
    def finalize(results: list) -> str:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'finalize', 'STARTED', 'finalize is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            total = sum(int((r or {}).get("count") or 0) for r in results)

            # Airflow Task 로그 화면에서 바로 확인할 수 있도록 수집 결과 출력
            for r in results:
                if not r:
                    continue
                module_UTIL.U0001_logging(
                    f"[{JOB_ID}] market={r.get('market')} count={r.get('count')} path={r.get('path')}"
                )
            module_UTIL.U0001_logging(f"[{JOB_ID}] total_count={total}")

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'finalize', 'SUCCESS', 'finalize is finished.',
                p_section_count=total, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"results": results},
            )
            return "ok"
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'finalize', 'FAILED', f'finalize failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # ----- 파이프라인 -----
    flag_available = check_available(module_DATA.DS_A0003)

    collected = []
    for market_info in LIST_MARKET:
        t = collect_market_list.override(task_id=f"collect_{market_info['market']}")(market_info)
        flag_available >> t
        collected.append(t)

    fin = finalize(collected)
    collected >> fin


N0010_naver_equity_list()
