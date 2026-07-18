# -*- coding: utf-8 -*-
"""
N0014_naver_market
- NAVER 시세/지표: 유가/귀금속/국내금/원자재 + 해외 지수(유럽·아메리카/아시아) 수집
- PBCN_000700/800/900/1000/1100/1200 → Airflow TaskFlow
- 페이지 순회/증분 병합/조기 종료 로직 적용
- 출력은 market 폴더의 output-*.csv로 통일
- 입력(증분) 파일명 규칙:
  * oil:      oil/input_oil_{코드}.csv
  * jewelry:  jewelry/input_jewelry_{코드}.csv
  * material: material/input_material_{코드}.csv
  * indices & gold KR: market/input_market_{키}.csv
- 상위 파이프라인 Dataset inlet: module_DATA.DS_N0013
- Task 실행 정보는 U0001_db_logging() 을 통해 dag_job_log 테이블에, 수집 결과 요약은
  U0001_db_save_collect_result() 를 통해 dag_collect_result 테이블에 각각 저장한다
  (Apache Superset 등 BI 도구에서 공통 조회 가능).
"""

import os
import time
import json
import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import U0001_functions as module_UTIL   # U0001_collect, U0001_logging, U0001_db_logging, U0001_db_save_collect_result 등
import U0002_datasets  as module_DATA   # DS_N0013, DS_N0014 등


# ===== 상수/경로 =====
KST = "Asia/Seoul"

# 작업(Task) 실행 정보를 DB(dag_job_log)에 저장할 때 사용하는 작업 구분 키.
# 추후 다른 작업(N0015, N0016, ...)을 추가하면 각 DAG 파일에서 이 값만 다르게 지정하면 된다.
JOB_ID = "N0014"

# 서버 부하 방지를 위한 요청 간 유휴시간(초). U0001_collect()가 요청마다 이 시간만큼 sleep 한다.
COLLECT_SLEEP_SEC = 2

# 컨테이너 내부 기본값(환경변수로 덮어쓰기 가능)
WORK_DIR = "/opt/apollon-data/finance-data/work"
DIRS = {
    "jewelry":  os.path.join(WORK_DIR, "jewelry"),
    "material": os.path.join(WORK_DIR, "material"),
    "oil":      os.path.join(WORK_DIR, "oil"),
    "market":   os.path.join(WORK_DIR, "market"),
}

# 조기 종료/제어 파라미터
EMPTY_PAGE_BREAK_AFTER   = int(os.environ.get("EMPTY_PAGE_BREAK_AFTER", "3"))   # 빈 페이지 연속 N회 시 종료
STAGNATE_BREAK_AFTER     = int(os.environ.get("STAGNATE_BREAK_AFTER", "5"))     # 증분 없음 연속 N회 시 종료
OLDER_THAN_EXISTING_BREAK_AFTER = int(os.environ.get("OLDER_THAN_EXISTING_BREAK_AFTER", "2"))  # 국내금 전용: 가진 최저일자보다 과거만 연속 N회면 종료
MIN_DATE_BOUNDARY        = int(os.environ.get("MIN_DATE_BOUNDARY", "20010101")) # 최소 하한 날짜
PAGE_MAX                 = int(os.environ.get("PAGE_MAX", "400"))               # 과다 페이지 수집 방지

def _ensure_dirs():
    """런타임에서만 디렉터리 생성 (파싱시 권한 에러 회피)"""
    for d in DIRS.values():
        try:
            os.makedirs(d, exist_ok=True)
        except Exception as e:
            module_UTIL.U0001_logging("ensure_dirs", "warn", d, str(e))

def _now_ts() -> str:
    return str(time.time())

def _log_df_shapes(stage: str, code: str, page_no: int, df_page, df_index) -> None:
    """수집(페이지/병합) 단계별 df shape 로깅"""
    try:
        page_shape = tuple(df_page.shape) if df_page is not None else None
        acc_shape  = tuple(df_index.shape) if df_index is not None else None
        module_UTIL.U0001_logging(
            "shape", stage, f"code={code}", f"page={page_no}",
            f"df_page={page_shape}", f"df_index={acc_shape}"
        )
    except Exception as e:
        module_UTIL.U0001_logging("shape_log_error", stage, code, page_no, str(e))


@dag(
    dag_id="N0014_naver_market",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[module_DATA.DS_N0013],   # 상위 파이프라인 완료 후 실행
    catchup=False,
    tags=["NAVER", "market", "commodities", "indices", "TaskFlow"],
)
def N0014_naver_market():
    """시장 정보 수집(유가/귀금속/국내금/원자재/해외지수) 통합 DAG"""

    # 시작 타임스탬프 + 디렉터리 생성
    @task
    def start_ts() -> str:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'start_ts', 'STARTED', 'start_ts is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            _ensure_dirs()
            ts_val = _now_ts()

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'start_ts', 'SUCCESS', 'start_ts is finished.',
                p_section_count=len(DIRS), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"dirs": list(DIRS.values())},
            )
            return ts_val
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'start_ts', 'FAILED', f'start_ts failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # ==================
    # 유가 (PBCN_000700)
    # ==================
    @task(task_id="collect_oil")
    def collect_oil() -> str:
        import bs4, pandas as pd

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        start_time = time.time()
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_oil', 'STARTED', 'collect_oil is started.',
            p_context=ctx, p_start_time=t_start,
        )
        module_UTIL.U0001_logging("PBCN_000700", "started", ctx.get("ds_nodash"))

        try:
            output_file_name = os.path.join(DIRS["market"], "output-oil.csv")

            list_global_index = [
                "OIL_GSL",   # 휘발유
                "OIL_HGSL",  # 고급휘발유
                "OIL_LO",    # 경유
                "OIL_DU",    # 두바이유
                "OIL_BRT",   # 브렌트유
                "OIL_CL",    # WTI
            ]

            list_oil = []
            set_days = set()
            instruments_succeeded = 0
            for l_equity_code in list_global_index:
                input_path = os.path.join(DIRS["oil"], f"input_oil_{l_equity_code}.csv")
                l_page_from, l_page_to = 1, PAGE_MAX

                list_oil.append({"item_code": l_equity_code, "file_name": input_path})

                if os.path.exists(input_path):
                    df_index = pd.read_csv(input_path).drop_duplicates()
                    if "eod_date" in df_index: df_index["eod_date"] = df_index["eod_date"].astype(int)
                    if "price_close" in df_index: df_index["price_close"] = df_index["price_close"].astype(float)
                    df_index = df_index.sort_values(by=["eod_date"], ascending=True)
                    if df_index.shape[0] > 10:  # 최근 10행은 다시 크롤링해 merge 안정화
                        df_index = df_index.head(df_index.shape[0] - 10)
                else:
                    df_index = pd.DataFrame()

                url_domestic = "https://finance.naver.com/marketindex/oilDailyQuote.naver"
                url_world    = "https://finance.naver.com/marketindex/worldDailyQuote.naver"
                base_url     = url_domestic

                not_change_count = 0
                for page_no in range(l_page_from, l_page_to + 1):
                    # 시도 1: domestic
                    df_page = pd.DataFrame()
                    try:
                        list_price = []
                        param = {"marketindexCd": l_equity_code, "fdtc": 2, "page": page_no}
                        results = module_UTIL.U0001_collect(base_url, param, p_sleep_time=COLLECT_SLEEP_SEC)
                        price_table = bs4.BeautifulSoup(results.content, "html.parser").find_all("table")[0]
                        price_trs = bs4.BeautifulSoup(str(price_table), "html.parser").find_all("tr")
                        for price_tr in price_trs:
                            row = bs4.BeautifulSoup(str(price_tr), "html.parser").find_all("td")
                            if len(row) > 3:
                                try:
                                    list_price.append({
                                        "eod_date": int(row[0].text.strip().replace(".", "")),
                                        "item_code": l_equity_code,
                                        "price_close": float(row[1].text.strip().replace(",", "")),
                                    })
                                except Exception:
                                    module_UTIL.U0001_logging(l_equity_code, page_no, row)
                        df_page = pd.DataFrame(list_price)
                        if df_page.shape[0] > 0:
                            df_page["eod_date"] = df_page["eod_date"].astype(int)
                    except Exception:
                        # 실패 시 world 페이지 재시도
                        base_url = url_world
                        try:
                            list_price = []
                            param = {"marketindexCd": l_equity_code, "fdtc": 2, "page": page_no}
                            results = module_UTIL.U0001_collect(base_url, param, p_sleep_time=COLLECT_SLEEP_SEC)
                            price_table = bs4.BeautifulSoup(results.content, "html.parser").find_all("table")[0]
                            price_trs = bs4.BeautifulSoup(str(price_table), "html.parser").find_all("tr")
                            for price_tr in price_trs:
                                row = bs4.BeautifulSoup(str(price_tr), "html.parser").find_all("td")
                                if len(row) > 3:
                                    try:
                                        list_price.append({
                                            "eod_date": int(row[0].text.strip().replace(".", "")),
                                            "item_code": l_equity_code,
                                            "price_close": float(row[1].text.strip().replace(",", "")),
                                        })
                                    except Exception:
                                        module_UTIL.U0001_logging(l_equity_code, page_no, row)
                            df_page = pd.DataFrame(list_price)
                            if df_page.shape[0] > 0:
                                df_page["eod_date"] = df_page["eod_date"].astype(int)
                        except Exception:
                            df_page = pd.DataFrame()

                    # 페이지 수집 직후 shape 로그
                    _log_df_shapes("df_page", l_equity_code, page_no, df_page, df_index)

                    # 빈 페이지 처리
                    if df_page.shape[0] == 0:
                        not_change_count += 1
                        module_UTIL.U0001_logging(l_equity_code, page_no, "empty_page", f"streak={not_change_count}")
                        if not_change_count >= EMPTY_PAGE_BREAK_AFTER:
                            module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_empty_pages")
                            break
                        continue

                    # 증분 병합
                    prev_rows = df_index.shape[0]
                    df_keep = df_index[~df_index.get("eod_date", pd.Series(dtype=int)).isin(set(df_page["eod_date"].unique()))] if prev_rows > 0 else df_index
                    df_index = pd.concat([df_keep, df_page], sort=False).drop_duplicates()
                    df_index["eod_date"] = df_index["eod_date"].astype(int)
                    df_index = df_index.sort_values(by=["eod_date"], ascending=True)

                    # 병합 직후 shape 로그
                    _log_df_shapes("merged", l_equity_code, page_no, df_page, df_index)

                    # 조기 종료: 행수 증가 없으면 카운트, 있으면 리셋
                    if df_index.shape[0] == prev_rows:
                        not_change_count += 1
                    else:
                        not_change_count = 0

                    # 저장/하한 체크
                    df_index.to_csv(input_path, index=None)
                    try:
                        if int(df_page["eod_date"].min()) < MIN_DATE_BOUNDARY:
                            module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_min_date")
                            break
                    except Exception:
                        pass

                    if not_change_count >= STAGNATE_BREAK_AFTER:
                        module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_stagnate", f"streak={not_change_count}")
                        break

                if df_index.shape[0] > 0:
                    set_days.update(df_index["eod_date"].unique())
                    instruments_succeeded += 1

            # 집계/출력
            if len(set_days) > 0:
                df_oil = pd.DataFrame(list(set_days), columns=["eod_date"]).set_index("eod_date")
                df_oil["dummy"] = 0
                for item_info in list_oil:
                    if not os.path.exists(item_info["file_name"]):
                        continue
                    df = pd.read_csv(item_info["file_name"])
                    if df.shape[0] == 0:
                        continue
                    item_code = df.head(1)["item_code"].tolist()[0]
                    df = df[["eod_date", "price_close"]]
                    df.columns = ["eod_date", f"{item_code}_price_close"]
                    df = df.set_index("eod_date")
                    df_oil = df_oil.join(df, how="left")
                df_oil = df_oil.drop(columns=["dummy"], errors="ignore").sort_index(ascending=True)
                df_oil.to_csv(output_file_name)

            exec_time = time.time() - start_time
            row_count = 0
            if os.path.exists(output_file_name):
                dfo = pd.read_csv(output_file_name)
                if dfo.shape[0] > 0 and "eod_date" in dfo.columns:
                    row_count = dfo.shape[0]
                    module_UTIL.U0001_db_save_collect_result(
                        JOB_ID, p_category='oil', p_item_code='PBCN_000700', p_item_name='유가정보',
                        p_row_count=row_count,
                        p_eod_date_min=int(dfo["eod_date"].min()), p_eod_date_max=int(dfo["eod_date"].max()),
                        p_file_path=output_file_name, p_message=f"exec_time={exec_time:.2f}s", p_context=ctx,
                        p_extra={"instruments_attempted": len(list_global_index), "instruments_succeeded": instruments_succeeded},
                    )

            module_UTIL.U0001_logging("PBCN_000700", "finished", output_file_name)

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_oil', 'SUCCESS', 'collect_oil is finished.',
                p_section_count=row_count, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"instruments_attempted": len(list_global_index), "instruments_succeeded": instruments_succeeded,
                         "output_file": output_file_name},
            )
            return f"oil:{output_file_name}"
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_oil', 'FAILED', f'collect_oil failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # ==================
    # 귀금속 (PBCN_000800)
    # ==================
    @task(task_id="collect_jewelry")
    def collect_jewelry() -> str:
        import bs4, pandas as pd

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        start_time = time.time()
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_jewelry', 'STARTED', 'collect_jewelry is started.',
            p_context=ctx, p_start_time=t_start,
        )
        module_UTIL.U0001_logging("PBCN_000800", "started", ctx.get("ds_nodash"))

        try:
            output_file_name = os.path.join(DIRS["market"], "output-jewelry.csv")
            base_url = "https://finance.naver.com/marketindex/worldDailyQuote.naver"

            list_global_index = ["CMDT_GC", "CMDT_PL", "CMDT_SI", "CMDT_PA"]
            list_jewelry, set_days = [], set()
            instruments_succeeded = 0

            for l_equity_code in list_global_index:
                input_path = os.path.join(DIRS["jewelry"], f"input_jewelry_{l_equity_code}.csv")
                l_page_from, l_page_to = 1, PAGE_MAX
                list_jewelry.append({"item_code": l_equity_code, "file_name": input_path})

                if os.path.exists(input_path):
                    df_index = pd.read_csv(input_path).drop_duplicates()
                    if "eod_date" in df_index: df_index["eod_date"] = df_index["eod_date"].astype(int)
                    if "price_close" in df_index: df_index["price_close"] = df_index["price_close"].astype(float)
                    df_index = df_index.sort_values(by=["eod_date"], ascending=True)
                    if df_index.shape[0] > 10: df_index = df_index.head(df_index.shape[0] - 10)
                else:
                    df_index = pd.DataFrame()

                not_change_count = 0
                for page_no in range(l_page_from, l_page_to + 1):
                    df_page = pd.DataFrame()
                    try:
                        list_price = []
                        param = {"marketindexCd": l_equity_code, "fdtc": 2, "page": page_no}
                        results = module_UTIL.U0001_collect(base_url, param, p_sleep_time=COLLECT_SLEEP_SEC)
                        price_table = bs4.BeautifulSoup(results.content, "html.parser").find_all("table")[0]
                        price_trs = bs4.BeautifulSoup(str(price_table), "html.parser").find_all("tr")
                        for price_tr in price_trs:
                            row = bs4.BeautifulSoup(str(price_tr), "html.parser").find_all("td")
                            if len(row) > 3:
                                try:
                                    list_price.append({
                                        "eod_date": int(row[0].text.strip().replace(".", "")),
                                        "item_code": l_equity_code,
                                        "price_close": float(row[1].text.strip().replace(",", "")),
                                    })
                                except Exception:
                                    module_UTIL.U0001_logging(l_equity_code, page_no, row)
                        df_page = pd.DataFrame(list_price)
                        if df_page.shape[0] > 0:
                            df_page["eod_date"] = df_page["eod_date"].astype(int)
                    except Exception:
                        df_page = pd.DataFrame()

                    _log_df_shapes("df_page", l_equity_code, page_no, df_page, df_index)

                    if df_page.shape[0] == 0:
                        not_change_count += 1
                        module_UTIL.U0001_logging(l_equity_code, page_no, "empty_page", f"streak={not_change_count}")
                        if not_change_count >= EMPTY_PAGE_BREAK_AFTER:
                            module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_empty_pages")
                            break
                        continue

                    prev_rows = df_index.shape[0]
                    df_keep = df_index[~df_index.get("eod_date", pd.Series(dtype=int)).isin(set(df_page["eod_date"].unique()))] if prev_rows > 0 else df_index
                    df_index = pd.concat([df_keep, df_page], sort=False).drop_duplicates()
                    df_index["eod_date"] = df_index["eod_date"].astype(int)
                    df_index = df_index.sort_values(by=["eod_date"], ascending=True)

                    _log_df_shapes("merged", l_equity_code, page_no, df_page, df_index)

                    if df_index.shape[0] == prev_rows:
                        not_change_count += 1
                    else:
                        not_change_count = 0

                    df_index.to_csv(input_path, index=None)
                    try:
                        if int(df_page["eod_date"].min()) < MIN_DATE_BOUNDARY:
                            module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_min_date")
                            break
                    except Exception:
                        pass
                    if not_change_count >= STAGNATE_BREAK_AFTER:
                        module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_stagnate", f"streak={not_change_count}")
                        break

                if df_index.shape[0] > 0:
                    set_days.update(df_index["eod_date"].unique())
                    instruments_succeeded += 1

            if len(set_days) > 0:
                df_jewelry = pd.DataFrame(list(set_days), columns=["eod_date"]).set_index("eod_date")
                df_jewelry["dummy"] = 0
                for item_info in list_jewelry:
                    if not os.path.exists(item_info["file_name"]):
                        continue
                    df = pd.read_csv(item_info["file_name"])
                    if df.shape[0] == 0:
                        continue
                    item_code = df.head(1)["item_code"].tolist()[0]
                    df = df[["eod_date", "price_close"]]
                    df.columns = ["eod_date", f"{item_code}_price_close"]
                    df = df.set_index("eod_date")
                    df_jewelry = df_jewelry.join(df, how="left")
                df_jewelry = df_jewelry.drop(columns=["dummy"], errors="ignore").sort_index(ascending=True)
                df_jewelry.to_csv(output_file_name)

            exec_time = time.time() - start_time
            row_count = 0
            if os.path.exists(output_file_name):
                dfo = pd.read_csv(output_file_name)
                if dfo.shape[0] > 0 and "eod_date" in dfo.columns:
                    row_count = dfo.shape[0]
                    module_UTIL.U0001_db_save_collect_result(
                        JOB_ID, p_category='jewelry', p_item_code='PBCN_000800', p_item_name='귀금속 정보',
                        p_row_count=row_count,
                        p_eod_date_min=int(dfo["eod_date"].min()), p_eod_date_max=int(dfo["eod_date"].max()),
                        p_file_path=output_file_name, p_message=f"exec_time={exec_time:.2f}s", p_context=ctx,
                        p_extra={"instruments_attempted": len(list_global_index), "instruments_succeeded": instruments_succeeded},
                    )

            module_UTIL.U0001_logging("PBCN_000800", "finished", output_file_name)

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_jewelry', 'SUCCESS', 'collect_jewelry is finished.',
                p_section_count=row_count, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"instruments_attempted": len(list_global_index), "instruments_succeeded": instruments_succeeded,
                         "output_file": output_file_name},
            )
            return f"jewelry:{output_file_name}"
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_jewelry', 'FAILED', f'collect_jewelry failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # ==================
    # 국내 금 (PBCN_000900)
    # ==================
    @task(task_id="collect_gold_kr")
    def collect_gold_kr() -> str:
        import bs4, pandas as pd

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        start_time = time.time()
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_gold_kr', 'STARTED', 'collect_gold_kr is started.',
            p_context=ctx, p_start_time=t_start,
        )
        module_UTIL.U0001_logging("PBCN_000900", "started", ctx.get("ds_nodash"))

        try:
            input_file       = os.path.join(DIRS["market"], "input_market_CMDT_KR.csv")  # 증분/히스토리
            output_file_name = os.path.join(DIRS["market"], "output-gold-kr.csv")

            l_equity_code = "CMDT_KR"
            l_page_from, l_page_to = 1, PAGE_MAX

            if os.path.exists(input_file):
                df_index = pd.read_csv(input_file).drop_duplicates()
                if "eod_date" in df_index: df_index["eod_date"] = df_index["eod_date"].astype(int)
                for col in [
                    "trade_standard_rate", "commodity_buy", "commodity_sell",
                    "account_buy", "account_sell", "global_gold_price",
                    "global_ratio_won_dollar", "rate"
                ]:
                    if col in df_index.columns:
                        df_index[col] = df_index[col].astype(float)
                df_index = df_index.sort_values(by=["eod_date"], ascending=True)
                if df_index.shape[0] > 10: df_index = df_index.head(df_index.shape[0] - 10)
            else:
                df_index = pd.DataFrame()

            base_url = "https://finance.naver.com/marketindex/goldDailyQuote.nhn"
            not_change_count = 0
            old_only_count   = 0  # 이미 가진 최저일자보다 과거 데이터만 연속 등장할 때 카운트

            for page_no in range(l_page_from, l_page_to + 1):
                df_page = pd.DataFrame()
                try:
                    list_price = []
                    param = {"page": page_no}
                    results = module_UTIL.U0001_collect(base_url, param, p_sleep_time=COLLECT_SLEEP_SEC)
                    price_table = bs4.BeautifulSoup(results.content, "html.parser").find_all("table")[0]
                    price_trs = bs4.BeautifulSoup(str(price_table), "html.parser").find_all("tr")
                    for price_tr in price_trs:
                        row = bs4.BeautifulSoup(str(price_tr), "html.parser").find_all("td")
                        if len(row) > 3:
                            try:
                                list_price.append({
                                    "eod_date": int(row[0].text.strip().replace(".", "")),
                                    "item_code": l_equity_code,
                                    "trade_standard_rate": float(row[1].text.strip().replace(",", "")),
                                    "rate": float(row[2].text.strip().replace(",", "")),
                                    "commodity_buy": float(row[3].text.strip().replace(",", "")),
                                    "commodity_sell": float(row[4].text.strip().replace(",", "")),
                                    "account_buy": float(row[5].text.strip().replace(",", "")),
                                    "account_sell": float(row[6].text.strip().replace(",", "")),
                                    "global_gold_price": float(row[7].text.strip().replace(",", "")),
                                    "global_ratio_won_dollar": float(row[8].text.strip().replace(",", "")),
                                })
                            except Exception:
                                module_UTIL.U0001_logging(l_equity_code, page_no, row)
                    df_page = pd.DataFrame(list_price)
                    if df_page.shape[0] > 0:
                        df_page["eod_date"] = df_page["eod_date"].astype(int)
                except Exception:
                    df_page = pd.DataFrame()

                _log_df_shapes("df_page", l_equity_code, page_no, df_page, df_index)

                # 빈 페이지 처리
                if df_page.shape[0] == 0:
                    not_change_count += 1
                    module_UTIL.U0001_logging(l_equity_code, page_no, "empty_page", f"streak={not_change_count}")
                    if not_change_count >= EMPTY_PAGE_BREAK_AFTER:
                        module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_empty_pages")
                        break
                    continue

                prev_rows = df_index.shape[0]

                # 이미 가진 최저일자보다 과거 데이터만 있는 페이지가 연속 등장할 때 종료
                if prev_rows > 0:
                    cur_min_have = int(df_index["eod_date"].min())
                    if int(df_page["eod_date"].max()) <= cur_min_have:
                        old_only_count += 1
                        module_UTIL.U0001_logging(l_equity_code, page_no, "only_older_than_existing", f"streak={old_only_count}")
                        if old_only_count >= OLDER_THAN_EXISTING_BREAK_AFTER:
                            module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_only_older_pages")
                            break
                    else:
                        old_only_count = 0

                # 증분 병합
                df_keep = df_index[~df_index.get("eod_date", pd.Series(dtype=int)).isin(set(df_page["eod_date"].unique()))] if prev_rows > 0 else df_index
                df_index = pd.concat([df_keep, df_page], sort=False).drop_duplicates()
                df_index["eod_date"] = df_index["eod_date"].astype(int)
                df_index = df_index.sort_values(by=["eod_date"], ascending=True)

                _log_df_shapes("merged", l_equity_code, page_no, df_page, df_index)

                # 조기 종료: 행수 증가 없으면 카운트(+1), 있으면 리셋
                if df_index.shape[0] == prev_rows:
                    not_change_count += 1
                else:
                    not_change_count = 0

                # 증분 저장 + 하한/중단 체크
                df_index.to_csv(input_file, index=None)

                try:
                    if int(df_page["eod_date"].min()) < MIN_DATE_BOUNDARY:
                        module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_min_date")
                        break
                except Exception:
                    pass

                if not_change_count >= STAGNATE_BREAK_AFTER:
                    module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_stagnate", f"streak={not_change_count}")
                    break

            # 최종 산출물 저장
            df_out = df_index.sort_values(by=["eod_date"], ascending=True)
            df_out.to_csv(output_file_name, index=False)

            exec_time = time.time() - start_time
            row_count = 0
            if df_out.shape[0] > 0:
                row_count = df_out.shape[0]
                module_UTIL.U0001_db_save_collect_result(
                    JOB_ID, p_category='gold_kr', p_item_code='PBCN_000900', p_item_name='국내금 정보',
                    p_row_count=row_count,
                    p_eod_date_min=int(df_out["eod_date"].min()), p_eod_date_max=int(df_out["eod_date"].max()),
                    p_file_path=output_file_name, p_message=f"exec_time={exec_time:.2f}s", p_context=ctx,
                )

            module_UTIL.U0001_logging("PBCN_000900", "finished", output_file_name)

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_gold_kr', 'SUCCESS', 'collect_gold_kr is finished.',
                p_section_count=row_count, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"output_file": output_file_name},
            )
            return f"gold_kr:{output_file_name}"
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_gold_kr', 'FAILED', f'collect_gold_kr failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # ==================
    # 원자재 (PBCN_001000)
    # ==================
    @task(task_id="collect_materials")
    def collect_materials() -> str:
        import bs4, pandas as pd

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        start_time = time.time()
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_materials', 'STARTED', 'collect_materials is started.',
            p_context=ctx, p_start_time=t_start,
        )
        module_UTIL.U0001_logging("PBCN_001000", "started", ctx.get("ds_nodash"))

        try:
            output_file_name = os.path.join(DIRS["market"], "output-materials.csv")
            base_url = "https://finance.naver.com/marketindex/worldDailyQuote.naver"

            list_global_index = [
                "CMDT_HO", "CMDT_NG",
                "CMDT_CDY", "CMDT_PDY", "CMDT_ZDY", "CMDT_NDY", "CMDT_AAY", "CMDT_SDY",
                "CMDT_C", "CMDT_SB", "CMDT_S", "CMDT_SM", "CMDT_BO", "CMDT_CT", "CMDT_W",
                "CMDT_RR", "CMDT_OJ", "CMDT_KC", "CMDT_CC",
            ]

            list_material, set_days = [], set()
            instruments_succeeded = 0

            for l_equity_code in list_global_index:
                input_path = os.path.join(DIRS["material"], f"input_material_{l_equity_code}.csv")
                l_page_from, l_page_to = 1, PAGE_MAX

                list_material.append({"item_code": l_equity_code, "file_name": input_path})

                if os.path.exists(input_path):
                    df_index = pd.read_csv(input_path).drop_duplicates()
                    if "eod_date" in df_index: df_index["eod_date"] = df_index["eod_date"].astype(int)
                    if "price_close" in df_index: df_index["price_close"] = df_index["price_close"].astype(float)
                    df_index = df_index.sort_values(by=["eod_date"], ascending=True)
                    if df_index.shape[0] > 10: df_index = df_index.head(df_index.shape[0] - 10)
                else:
                    df_index = pd.DataFrame()

                not_change_count = 0
                for page_no in range(l_page_from, l_page_to + 1):
                    df_page = pd.DataFrame()
                    try:
                        list_price = []
                        param = {"marketindexCd": l_equity_code, "fdtc": 2, "page": page_no}
                        results = module_UTIL.U0001_collect(base_url, param, p_sleep_time=COLLECT_SLEEP_SEC)
                        price_table = bs4.BeautifulSoup(results.content, "html.parser").find_all("table")[0]
                        price_trs = bs4.BeautifulSoup(str(price_table), "html.parser").find_all("tr")
                        for price_tr in price_trs:
                            row = bs4.BeautifulSoup(str(price_tr), "html.parser").find_all("td")
                            if len(row) > 3:
                                try:
                                    list_price.append({
                                        "eod_date": int(row[0].text.strip().replace(".", "")),
                                        "item_code": l_equity_code,
                                        "price_close": float(row[1].text.strip().replace(",", "")),
                                    })
                                except Exception:
                                    module_UTIL.U0001_logging(l_equity_code, page_no, row)
                        df_page = pd.DataFrame(list_price)
                        if df_page.shape[0] > 0:
                            df_page["eod_date"] = df_page["eod_date"].astype(int)
                    except Exception:
                        df_page = pd.DataFrame()

                    _log_df_shapes("df_page", l_equity_code, page_no, df_page, df_index)

                    if df_page.shape[0] == 0:
                        not_change_count += 1
                        module_UTIL.U0001_logging(l_equity_code, page_no, "empty_page", f"streak={not_change_count}")
                        if not_change_count >= EMPTY_PAGE_BREAK_AFTER:
                            module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_empty_pages")
                            break
                        continue

                    prev_rows = df_index.shape[0]
                    df_keep = df_index[~df_index.get("eod_date", pd.Series(dtype=int)).isin(set(df_page["eod_date"].unique()))] if prev_rows > 0 else df_index
                    df_index = pd.concat([df_keep, df_page], sort=False).drop_duplicates()
                    df_index["eod_date"] = df_index["eod_date"].astype(int)
                    df_index = df_index.sort_values(by=["eod_date"], ascending=True)

                    _log_df_shapes("merged", l_equity_code, page_no, df_page, df_index)

                    if df_index.shape[0] == prev_rows:
                        not_change_count += 1
                    else:
                        not_change_count = 0

                    df_index.to_csv(input_path, index=None)
                    try:
                        if int(df_page["eod_date"].min()) < MIN_DATE_BOUNDARY:
                            module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_min_date")
                            break
                    except Exception:
                        pass
                    if not_change_count >= STAGNATE_BREAK_AFTER:
                        module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_stagnate", f"streak={not_change_count}")
                        break

                if df_index.shape[0] > 0:
                    set_days.update(df_index["eod_date"].unique())
                    instruments_succeeded += 1

            if len(set_days) > 0:
                df_material = pd.DataFrame(list(set_days), columns=["eod_date"]).set_index("eod_date")
                df_material["dummy"] = 0
                for item_info in list_material:
                    if not os.path.exists(item_info["file_name"]):
                        continue
                    df = pd.read_csv(item_info["file_name"])
                    if df.shape[0] == 0:
                        continue
                    item_code = df.head(1)["item_code"].tolist()[0]
                    df = df[["eod_date", "price_close"]]
                    df.columns = ["eod_date", f"{item_code}_price_close"]
                    df = df.set_index("eod_date")
                    df_material = df_material.join(df, how="left")
                df_material = df_material.drop(columns=["dummy"], errors="ignore").sort_index(ascending=True)
                df_material.to_csv(output_file_name)

            exec_time = time.time() - start_time
            row_count = 0
            if os.path.exists(output_file_name):
                dfo = pd.read_csv(output_file_name)
                if dfo.shape[0] > 0 and "eod_date" in dfo.columns:
                    row_count = dfo.shape[0]
                    module_UTIL.U0001_db_save_collect_result(
                        JOB_ID, p_category='material', p_item_code='PBCN_001000', p_item_name='원자재 정보',
                        p_row_count=row_count,
                        p_eod_date_min=int(dfo["eod_date"].min()), p_eod_date_max=int(dfo["eod_date"].max()),
                        p_file_path=output_file_name, p_message=f"exec_time={exec_time:.2f}s", p_context=ctx,
                        p_extra={"instruments_attempted": len(list_global_index), "instruments_succeeded": instruments_succeeded},
                    )

            module_UTIL.U0001_logging("PBCN_001000", "finished", output_file_name)

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_materials', 'SUCCESS', 'collect_materials is finished.',
                p_section_count=row_count, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"instruments_attempted": len(list_global_index), "instruments_succeeded": instruments_succeeded,
                         "output_file": output_file_name},
            )
            return f"materials:{output_file_name}"
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_materials', 'FAILED', f'collect_materials failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # ===========================
    # 해외 지수 (유럽·아메리카) 001100
    # ===========================
    @task(task_id="collect_world_eu_us")
    def collect_world_eu_us() -> str:
        import pandas as pd

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        start_time = time.time()
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_world_eu_us', 'STARTED', 'collect_world_eu_us is started.',
            p_context=ctx, p_start_time=t_start,
        )
        module_UTIL.U0001_logging("PBCN_001100", "started", ctx.get("ds_nodash"))

        try:
            output_file_name = os.path.join(DIRS["market"], "output-world-eu-us.csv")
            base_url = "http://finance.naver.com/world/worldDayListJson.nhn"

            list_global_index = [
                "DJI@DJI", "DJI@DJT", "NAS@IXIC", "NAS@NDX",
                "SPI@SPX", "NAS@SOX", "BRI@BVSP", "LNS@FTSE100",
                "PAS@CAC40", "XTR@DAX30", "STX@SX5E", "RUI@RTSI", "ITI@FTSEMIB",
            ]

            list_market, set_days = [], set()
            instruments_succeeded = 0

            for l_equity_code in list_global_index:
                l_market_brief_name = l_equity_code.replace("@", "_")
                input_path = os.path.join(DIRS["market"], f"input_market_{l_market_brief_name}.csv")
                l_page_from, l_page_to = 1, PAGE_MAX

                list_market.append({"item_code": l_equity_code, "file_name": input_path})

                if os.path.exists(input_path):
                    df_index = pd.read_csv(input_path).drop_duplicates()
                    if "eod_date" in df_index: df_index["eod_date"] = df_index["eod_date"].astype(int)
                    for col in ["price_open","price_high","price_low","price_close","diff","rate"]:
                        if col in df_index.columns: df_index[col] = df_index[col].astype(float)
                    if "trade_amount" in df_index.columns:
                        df_index["trade_amount"] = df_index["trade_amount"].astype(int)
                    df_index = df_index.sort_values(by=["eod_date"], ascending=True)
                    if df_index.shape[0] > 10: df_index = df_index.head(df_index.shape[0] - 10)
                else:
                    df_index = pd.DataFrame()

                not_change_count = 0
                for page_no in range(l_page_from, l_page_to + 1):
                    df_page = pd.DataFrame()
                    try:
                        list_price = []
                        param = {"symbol": l_equity_code, "fdtc": "0", "page": page_no}
                        results = module_UTIL.U0001_collect(base_url, param, p_sleep_time=COLLECT_SLEEP_SEC)
                        rows = json.loads(results.text)
                        for row in rows:
                            list_price.append({
                                "eod_date": row["xymd"],
                                "item_code": row["symb"],
                                "price_open": row["open"],
                                "price_high": row["high"],
                                "price_low": row["low"],
                                "price_close": row["clos"],
                                "trade_amount": row.get("gvol", 0),
                                "diff": row.get("diff", 0),
                                "rate": row.get("rate", 0),
                            })
                        df_page = pd.DataFrame(list_price)
                        if df_page.shape[0] > 0:
                            df_page["eod_date"] = df_page["eod_date"].astype(int)
                    except Exception as e:
                        module_UTIL.U0001_logging(l_equity_code, page_no, "parse_error", str(e))
                        df_page = pd.DataFrame()

                    _log_df_shapes("df_page", l_equity_code, page_no, df_page, df_index)

                    if df_page.shape[0] == 0:
                        not_change_count += 1
                        module_UTIL.U0001_logging(l_equity_code, page_no, "empty_page", f"streak={not_change_count}")
                        if not_change_count >= EMPTY_PAGE_BREAK_AFTER:
                            module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_empty_pages")
                            break
                        continue

                    prev_rows = df_index.shape[0]
                    df_keep = df_index[~df_index.get("eod_date", pd.Series(dtype=int)).isin(set(df_page["eod_date"].unique()))] if prev_rows > 0 else df_index
                    df_index = pd.concat([df_keep, df_page], sort=False).drop_duplicates()
                    df_index["eod_date"] = df_index["eod_date"].astype(int)
                    df_index = df_index.sort_values(by=["eod_date"], ascending=True)

                    _log_df_shapes("merged", l_equity_code, page_no, df_page, df_index)

                    if df_index.shape[0] == prev_rows:
                        not_change_count += 1
                    else:
                        not_change_count = 0

                    set_days.update(df_index["eod_date"].unique())
                    df_index.to_csv(input_path, index=None)

                    try:
                        if int(df_page["eod_date"].min()) < MIN_DATE_BOUNDARY:
                            module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_min_date")
                            break
                    except Exception:
                        pass
                    if not_change_count >= STAGNATE_BREAK_AFTER:
                        module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_stagnate", f"streak={not_change_count}")
                        break

                if df_index.shape[0] > 0:
                    instruments_succeeded += 1

            if len(set_days) > 0:
                df_market = pd.DataFrame(list(set_days), columns=["eod_date"]).set_index("eod_date")
                df_market["dummy"] = 0
                for item_info in list_market:
                    if not os.path.exists(item_info["file_name"]):
                        continue
                    df = pd.read_csv(item_info["file_name"])
                    if df.shape[0] == 0:
                        continue
                    item_code = df.head(1)["item_code"].tolist()[0]
                    cols = ["eod_date","price_open","price_high","price_low","price_close","trade_amount","diff","rate"]
                    df = df[cols]
                    df.columns = [
                        "eod_date",
                        f"{item_code}_price_open", f"{item_code}_price_high", f"{item_code}_price_low",
                        f"{item_code}_price_close", f"{item_code}_trade_amount",
                        f"{item_code}_diff", f"{item_code}_rate"
                    ]
                    df = df.set_index("eod_date")
                    df_market = df_market.join(df, how="left")
                df_market = df_market.drop(columns=["dummy"], errors="ignore").sort_index(ascending=True)
                df_market.to_csv(output_file_name)

            exec_time = time.time() - start_time
            row_count = 0
            if os.path.exists(output_file_name):
                dfo = pd.read_csv(output_file_name)
                if dfo.shape[0] > 0 and "eod_date" in dfo.columns:
                    row_count = dfo.shape[0]
                    module_UTIL.U0001_db_save_collect_result(
                        JOB_ID, p_category='world_eu_us', p_item_code='PBCN_001100', p_item_name='유럽/아메리카 지수',
                        p_row_count=row_count,
                        p_eod_date_min=int(dfo["eod_date"].min()), p_eod_date_max=int(dfo["eod_date"].max()),
                        p_file_path=output_file_name, p_message=f"exec_time={exec_time:.2f}s", p_context=ctx,
                        p_extra={"instruments_attempted": len(list_global_index), "instruments_succeeded": instruments_succeeded},
                    )

            module_UTIL.U0001_logging("PBCN_001100", "finished", output_file_name)

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_world_eu_us', 'SUCCESS', 'collect_world_eu_us is finished.',
                p_section_count=row_count, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"instruments_attempted": len(list_global_index), "instruments_succeeded": instruments_succeeded,
                         "output_file": output_file_name},
            )
            return f"world_eu_us:{output_file_name}"
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_world_eu_us', 'FAILED', f'collect_world_eu_us failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # ======================
    # 해외 지수 (아시아) 001200
    # ======================
    @task(task_id="collect_world_asia")
    def collect_world_asia() -> str:
        import pandas as pd

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        start_time = time.time()
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_world_asia', 'STARTED', 'collect_world_asia is started.',
            p_context=ctx, p_start_time=t_start,
        )
        module_UTIL.U0001_logging("PBCN_001200", "started", ctx.get("ds_nodash"))

        try:
            output_file_name = os.path.join(DIRS["market"], "output-world-asia.csv")
            base_url = "http://finance.naver.com/world/worldDayListJson.nhn"

            list_global_index = [
                "SHS@000001","SHS@000002","SHS@000003",
                "NII@NI225",
                "HSI@HSI","HSI@HSCE","HSI@HSCC",
                "TWS@TI01",
                "INI@BSE30",
                "MYI@KLSE",
                "IDI@JKSE",
            ]

            list_market, set_days = [], set()
            instruments_succeeded = 0

            for l_equity_code in list_global_index:
                l_market_brief_name = l_equity_code.replace("@", "_")
                input_path = os.path.join(DIRS["market"], f"input_market_{l_market_brief_name}.csv")
                l_page_from, l_page_to = 1, PAGE_MAX

                list_market.append({"item_code": l_equity_code, "file_name": input_path})

                if os.path.exists(input_path):
                    df_index = pd.read_csv(input_path).drop_duplicates()
                    if "eod_date" in df_index: df_index["eod_date"] = df_index["eod_date"].astype(int)
                    for col in ["price_open","price_high","price_low","price_close","diff","rate"]:
                        if col in df_index.columns: df_index[col] = df_index[col].astype(float)
                    if "trade_amount" in df_index.columns:
                        df_index["trade_amount"] = df_index["trade_amount"].astype(int)
                    df_index = df_index.sort_values(by=["eod_date"], ascending=True)
                    if df_index.shape[0] > 10: df_index = df_index.head(df_index.shape[0] - 10)
                else:
                    df_index = pd.DataFrame()

                not_change_count = 0
                for page_no in range(l_page_from, l_page_to + 1):
                    df_page = pd.DataFrame()
                    try:
                        list_price = []
                        param = {"symbol": l_equity_code, "fdtc": "0", "page": page_no}
                        results = module_UTIL.U0001_collect(base_url, param, p_sleep_time=COLLECT_SLEEP_SEC)
                        rows = json.loads(results.text)
                        for row in rows:
                            list_price.append({
                                "eod_date": row["xymd"],
                                "item_code": row["symb"],
                                "price_open": row["open"],
                                "price_high": row["high"],
                                "price_low": row["low"],
                                "price_close": row["clos"],
                                "trade_amount": row.get("gvol", 0),
                                "diff": row.get("diff", 0),
                                "rate": row.get("rate", 0),
                            })
                        df_page = pd.DataFrame(list_price)
                        if df_page.shape[0] > 0:
                            df_page["eod_date"] = df_page["eod_date"].astype(int)
                    except Exception as e:
                        module_UTIL.U0001_logging(l_equity_code, page_no, "parse_error", str(e))
                        df_page = pd.DataFrame()

                    _log_df_shapes("df_page", l_equity_code, page_no, df_page, df_index)

                    if df_page.shape[0] == 0:
                        not_change_count += 1
                        module_UTIL.U0001_logging(l_equity_code, page_no, "empty_page", f"streak={not_change_count}")
                        if not_change_count >= EMPTY_PAGE_BREAK_AFTER:
                            module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_empty_pages")
                            break
                        continue

                    prev_rows = df_index.shape[0]
                    df_keep = df_index[~df_index.get("eod_date", pd.Series(dtype=int)).isin(set(df_page["eod_date"].unique()))] if prev_rows > 0 else df_index
                    df_index = pd.concat([df_keep, df_page], sort=False).drop_duplicates()
                    df_index["eod_date"] = df_index["eod_date"].astype(int)
                    df_index = df_index.sort_values(by=["eod_date"], ascending=True)

                    _log_df_shapes("merged", l_equity_code, page_no, df_page, df_index)

                    if df_index.shape[0] == prev_rows:
                        not_change_count += 1
                    else:
                        not_change_count = 0

                    set_days.update(df_index["eod_date"].unique())
                    df_index.to_csv(input_path, index=None)

                    try:
                        if int(df_page["eod_date"].min()) < MIN_DATE_BOUNDARY:
                            module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_min_date")
                            break
                    except Exception:
                        pass
                    if not_change_count >= STAGNATE_BREAK_AFTER:
                        module_UTIL.U0001_logging(l_equity_code, page_no, "break_on_stagnate", f"streak={not_change_count}")
                        break

                if df_index.shape[0] > 0:
                    instruments_succeeded += 1

            if len(set_days) > 0:
                df_market = pd.DataFrame(list(set_days), columns=["eod_date"]).set_index("eod_date")
                df_market["dummy"] = 0
                for item_info in list_market:
                    if not os.path.exists(item_info["file_name"]):
                        continue
                    df = pd.read_csv(item_info["file_name"])
                    if df.shape[0] == 0:
                        continue
                    item_code = df.head(1)["item_code"].tolist()[0]
                    cols = ["eod_date","price_open","price_high","price_low","price_close","trade_amount","diff","rate"]
                    df = df[cols]
                    df.columns = [
                        "eod_date",
                        f"{item_code}_price_open", f"{item_code}_price_high", f"{item_code}_price_low",
                        f"{item_code}_price_close", f"{item_code}_trade_amount",
                        f"{item_code}_diff", f"{item_code}_rate"
                    ]
                    df = df.set_index("eod_date")
                    df_market = df_market.join(df, how="left")
                df_market = df_market.drop(columns=["dummy"], errors="ignore").sort_index(ascending=True)
                df_market.to_csv(output_file_name)

            exec_time = time.time() - start_time
            row_count = 0
            if os.path.exists(output_file_name):
                dfo = pd.read_csv(output_file_name)
                if dfo.shape[0] > 0 and "eod_date" in dfo.columns:
                    row_count = dfo.shape[0]
                    module_UTIL.U0001_db_save_collect_result(
                        JOB_ID, p_category='world_asia', p_item_code='PBCN_001200', p_item_name='아시아 지수',
                        p_row_count=row_count,
                        p_eod_date_min=int(dfo["eod_date"].min()), p_eod_date_max=int(dfo["eod_date"].max()),
                        p_file_path=output_file_name, p_message=f"exec_time={exec_time:.2f}s", p_context=ctx,
                        p_extra={"instruments_attempted": len(list_global_index), "instruments_succeeded": instruments_succeeded},
                    )

            module_UTIL.U0001_logging("PBCN_001200", "finished", output_file_name)

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_world_asia', 'SUCCESS', 'collect_world_asia is finished.',
                p_section_count=row_count, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"instruments_attempted": len(list_global_index), "instruments_succeeded": instruments_succeeded,
                         "output_file": output_file_name},
            )
            return f"world_asia:{output_file_name}"
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_world_asia', 'FAILED', f'collect_world_asia failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # ===============
    # 최종 요약/마감
    # ===============
    @task(outlets=[module_DATA.DS_N0014])
    def finalize(ts0: str, r1: str, r2: str, r3: str, r4: str, r5: str, r6: str) -> str:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'finalize', 'STARTED', 'finalize is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"ts0": ts0},
        )
        try:
            module_UTIL.U0001_logging(
                "N0014_naver_market", "finalize",
                f"started_at={ts0}",
                "outputs:", r1, r2, r3, r4, r5, r6
            )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'finalize', 'SUCCESS', 'finalize is finished.',
                p_section_count=6, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"outputs": [r1, r2, r3, r4, r5, r6]},
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

    # -------- 파이프라인 연결(순차 실행) --------
    ts0 = start_ts()

    t_oil      = collect_oil()
    t_jewelry  = collect_jewelry()
    t_gold_kr  = collect_gold_kr()
    t_material = collect_materials()
    t_eu_us    = collect_world_eu_us()
    t_asia     = collect_world_asia()

    # 순차 실행 체인
    ts0 >> t_oil >> t_jewelry >> t_gold_kr >> t_material >> t_eu_us >> t_asia

    fin = finalize(ts0, t_oil, t_jewelry, t_gold_kr, t_material, t_eu_us, t_asia)
    t_asia >> fin


N0014_naver_market()
