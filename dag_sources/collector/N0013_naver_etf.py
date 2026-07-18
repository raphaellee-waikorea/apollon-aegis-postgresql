# -*- coding: utf-8 -*-
"""
N0013_naver_etf
- NAVER ETF 목록 + ETF 시세/거래 히스토리 수집
- 기존 PBCN_000500(ETF 목록) / PBCN_000600(ETF 시세) 로직을 Airflow TaskFlow로 변환
- Dynamic Task Mapping: ETF 종목코드 대량 처리 시 배치 분할 (Airflow map length 1024 제한 회피)
- 동시 실행 5개 제한 (max_active_tis_per_dag=5)
- XCom 직렬화 안전: 정수/문자열/소형 리스트만 교환
- A0003_build_calendar.py 와 동일한 방식으로 DB 접속정보(Airflow Variable)와
  Task 실행 로그(dag_job_log, Task별 STARTED/SUCCESS/FAILED)를 사용한다.
- NAVER 서버 부하 방지를 위해 요청 사이에 유휴 시간(COLLECT_SLEEP_SEC)을 둔다.

경로 구성:
- (영업일/참고) INPUT_EQUITY_DIR = "/opt/apollon-data/finance-data/work/equities/"
  - 파일명: input_KOSPI-YYYYMMDD.csv (또는 fallback: input_KOSPI.csv) → day_count 계산에 사용
- (ETF 작업 루트) ETF_BASE_DIR = "/opt/apollon-data/finance-data/work/etf/"
  - ETF_LIST_DIR = "<ETF_BASE_DIR>/"
    - 저장: input_ETF-YYYYMMDD.csv
  - ETF_HIST_DIR = "<ETF_BASE_DIR>/hist/"
    - 저장: prices-<item_code>.csv

원본과의 차이/보강:
- 멀티프로세싱 제거 → Airflow 동적 매핑 병렬화 사용
- API 가용성 점검(short_circuit)
- ETF 목록 수집 후 파일로 저장(+ 수집 결과를 dag_collect_result 공용 테이블에 기록)
- 최신 ETF 목록에서 item_code 추출 → 배치 분할 → 차트데이터 수집/저장
- 최종 완료 시 DS_N0013 방출 및 로깅
- (버그 수정) 원본에 있던 module_UTIL.WAI_save_db(...) 호출은 U0001_functions.py 에 정의되어
  있지 않아 항상 AttributeError 가 발생했고, 그 예외가 try/except 로 조용히 삼켜지고 있었다.
  이번 리팩터링에서 두 호출 모두 실제 존재하는 공용 함수인
  module_UTIL.U0001_db_save_collect_result(...) 로 교체하여 수집 결과가
  dag_collect_result 테이블(Apache Superset 조회용)에 정상적으로 기록되도록 수정했다.
"""
import os
import re
import time
import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import U0001_functions as module_UTIL   # U0001_collect, U0001_logging, U0001_db_logging, U0001_db_save_collect_result, U0001_naver_chartdata
import U0002_datasets as module_DATA    # DS_N0010, DS_N0011, DS_N0012, DS_N0013

# ===== 고정값/경로 =====
KST = "Asia/Seoul"

# 작업(Task) 실행 정보를 DB(dag_job_log)에 저장할 때 사용하는 작업 구분 키.
# 추후 다른 작업(N0014, N0015, ...)을 추가하면 각 DAG 파일에서 이 값만 다르게 지정하면 된다.
JOB_ID = "N0013"

# (영업일/참고) KOSPI 일자 파일 경로
INPUT_EQUITY_DIR = "/opt/apollon-data/finance-data/work/equities/"

# ETF 작업 경로
ETF_BASE_DIR  = "/opt/apollon-data/finance-data/work/etf/"
ETF_LIST_DIR  = ETF_BASE_DIR
ETF_HIST_DIR  = os.path.join(ETF_BASE_DIR, "hist/")

for d in (ETF_BASE_DIR, ETF_LIST_DIR, ETF_HIST_DIR):
    os.makedirs(d, exist_ok=True)

# 배치 크기(1024 제한 여유있게)
MAP_BATCH_SIZE = 800

# 서버 부하 방지를 위한 요청 간 유휴시간(초).
# - U0001_collect() 호출 시 p_sleep_time 으로 전달됨
# - collect_batch 내 종목별 수집 반복 사이의 명시적 sleep 에도 동일하게 사용
COLLECT_SLEEP_SEC = 2

# NAVER ETF 목록 API
NAVER_ETF_LIST_URL = "https://finance.naver.com/api/sise/etfItemList.nhn"

# =========================
# 유틸 (태스크 아님)
# =========================
_DATE_RE = re.compile(r"(\d{8})")

def _latest_csv_by_date(dir_path: str, prefix: str) -> str | None:
    """
    디렉터리에서 'prefix-YYYYMMDD.csv' 패턴 중 가장 최신 파일 경로 반환.
    (없으면 None)
    """
    if not os.path.exists(dir_path):
        return None
    candidates = []
    for fname in os.listdir(dir_path):
        if not fname.startswith(prefix + "-") or not fname.endswith(".csv"):
            continue
        m = _DATE_RE.search(fname)
        if not m:
            continue
        ymd = m.group(1)
        candidates.append((ymd, os.path.join(dir_path, fname)))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def _chunk_list(items: list[str], size: int) -> list[list[str]]:
    return [items[i:i+size] for i in range(0, len(items), size)]

# =========================
# DAG 정의
# =========================
@dag(
    dag_id="N0013_naver_etf",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[module_DATA.DS_N0011, module_DATA.DS_N0012],  # 선행 파이프라인(영업일 등) 완료 후 실행
    catchup=False,
    tags=["N0013_naver_etf", "NAVER", "ETF", "prices", "history"],
)
def N0013_naver_etf():
    """
    파이프라인 개요
    1) API 가용성 점검 및 입력(KOSPI 일자 파일) 점검
    2) ETF 목록 수집 → input_ETF-YYYYMMDD.csv 저장
    3) 최신 ETF 목록에서 item_code 리스트 산출
    4) 코드 리스트를 800개 단위 배치 분할 (Airflow map length 1024 제한 회피)
    5) KOSPI 일자 파일에서 거래일 개수(day_count) 산출
    6) 배치 단위로 NAVER 차트 데이터 수집/저장 → prices-<code>.csv
    7) 최종 요약/로깅 및 DS_N0013 방출
    """

    # -----------------------------
    # 0) 입력/API 점검
    # -----------------------------
    @task.short_circuit(inlets=[module_DATA.DS_N0010])
    def check_inputs() -> bool:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'check_inputs', 'STARTED', 'check_inputs is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            # KOSPI 일자 파일 존재 확인
            kospi_days = _latest_csv_by_date(INPUT_EQUITY_DIR, "input_KOSPI")
            if kospi_days is None:
                fallback_days = os.path.join(INPUT_EQUITY_DIR, "input_KOSPI.csv")
                if os.path.exists(fallback_days):
                    kospi_days = fallback_days

            if kospi_days is None:
                module_UTIL.U0001_logging("N0013", "MISSING", "input_KOSPI(-YYYYMMDD).csv for day_count")
                t_end = pendulum.now(KST)
                module_UTIL.U0001_db_logging(
                    JOB_ID, 'check_inputs', 'FAILED', 'check_inputs failed: input_KOSPI(-YYYYMMDD).csv not found.',
                    p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                    p_extra={"reason": "missing_kospi_days_file"},
                )
                return False

            # ETF 목록 API 확인
            try:
                # 원본 파라미터
                param = {
                    "etfType": "0",
                    "targetColumn": "market_sum",
                    "sortOrder": "desc",
                }
                res = module_UTIL.U0001_collect(NAVER_ETF_LIST_URL, param, p_sleep_time=COLLECT_SLEEP_SEC)
                # EUC-KR 디코딩 확인 및 최소 키워드 확인
                text = res.content.decode("euc-kr", errors="ignore")
                ok = ("itemcode" in text) and ("itemname" in text)
            except Exception as e:
                ok = False
                module_UTIL.U0001_logging("N0013", "check_inputs", "api_probe_error", str(e))

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_inputs', 'SUCCESS' if ok else 'FAILED',
                f'check_inputs is finished. ok={ok}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"days_file": kospi_days, "ok": ok},
            )
            return ok
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_inputs', 'FAILED', f'check_inputs failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            # short_circuit 태스크의 원래 동작(점검 실패 시 False 반환하여 다운스트림을
            # 정상적으로 skip)을 그대로 유지한다.
            return False

    # -----------------------------
    # 1) ETF 목록 수집 (input_ETF-YYYYMMDD.csv)
    # -----------------------------
    @task
    def collect_etf_list() -> str:
        import json
        import pandas

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        yyyymmdd = ctx["ds_nodash"]
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_etf_list', 'STARTED', 'collect_etf_list is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            param = {
                "etfType": "0",
                "targetColumn": "market_sum",
                "sortOrder": "desc",
            }
            res = module_UTIL.U0001_collect(NAVER_ETF_LIST_URL, param, p_sleep_time=COLLECT_SLEEP_SEC)
            text = res.content.decode("euc-kr", errors="ignore")

            # 원본 파싱 방식 유지: 대괄호 내부 JSON 배열만 추출
            try:
                json_list = text.split("[", 1)[1].split("]", 1)[0]
                etf_list = json.loads("[" + json_list + "]")
            except Exception as e:
                module_UTIL.U0001_logging("N0013", "collect_etf_list", "parse_error", str(e))
                etf_list = []

            rows = []
            for it in etf_list:
                rows.append({
                    "item_code": it.get("itemcode"),
                    "etfTabCode": it.get("etfTabCode"),
                    "itemname": it.get("itemname"),
                    "nowVal": it.get("nowVal"),
                    "risefall": it.get("risefall"),
                    "changeVal": it.get("changeVal"),
                    "changeRate": it.get("changeRate"),
                    "nav": it.get("nav"),
                    "threeMonthEarnRate": it.get("threeMonthEarnRate"),
                    "quant": it.get("quant"),
                    "amonut": it.get("amonut"),            # (원본 스펠링 유지)
                    "marketSum": it.get("marketSum"),
                })

            df_page = pandas.DataFrame(rows)
            out_path = os.path.join(ETF_LIST_DIR, f"input_ETF-{yyyymmdd}.csv")
            df_page.to_csv(out_path, index=False)

            module_UTIL.U0001_logging("N0013", "collect_etf_list", "saved", out_path, f"shape={df_page.shape}")

            # 수집 결과 요약을 공용 포맷(dag_collect_result)에 저장.
            # (원본의 module_UTIL.WAI_save_db("ETF목록 정보", ...) 호출을 대체 - WAI_save_db 는
            #  U0001_functions.py 에 정의되어 있지 않아 항상 실패하던 버그를 수정)
            module_UTIL.U0001_db_save_collect_result(
                p_job_id=JOB_ID, p_category='etf_list',
                p_item_code='ETF_LIST_ALL', p_item_name='NAVER ETF 목록(전체)',
                p_row_count=int(df_page.shape[0]),
                p_file_path=out_path, p_message='ETF목록 정보',
                p_context=ctx,
            )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_etf_list', 'SUCCESS', 'collect_etf_list is finished.',
                p_section_count=int(df_page.shape[0]), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"output_path": out_path},
            )
            return out_path
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_etf_list', 'FAILED', f'collect_etf_list failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # -----------------------------
    # 2) 최신 ETF 목록에서 item_code 리스트 생성
    # -----------------------------
    @task
    def get_etf_codes(latest_list_path: str) -> list[str]:
        import pandas

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'get_etf_codes', 'STARTED', 'get_etf_codes is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"latest_list_path": latest_list_path},
        )
        try:
            # latest_list_path 우선 사용, 없으면 디렉터리 스캔
            path = latest_list_path
            if not path or not os.path.exists(path):
                path = _latest_csv_by_date(ETF_LIST_DIR, "input_ETF")
            if not path or not os.path.exists(path):
                # fallback 없음 → 빈 리스트
                codes = []
            else:
                df = pandas.read_csv(path, dtype={"item_code": str})
                if "item_code" not in df.columns:
                    codes = []
                else:
                    codes = sorted(df["item_code"].dropna().astype(str).unique().tolist())

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'get_etf_codes', 'SUCCESS', 'get_etf_codes is finished.',
                p_section_count=len(codes), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"path": path},
            )
            return codes
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'get_etf_codes', 'FAILED', f'get_etf_codes failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # -----------------------------
    # 3) 코드 리스트 → 배치 분할 (<= 800/배치)
    # -----------------------------
    @task
    def make_code_batches(codes: list[str]) -> list[list[str]]:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'make_code_batches', 'STARTED', 'make_code_batches is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"codes_count": len(codes or [])},
        )
        try:
            batches = _chunk_list(codes, MAP_BATCH_SIZE)

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'make_code_batches', 'SUCCESS', 'make_code_batches is finished.',
                p_section_count=len(batches), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"batch_size": MAP_BATCH_SIZE, "batch_count": len(batches)},
            )
            return batches
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'make_code_batches', 'FAILED', f'make_code_batches failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # -----------------------------
    # 4) 거래일 개수 산출 (input_KOSPI-YYYYMMDD.csv 기준)
    # -----------------------------
    @task
    def get_day_count() -> int:
        import pandas

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'get_day_count', 'STARTED', 'get_day_count is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            kospi_days = _latest_csv_by_date(INPUT_EQUITY_DIR, "input_KOSPI")
            if kospi_days is None:
                kospi_days = os.path.join(INPUT_EQUITY_DIR, "input_KOSPI.csv")
            if not os.path.exists(kospi_days):
                raise FileNotFoundError(f"Required day file not found: {kospi_days}")
            df_days = pandas.read_csv(kospi_days)
            day_count = int(df_days.shape[0])

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'get_day_count', 'SUCCESS', 'get_day_count is finished.',
                p_section_count=day_count, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"days_file": kospi_days},
            )
            return day_count
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'get_day_count', 'FAILED', f'get_day_count failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # -----------------------------
    # 5) 배치 단위 수집 (배치 내 순차)
    #    - 각 ETF 코드에 대해 U0001_naver_chartdata(code, day_count) 호출
    #    - 결과: prices-<code>.csv 저장
    #    - 반환: 처리 건수(int)만 리턴 → XCom 부담 최소화
    # -----------------------------
    @task
    def collect_batch(code_batch: list[str], day_count: int) -> int:
        import numpy
        import time as _t

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_batch', 'STARTED', f'collect_batch is started. size={len(code_batch)}',
            p_context=ctx, p_start_time=t_start,
            p_extra={"batch_size": len(code_batch), "day_count": day_count},
        )
        try:
            processed = 0
            for code in code_batch:
                out_csv = os.path.join(ETF_HIST_DIR, f"prices-{code}.csv")
                try:
                    df_hist = module_UTIL.U0001_naver_chartdata(code, day_count)
                    # 원본처럼 바로 저장
                    df_hist.to_csv(out_csv, index=False)

                    module_UTIL.U0001_logging("N0013", "collect_batch", "saved", code, out_csv,
                                              f"shape={df_hist.shape}",
                                              "Range:",
                                              int(numpy.min(df_hist["eod_date"])) if "eod_date" in df_hist.columns and df_hist.shape[0] else None,
                                              "~",
                                              int(numpy.max(df_hist["eod_date"])) if "eod_date" in df_hist.columns and df_hist.shape[0] else None)
                    processed += 1
                except Exception as e:
                    module_UTIL.U0001_logging("collect_batch", "FAIL", code, str(e))
                # 원본의 rate-limit 배려(서버 부하 방지용 유휴 시간)를 유지
                _t.sleep(COLLECT_SLEEP_SEC)

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_batch', 'SUCCESS', f'collect_batch is finished. processed={processed}',
                p_section_count=processed, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"batch_size": len(code_batch), "day_count": day_count},
            )
            return int(processed)
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_batch', 'FAILED', f'collect_batch failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # -----------------------------
    # 6) 마감 태스크: 전체 요약 로그/DB 기록 + DS_N0013 방출
    # -----------------------------
    @task(outlets=[module_DATA.DS_N0013])
    def finalize(batch_counts: list[int], total_codes: list[str], started_ts: str) -> str:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'finalize', 'STARTED', 'finalize is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"started_ts": started_ts, "total_codes": len(total_codes or [])},
        )
        try:
            total_processed = int(sum(batch_counts or []))
            module_UTIL.U0001_logging(
                ctx["ds_nodash"], "N0013", "finalize",
                f"started_at={started_ts}",
                f"total_codes={len(total_codes or [])}",
                f"processed={total_processed}",
                f"hist_dir={ETF_HIST_DIR}",
            )

            # 수집 결과 요약을 공용 포맷(dag_collect_result)에 저장.
            # (원본의 module_UTIL.WAI_save_db("ETF가격 정보", ...) 호출을 대체 - WAI_save_db 는
            #  U0001_functions.py 에 정의되어 있지 않아 항상 실패하던 버그를 수정)
            module_UTIL.U0001_db_save_collect_result(
                p_job_id=JOB_ID, p_category='etf_price',
                p_item_code='ETF_PRICE_ALL', p_item_name='NAVER ETF 가격 히스토리(전체)',
                p_row_count=total_processed,
                p_file_path=ETF_HIST_DIR, p_message='ETF가격 정보',
                p_context=ctx,
                p_extra={"total_codes": len(total_codes or [])},
            )

            # 완료 마커 파일(optional)
            done_marker = os.path.join(ETF_BASE_DIR, "N0013_naver_etf.done")
            try:
                with open(done_marker, "w", encoding="utf-8") as f:
                    f.write("ok\n")
            except Exception:
                pass

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'finalize', 'SUCCESS', 'finalize is finished.',
                p_section_count=total_processed, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"total_codes": len(total_codes or []), "done_marker": done_marker},
            )
            return done_marker
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'finalize', 'FAILED', f'finalize failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # 시작 타임스탬프
    @task
    def _start_ts() -> str:
        import time as _time

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, '_start_ts', 'STARTED', '_start_ts is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            ts = str(_time.time())
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, '_start_ts', 'SUCCESS', '_start_ts is finished.',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
            )
            return ts
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, '_start_ts', 'FAILED', f'_start_ts failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # -----------------------------
    # 파이프라인 연결
    # -----------------------------
    ok = check_inputs()
    ts0 = _start_ts()

    latest_list_csv = collect_etf_list()
    codes = get_etf_codes(latest_list_csv)
    batches = make_code_batches(codes)
    day_cnt = get_day_count()

    # 배치 단위 Dynamic Mapping (동시 실행 5개 제한)
    mapped_collect = (
        collect_batch
        .override(task_id="collect_batch", max_active_tis_per_dag=5)
        .partial(day_count=day_cnt)
        .expand(code_batch=batches)
    )

    # 의존성
    ok >> ts0
    ok >> latest_list_csv
    ok >> day_cnt
    latest_list_csv >> codes >> batches
    # 요약/마감
    fin = finalize(batch_counts=mapped_collect, total_codes=codes, started_ts=ts0)
    [mapped_collect, codes, ts0] >> fin


N0013_naver_etf()
