# -*- coding: utf-8 -*-
"""
N0011_naver_equity_prices
- NAVER 종목별 시세 히스토리 수집 (KOSPI/KOSDAQ 전체)
- 기존 PBCN_000300 로직을 Airflow TaskFlow로 변환
- Dynamic Task Mapping: 대용량 종목코드 → 배치로 나눠 매핑 (Airflow map length 1024 제한 회피)
- 동시 실행 5개 제한 (max_active_tis_per_dag=5)
- XCom 직렬화 안전: 문자열/정수/소형 리스트만 교환
- A0003_build_calendar.py 와 동일한 방식으로 DB 접속정보(Airflow Variable)와
  Task 실행 로그(dag_job_log, Task별 STARTED/SUCCESS/FAILED)를 사용한다.
- NAVER 서버 부하 방지를 위한 유휴 시간(sleep)은 module_UTIL.U0001_save_hist_data() 내부에
  이미 포함되어 있다 (요청마다 p_sleep_time=2초 대기) - 이 파일에서 별도로 추가하지 않는다.
- 배치/전체 단위 수집 결과는 module_UTIL.U0001_db_save_collect_result() 를 통해
  dag_collect_result 테이블(Apache Superset 등 BI 조회용 공통 포맷)에 저장한다.

요구사항 반영:
- pandas 임포트는 `import pandas` 형태로 사용
- 경로:
  BASE_WORK_DIR = "/opt/apollon-data/finance-data/work/equities/"
  HIST_DIR = os.path.join(BASE_WORK_DIR, "hist/")
  SPOT_DIR = os.path.join(BASE_WORK_DIR, "spot/")
- 저장 파일명: prices-017860.csv (= prices-{equity_code}.csv)
- 입력 목록 파일은 input_KOSPI-YYYYMMDD.csv / input_KOSDAQ-YYYYMMDD.csv 최신본 사용
"""
import os
import re
import pendulum

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.python import get_current_context

import U0001_functions as module_UTIL   # U0001_collect, U0001_logging, U0001_db_logging, U0001_db_save_collect_result, U0001_save_hist_data
import U0002_datasets as module_DATA    # DS_N0010, DS_N0011

KST = "Asia/Seoul"

# 작업(Task) 실행 정보를 DB(dag_job_log)에 저장할 때 사용하는 작업 구분 키.
# 추후 다른 작업(N0012, N0013, ...)을 추가하면 각 DAG 파일에서 이 값만 다르게 지정하면 된다.
JOB_ID = "N0011"

# ===== 경로 설정 =====
BASE_WORK_DIR = "/opt/apollon-data/finance-data/work/equities/"
HIST_DIR = os.path.join(BASE_WORK_DIR, "hist/")
SPOT_DIR = os.path.join(BASE_WORK_DIR, "spot/")
for d in (BASE_WORK_DIR, HIST_DIR, SPOT_DIR):
    os.makedirs(d, exist_ok=True)

# 배치 크기(1024 제한 여유있게)
MAP_BATCH_SIZE = 800

# =========================
# 유틸 (태스크 아님)
# =========================
_DATE_RE = re.compile(r"(\d{8})")

def _latest_csv_by_date(dir_path: str, prefix: str) -> str | None:
    """
    디렉터리에서 'prefix-YYYYMMDD.csv' 패턴 중 가장 최신 날짜 파일을 찾아 반환.
    (없으면 None)
    """
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
    dag_id="N0011_naver_equity_prices",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[module_DATA.DS_N0010],  # 영업일(또는 선행 파이프라인) 완료 후 실행
    catchup=False,
    tags=["N0011_naver_equity_prices", "NAVER", "prices", "history"],
)
def N0011_naver_equity_prices():
    """
    파이프라인 개요
    1) 입력 점검: KOSPI/KOSDAQ 목록 CSV 및 KOSPI 지수 CSV 확인
    2) 최신(날짜 기준) KOSPI/KOSDAQ 종목 목록에서 종목코드 리스트 산출
    3) 코드 리스트를 800개 단위 배치로 분할 (Airflow map length 1024 제한 회피)
    4) KOSPI 지수 CSV에서 영업일 개수(day_count) 산출
    5) 배치 단위로 히스토리 수집(각 배치 내에서 순차 처리) → prices-<code>.csv 저장
    6) need_to_init.csv 생성 및 요약 로그로 마감
    """

    # -----------------------------
    # 0) 입력 점검
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
            latest_kospi_list = _latest_csv_by_date(BASE_WORK_DIR, "input_KOSPI")
            latest_kosdaq_list = _latest_csv_by_date(BASE_WORK_DIR, "input_KOSDAQ")
            if latest_kospi_list is None:
                fallback = os.path.join(BASE_WORK_DIR, "input_KOSPI.csv")
                if os.path.exists(fallback):
                    latest_kospi_list = fallback
            if latest_kosdaq_list is None:
                fallback = os.path.join(BASE_WORK_DIR, "input_KOSDAQ.csv")
                if os.path.exists(fallback):
                    latest_kosdaq_list = fallback

            if latest_kospi_list is None and latest_kosdaq_list is None:
                module_UTIL.U0001_logging("N0011", "MISSING", "market lists (input_KOSPI-*.csv / input_KOSDAQ-*.csv)")
                t_end = pendulum.now(KST)
                module_UTIL.U0001_db_logging(
                    JOB_ID, 'check_inputs', 'FAILED', 'check_inputs failed: market lists missing.',
                    p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                )
                return False

            latest_kospi_days = _latest_csv_by_date(BASE_WORK_DIR, "input_KOSPI")
            if latest_kospi_days is None:
                fallback_days = os.path.join(BASE_WORK_DIR, "input_KOSPI.csv")
                if os.path.exists(fallback_days):
                    latest_kospi_days = fallback_days

            if latest_kospi_days is None:
                module_UTIL.U0001_logging("N0011", "MISSING", "input_KOSPI(-YYYYMMDD).csv for day_count")
                t_end = pendulum.now(KST)
                module_UTIL.U0001_db_logging(
                    JOB_ID, 'check_inputs', 'FAILED', 'check_inputs failed: day-count source missing.',
                    p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                )
                return False

            module_UTIL.U0001_logging(
                ctx["ds_nodash"], "N0011", "check_inputs",
                "ok",
                f"list_kospi={latest_kospi_list}",
                f"list_kosdaq={latest_kosdaq_list}",
                f"days={latest_kospi_days}",
            )
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_inputs', 'SUCCESS', 'check_inputs is finished.',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"list_kospi": latest_kospi_list, "list_kosdaq": latest_kosdaq_list, "days": latest_kospi_days},
            )
            return True
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_inputs', 'FAILED', f'check_inputs failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            # short_circuit 태스크의 원래 동작(입력 점검 실패 시 False 반환하여 다운스트림을
            # 정상적으로 skip)을 그대로 유지한다. raise 로 바꾸면 일시적 오류에도 DAG 전체가
            # 실패로 처리되어 기존 비즈니스 로직(그레이스풀 skip)이 바뀌므로 여기서는 raise 하지 않는다.
            return False

    # -----------------------------
    # 1) 최신 종목코드 리스트 수집
    # -----------------------------
    @task
    def get_equity_codes() -> list[str]:
        import pandas

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'get_equity_codes', 'STARTED', 'get_equity_codes is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            def _load_codes_from(latest_path: str | None) -> list[str]:
                if latest_path is None or not os.path.exists(latest_path):
                    return []
                df = pandas.read_csv(latest_path, dtype={"equity_code": str})
                if "equity_code" not in df.columns:
                    return []
                return df["equity_code"].dropna().astype(str).tolist()

            latest_kospi_list = _latest_csv_by_date(BASE_WORK_DIR, "input_KOSPI")
            latest_kosdaq_list = _latest_csv_by_date(BASE_WORK_DIR, "input_KOSDAQ")
            if latest_kospi_list is None:
                fallback = os.path.join(BASE_WORK_DIR, "input_KOSPI.csv")
                if os.path.exists(fallback):
                    latest_kospi_list = fallback
            if latest_kosdaq_list is None:
                fallback = os.path.join(BASE_WORK_DIR, "input_KOSDAQ.csv")
                if os.path.exists(fallback):
                    latest_kosdaq_list = fallback

            codes = set()
            codes.update(_load_codes_from(latest_kospi_list))
            codes.update(_load_codes_from(latest_kosdaq_list))

            codes = sorted(list(codes))

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'get_equity_codes', 'SUCCESS', 'get_equity_codes is finished.',
                p_section_count=len(codes), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"list_kospi": latest_kospi_list, "list_kosdaq": latest_kosdaq_list},
            )
            return codes
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'get_equity_codes', 'FAILED', f'get_equity_codes failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # -----------------------------
    # 2) 코드 리스트 → 배치 분할 (<= 800/배치)
    # -----------------------------
    @task
    def make_code_batches(codes: list[str]) -> list[list[str]]:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'make_code_batches', 'STARTED', 'make_code_batches is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"total_codes": len(codes), "batch_size": MAP_BATCH_SIZE},
        )
        try:
            batches = _chunk_list(codes, MAP_BATCH_SIZE)
            # 로그용으로 소형 메타 반환도 가능하지만, downstream map length 검사엔 영향 없음
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'make_code_batches', 'SUCCESS', 'make_code_batches is finished.',
                p_section_count=len(codes), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"batch_count": len(batches), "batch_size": MAP_BATCH_SIZE},
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
    # 3) 영업일 개수 산출 (input_KOSPI-YYYYMMDD.csv 기준)
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
            latest_kospi_days = _latest_csv_by_date(BASE_WORK_DIR, "input_KOSPI")
            if latest_kospi_days is None:
                latest_kospi_days = os.path.join(BASE_WORK_DIR, "input_KOSPI.csv")
            if not os.path.exists(latest_kospi_days):
                raise FileNotFoundError(f"Required day file not found: {latest_kospi_days}")
            df_days = pandas.read_csv(latest_kospi_days)
            day_count = int(df_days.shape[0])

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'get_day_count', 'SUCCESS', 'get_day_count is finished.',
                p_section_count=day_count, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"days_source": latest_kospi_days},
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
    # 4) 배치 단위 수집 (배치 내에서는 순차 처리)
    #    - 각 종목에 대해: prices-<code>.csv 로 저장
    #    - 반환: 처리한 종목 수(int)만 리턴 → XCom 부담 최소화
    #    - module_UTIL.U0001_save_hist_data() 내부에 요청당 2초 sleep 이 이미 포함되어 있으므로
    #      여기서 별도의 유휴 시간을 추가하지 않는다 (서버 부하 방지 목적은 이미 충족됨).
    #    - Dynamic Task Mapping 으로 실행되므로 get_current_context() 의 ti 에는 map_index 가
    #      포함되며, U0001_db_logging()이 이를 그대로 기록한다 (매핑 인스턴스별 개별 로그 행).
    # -----------------------------
    @task
    def collect_batch(code_batch: list[str], day_count: int) -> int:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_batch', 'STARTED', 'collect_batch is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"batch_size": len(code_batch), "day_count": day_count},
        )
        try:
            processed = 0
            for equity_code in code_batch:
                out_csv = os.path.join(HIST_DIR, f"prices-{equity_code}.csv")
                try:
                    module_UTIL.U0001_save_hist_data(out_csv, equity_code, day_count)
                    processed += 1
                except Exception as e:
                    # 개별 종목 실패는 로깅만 하고 배치 전체는 계속 진행 (기존 로직 유지)
                    module_UTIL.U0001_logging("collect_batch", "FAIL", equity_code, str(e))

            module_UTIL.U0001_logging(ctx["ds_nodash"], "N0011", "collect_batch", f"done processed={processed}")

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_batch', 'SUCCESS', 'collect_batch is finished.',
                p_section_count=processed, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"batch_size": len(code_batch), "processed": processed, "day_count": day_count},
            )
            return int(processed)
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_batch', 'FAILED', f'collect_batch failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"batch_size": len(code_batch), "error": str(e), "error_type": type(e).__name__},
            )
            raise

    # -----------------------------
    # 5) 마감 태스크: need_to_init.csv 생성 및 요약 로그
    #    - 전체 실행에 대한 요약 1건을 dag_collect_result 에 저장한다 (종목 단위로 수천 건을
    #      저장하지 않고, 배치/실행 단위로 요약하여 Superset 등에서 조회 부담을 줄인다).
    # -----------------------------
    @task(outlets=[module_DATA.DS_N0011])
    def finalize(batch_counts: list[int], total_codes, started_ts: str) -> str:
        import pandas
        ctx = get_current_context()
        t_start = pendulum.now(KST)

        # total_codes 는 DAG 배선상 get_equity_codes() 의 반환값(XCom, 코드 리스트)이 그대로
        # 전달된다. 리스트/정수 어느 쪽으로 오더라도 안전하게 "코드 개수"로 환산한다.
        if isinstance(total_codes, (list, tuple, set)):
            total_codes_count = len(total_codes)
        else:
            try:
                total_codes_count = int(total_codes)
            except (TypeError, ValueError):
                total_codes_count = 0

        module_UTIL.U0001_db_logging(
            JOB_ID, 'finalize', 'STARTED', 'finalize is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"started_ts": started_ts, "total_codes": total_codes_count, "batch_count": len(batch_counts)},
        )
        try:
            need_path = os.path.join(BASE_WORK_DIR, "need_to_init.csv")
            pandas.DataFrame([]).to_csv(need_path, index=False)

            total_processed = int(sum(batch_counts))

            # 전체 실행 단위 요약 1건을 공통 수집결과 테이블(dag_collect_result)에 저장
            module_UTIL.U0001_db_save_collect_result(
                p_job_id=JOB_ID,
                p_category='equity_price',
                p_item_code='ALL',
                p_item_name='NAVER 종목 시세 수집 (KOSPI/KOSDAQ 전체)',
                p_row_count=total_processed,
                p_file_path=HIST_DIR,
                p_message=(
                    f"started_at={started_ts}, total_codes={total_codes_count}, "
                    f"processed={total_processed}, batches={len(batch_counts)}"
                ),
                p_context=ctx,
                p_extra={"started_ts": started_ts, "total_codes": total_codes_count, "batch_counts": batch_counts},
            )

            module_UTIL.U0001_logging(
                ctx["ds_nodash"], "N0011", "finalize",
                f"started_at={started_ts}",
                f"total_codes={total_codes_count}",
                f"processed={total_processed}",
                f"need_to_init={need_path}",
            )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'finalize', 'SUCCESS', 'finalize is finished.',
                p_section_count=total_processed, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"started_ts": started_ts, "total_codes": total_codes_count, "need_to_init": need_path},
            )
            return need_path
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
        import time
        return str(time.time())

    # -----------------------------
    # 파이프라인 연결
    # -----------------------------
    ok = check_inputs()
    ts0 = _start_ts()
    codes = get_equity_codes()
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
    ok >> codes
    ok >> batches
    ok >> day_cnt

    # 요약/마감
    fin = finalize(batch_counts=mapped_collect, total_codes=codes, started_ts=ts0)
    [mapped_collect, codes, ts0] >> fin


N0011_naver_equity_prices()
