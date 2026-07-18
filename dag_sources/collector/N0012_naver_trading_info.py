# -*- coding: utf-8 -*-
"""
N0012_naver_trading_info
- NAVER 종목별 외인보유·순매매/기관 순매매 금액 히스토리 수집 (KOSPI/KOSDAQ 전체)
- 기존 PBCN_000400 크롤러를 Airflow TaskFlow로 변환
- Dynamic Task Mapping: 대용량 종목코드 → 배치로 나눠 매핑 (Airflow map length 1024 제한 회피)
- 동시 실행 5개 제한 (max_active_tis_per_dag=5)
- XCom 직렬화 안전: 정수/문자열/소형 리스트만 교환

경로 구성(입력/출력 분리):
- 입력 종목 리스트: INPUT_DIR = "/opt/apollon-data/finance-data/work/equities/"
  - 파일명: input_KOSPI-YYYYMMDD.csv / input_KOSDAQ-YYYYMMDD.csv (또는 fallback: input_KOSPI.csv / input_KOSDAQ.csv)
- 결과 저장: BASE_WORK_DIR = "/opt/apollon-data/finance-data/work/naver_frgn/"
  - HIST_DIR = "<BASE_WORK_DIR>/hist/"
  - 저장 파일명: trades-017860.csv (= trades-{equity_code}.csv)

주요 변경/보강:
- 멀티프로세싱 제거 → Airflow 동적 매핑 병렬화 사용
- 기존 CSV 있으면 뒤에서 10줄 제거 후 재수집/병합
- 금액 계산 로직 점검: org_trade_amount = price_close * org_trade_count (버그 수정 반영)
- 조기 종료/변동 없음 종료 조건 유지
- 페이지에서 0건이면 즉시 해당 종목 크롤링 종료 (요청 반영)
- (표준화) DB 접속정보는 Airflow Variable에서 조회하는 U0001_get_pg_conn_info() 기반으로만 사용
  (본 DAG는 실제로 DB 저장을 하지 않아 미사용 PgConfig/하드코딩 접속정보는 제거)
- (표준화) 모든 Task 시작/종료(성공/실패) 시점을 dag_job_log 에 기록 (module_UTIL.U0001_db_logging)
- (표준화) NAVER 서버 부하 방지를 위한 요청 간 유휴시간을 COLLECT_SLEEP_SEC 로 명시
- (버그 수정) finalize 에서 호출하던 module_UTIL.WAI_save_db(...) 는 U0001_functions.py 에 정의되어
  있지 않은 존재하지 않는 함수였다(호출 시 AttributeError 발생, try/except 로 조용히 무시되고 있었음).
  모든 DAG가 공통으로 사용하는 module_UTIL.U0001_db_save_collect_result(...) (dag_collect_result 테이블,
  Apache Superset 조회용) 호출로 교체하여 버그를 수정하고 수집결과 리포팅 요건을 충족시킨다.
"""
import os
import re
import pendulum

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.python import get_current_context

import U0001_functions as module_UTIL   # U0001_collect, U0001_logging, U0001_db_logging, U0001_db_save_collect_result (등)
import U0002_datasets as module_DATA    # DS_N0010, DS_N0012 (등)

# ===== 고정값/경로 =====
KST = "Asia/Seoul"

# 작업(Task) 실행 정보를 DB(dag_job_log)에 저장할 때 사용하는 작업 구분 키.
# 추후 다른 작업(N0013, N0014, ...)을 추가하면 각 DAG 파일에서 이 값만 다르게 지정하면 된다.
JOB_ID = "N0012"

# 서버 부하 방지를 위한 요청 간 유휴시간(초). U0001_collect()가 요청마다 이 시간만큼 sleep 한다.
COLLECT_SLEEP_SEC = 2

# 입력(종목코드 리스트) 위치: N0010과 동일 디렉터리 사용
INPUT_DIR = "/opt/apollon-data/finance-data/work/equities/"

# 결과 저장 루트
BASE_WORK_DIR = "/opt/apollon-data/finance-data/work/naver_frgn/"
HIST_DIR = os.path.join(BASE_WORK_DIR, "hist/")
for d in (BASE_WORK_DIR, HIST_DIR):
    os.makedirs(d, exist_ok=True)

# 배치 크기(1024 제한 여유있게)
MAP_BATCH_SIZE = 800

# NAVER 외인/기관 페이지
NAVER_BASE_URL = "https://finance.naver.com/item/frgn.naver"

# =========================
# 유틸 (태스크 아님)
# =========================
_DATE_RE = re.compile(r"(\d{8})")

def _latest_csv_by_date(dir_path: str, prefix: str) -> str | None:
    """
    디렉터리에서 'prefix-YYYYMMDD.csv' 패턴 중 가장 최신 날짜 파일 경로 반환.
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
    dag_id="N0012_naver_trading_info",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[module_DATA.DS_N0010],  # 영업일/선행 파이프라인 완료 후 실행
    catchup=False,
    tags=["PBCN_000400", "NAVER", "foreign/inst flows", "history"],
)
def N0012_naver_trading_info():
    """
    파이프라인 개요
    1) 입력 점검: KOSPI/KOSDAQ 목록 CSV 확인
    2) 최신(날짜 기준) KOSPI/KOSDAQ 종목 목록에서 종목코드 리스트 산출 (MARKET|CODE 포맷)
    3) 코드 리스트를 800개 단위 배치로 분할 (Airflow map length 1024 제한 회피)
    4) 배치 단위 수집(배치 내 순차): NAVER 페이지 페이징 크롤 → trades-<code>.csv 저장
    5) 최종 요약/로깅 및 완료 Dataset 방출
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
        module_UTIL.U0001_logging(ctx["ds_nodash"], "PBCN_000400", "check_inputs", "started")

        try:
            latest_kospi_list = _latest_csv_by_date(INPUT_DIR, "input_KOSPI")
            latest_kosdaq_list = _latest_csv_by_date(INPUT_DIR, "input_KOSDAQ")
            if latest_kospi_list is None:
                fallback = os.path.join(INPUT_DIR, "input_KOSPI.csv")
                if os.path.exists(fallback):
                    latest_kospi_list = fallback
            if latest_kosdaq_list is None:
                fallback = os.path.join(INPUT_DIR, "input_KOSDAQ.csv")
                if os.path.exists(fallback):
                    latest_kosdaq_list = fallback

            if latest_kospi_list is None and latest_kosdaq_list is None:
                module_UTIL.U0001_logging("PBCN_000400", "MISSING", "input_KOSPI/KOSDAQ csv lists")
                module_UTIL.U0001_logging(ctx["ds_nodash"], "PBCN_000400", "check_inputs", "failed")

                t_end = pendulum.now(KST)
                module_UTIL.U0001_db_logging(
                    JOB_ID, 'check_inputs', 'FAILED',
                    'check_inputs failed: missing input_KOSPI/KOSDAQ csv lists.',
                    p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                    p_extra={"input_dir": INPUT_DIR},
                )
                return False

            # 네이버 페이지 구조 간단 확인(삼성전자 005930 1페이지)
            try:
                res = module_UTIL.U0001_collect(
                    NAVER_BASE_URL, {"code": "005930", "page": 1}, p_sleep_time=COLLECT_SLEEP_SEC
                )
                import bs4
                soup = bs4.BeautifulSoup(res.content, "html.parser")
                tables = soup.find_all("table", {"class": "type2"})
                ok = len(tables) > 0
            except Exception as e:
                ok = False
                module_UTIL.U0001_logging("PBCN_000400", "check_inputs", "naver_probe_error", str(e))

            module_UTIL.U0001_logging(
                ctx["ds_nodash"], "PBCN_000400", "check_inputs",
                "ok" if ok else "not_ok",
                f"list_kospi={latest_kospi_list}",
                f"list_kosdaq={latest_kosdaq_list}",
            )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_inputs', 'SUCCESS' if ok else 'FAILED',
                f'check_inputs is finished. ok={ok}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"list_kospi": latest_kospi_list, "list_kosdaq": latest_kosdaq_list},
            )
            return ok
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
    #    --> MARKET|CODE 포맷으로 반환 (ex: "KOSPI|005930")
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

        def _load_codes_from(latest_path: str | None, market_label: str) -> dict:
            """
            파일에서 equity_code 열을 읽어 {code: market_label} 으로 반환.
            동일 코드가 여러 마켓에서 나오면 먼저 읽은 마켓이 우선됨.
            """
            out = {}
            if latest_path is None or not os.path.exists(latest_path):
                return out
            try:
                df = pandas.read_csv(latest_path, dtype={"equity_code": str})
            except Exception:
                return out
            if "equity_code" not in df.columns:
                return out
            for code in df["equity_code"].dropna().astype(str).tolist():
                # 이미 등록된 코드면 덮어쓰지 않음 (우선순위 유지)
                if code not in out:
                    out[code] = market_label
            return out

        try:
            latest_kospi_list = _latest_csv_by_date(INPUT_DIR, "input_KOSPI")
            latest_kosdaq_list = _latest_csv_by_date(INPUT_DIR, "input_KOSDAQ")
            if latest_kospi_list is None:
                fallback = os.path.join(INPUT_DIR, "input_KOSPI.csv")
                if os.path.exists(fallback):
                    latest_kospi_list = fallback
            if latest_kosdaq_list is None:
                fallback = os.path.join(INPUT_DIR, "input_KOSDAQ.csv")
                if os.path.exists(fallback):
                    latest_kosdaq_list = fallback

            code_market_map = {}
            # 우선 KOSPI (우선순위) -> KOSDAQ
            code_market_map.update(_load_codes_from(latest_kospi_list, "KOSPI"))
            qm = _load_codes_from(latest_kosdaq_list, "KOSDAQ")
            # KOSPI에 없는 코드만 추가
            for k, v in qm.items():
                if k not in code_market_map:
                    code_market_map[k] = v

            # 반환 형식: "MARKET|CODE"
            items = [f"{m}|{c}" for c, m in code_market_map.items()]
            items = sorted(items)

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'get_equity_codes', 'SUCCESS', f'get_equity_codes is finished. count={len(items)}',
                p_section_count=len(items), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"list_kospi": latest_kospi_list, "list_kosdaq": latest_kosdaq_list},
            )
            return items
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
            p_extra={"codes_count": len(codes or [])},
        )
        try:
            batches = _chunk_list(codes, MAP_BATCH_SIZE)

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'make_code_batches', 'SUCCESS',
                f'make_code_batches is finished. batches={len(batches)}',
                p_section_count=len(batches), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"batch_size": MAP_BATCH_SIZE, "codes_count": len(codes or [])},
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
    # 3) 배치 단위 수집 (배치 내 순차)
    #    - 각 종목에 대해 NAVER frgn 페이지 페이징 크롤
    #    - 결과: trades-<code>.csv 로 저장(증분 병합)
    #    - 반환: 처리 건수(int)만 리턴 → XCom 부담 최소화
    # -----------------------------
    @task
    def collect_batch(code_batch: list[str]) -> int:
        import pandas
        import numpy
        import bs4

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_batch', 'STARTED', f'collect_batch is started. size={len(code_batch)}',
            p_context=ctx, p_start_time=t_start,
            p_extra={"batch_size": len(code_batch)},
        )
        module_UTIL.U0001_logging(ctx["ds_nodash"], "PBCN_000400", "collect_batch", f"size={len(code_batch)}", "start")

        def _clean_num(s: str, repl_zero=True) -> float:
            s = s.strip().replace(",", "").replace("+", "").replace("%", "").replace("N/A", "0")
            if s == "" and repl_zero:
                return 0.0
            try:
                return float(s)
            except Exception:
                return 0.0

        def _collect_one_equity(entry: str) -> bool:
            """
            entry 형식: "MARKET|CODE"
            한 종목 코드 수집 후 CSV 저장. 성공/실패 반환.
            """
            try:
                market, equity_code = entry.split("|", 1)
            except Exception:
                # 포맷 문제가 있으면 로깅 후 실패 처리
                module_UTIL.U0001_logging("collect_one_equity", "BAD_ENTRY", entry)
                return False

            file_full_path = os.path.join(HIST_DIR, f"trades-{equity_code}.csv")

            # 기존 파일 로드 후 뒤에서 10줄 제거
            if os.path.exists(file_full_path):
                try:
                    df_equity = pandas.read_csv(file_full_path, dtype={"equity_code": object})
                    df_equity = df_equity.drop_duplicates()
                    if "eod_date" in df_equity.columns:
                        # eod_date가 문자열로 들어왔을 경우를 안전하게 처리
                        try:
                            df_equity["eod_date"] = df_equity["eod_date"].astype(int)
                        except Exception:
                            # 불가능하면 그대로 냅둠
                            pass
                    # 보유한 market 컬럼이 비어있거나 없는 경우도 있으므로 유지
                    df_equity = df_equity.sort_values(by=["eod_date"], ascending=True, ignore_index=True)
                    if df_equity.shape[0] > 10:
                        df_equity = df_equity.head(df_equity.shape[0] - 10)
                    last_count_of_date = df_equity.shape[0]
                except Exception:
                    df_equity = pandas.DataFrame()
                    last_count_of_date = 0
            else:
                df_equity = pandas.DataFrame()
                last_count_of_date = 0

            not_change_count = 0
            total_added = 0

            # NAVER는 과거 페이지로 갈수록 page++ 형태. 현실적으로 1~9999 범위 사용.
            for page_no in range(1, 9999):
                try:
                    res = module_UTIL.U0001_collect(
                        NAVER_BASE_URL, {"code": str(equity_code), "page": page_no},
                        p_sleep_time=COLLECT_SLEEP_SEC,
                    )
                    soup = bs4.BeautifulSoup(res.content, "html.parser")
                    table = soup.find_all("table", {"class": "type2"})
                    trs = bs4.BeautifulSoup(str(table), "html.parser").find_all("tr")

                    rows = []
                    loop_count = 0
                    for tr in trs:
                        tds = bs4.BeautifulSoup(str(tr), "html.parser").find_all("td")
                        if (len(tds) > 4) and (str(tds[0].text).strip().replace(".", "") != ""):
                            # 날짜 / 종가 / 전일비 / 등락률 / 거래량 / 기관-순매매량 / 외국인-순매매량 / 외국인-보유주수 / 외국인-보유율
                            eod_s = str(tds[0].text).strip().replace(".", "")
                            if eod_s == "":
                                continue
                            eod_date = int(eod_s)

                            price_close = _clean_num(tds[1].text)
                            org_trade_count = _clean_num(tds[5].text)
                            frg_trade_count = _clean_num(tds[6].text)
                            frg_hold_count  = _clean_num(tds[7].text)
                            frg_hold_ratio  = _clean_num(tds[8].text)

                            rows.append({
                                "market": market,  # 수정: market 값을 채움 (KOSPI 또는 KOSDAQ)
                                "equity_code": equity_code,
                                "eod_date": eod_date,
                                "price_close": price_close,
                                "org_trade_count": org_trade_count,
                                "frg_trade_count": frg_trade_count,
                                "frg_hold_count": frg_hold_count,
                                "frg_hold_ratio": frg_hold_ratio,
                            })
                            loop_count += 1

                    module_UTIL.U0001_logging("fetch", "PBCN_000400", equity_code, f"page={page_no}", f"rows={loop_count}")

                    # 바로 DataFrame으로 변환
                    df_page = pandas.DataFrame(rows)

                    # <<--- 수정된 부분: 이 페이지에서 0건이면 즉시 종료 (다음 페이지로 더 이상 가지 않음)
                    if df_page.shape[0] == 0:
                        module_UTIL.U0001_logging("fetch", "PBCN_000400", equity_code, f"page={page_no}", "rows=0 -> stop paging")
                        break
                    # --- 수정 끝 --->

                    if df_page.shape[0] > 0:
                        # 날짜 오름차순 정렬 및 carry-forward (외인보유주수 0 → 직전값)
                        df_page = df_page.sort_values(by=["eod_date"], ascending=True, ignore_index=True)
                        df_page["frg_hold_count"] = numpy.where(
                            df_page["frg_hold_count"] == 0,
                            df_page["frg_hold_count"].shift(1),
                            df_page["frg_hold_count"]
                        )

                        # 금액 파생
                        df_page["frg_hold_amount"]  = df_page["price_close"] * df_page["frg_hold_count"]
                        df_page["frg_trade_amount"] = df_page["price_close"] * df_page["frg_trade_count"]
                        # ✅ 버그 수정: org_trade_amount 는 org_trade_count 기준이어야 함
                        df_page["org_trade_amount"] = df_page["price_close"] * df_page["org_trade_count"]

                        df_page = df_page[
                            ["market", "equity_code", "eod_date",
                             "frg_hold_ratio", "frg_hold_amount", "frg_trade_amount", "org_trade_amount"]
                        ]

                        # 증분 병합
                        if df_equity.shape[0] > 0 and "eod_date" in df_equity.columns:
                            existing_dates = set(df_equity["eod_date"].unique())
                            df_add = df_page[~df_page["eod_date"].isin(existing_dates)]
                        else:
                            df_add = df_page

                        if df_add.shape[0] > 0:
                            # 기존 파일에 market 컬럼이 없거나 비어있어도 새 데이터에는 market이 채워짐
                            df_equity = pandas.concat([df_equity, df_add], ignore_index=True, sort=False)
                            df_equity = df_equity.drop_duplicates()
                            # eod_date를 int로 정리 시도
                            try:
                                df_equity["eod_date"] = df_equity["eod_date"].astype(int)
                            except Exception:
                                pass
                            df_equity = df_equity.sort_values(by=["eod_date"], ascending=True, ignore_index=True)

                            total_added += int(df_add.shape[0])

                            module_UTIL.U0001_logging(
                                "merge", "PBCN_000400", equity_code,
                                f"added={df_add.shape[0]}",
                                f"total={df_equity.shape[0]}",
                                "range:", int(df_equity["eod_date"].min()), "~", int(df_equity["eod_date"].max())
                            )

                        # 변화 체크
                        if last_count_of_date == df_equity.shape[0]:
                            not_change_count += 1
                        last_count_of_date = df_equity.shape[0]

                        # CSV 저장
                        try:
                            df_equity.to_csv(file_full_path, index=False)
                        except Exception as se:
                            module_UTIL.U0001_logging(file_full_path, "save_failed", str(se))

                        # 과거 충분히 내려갔으면 종료
                        if int(df_page["eod_date"].min()) < 20010101:
                            break

                    # 변화 없으면 루프 축소
                    if not_change_count > 3:
                        break

                except Exception as e:
                    module_UTIL.U0001_logging("error", "page", equity_code, page_no, str(e))
                    not_change_count += 1
                    if not_change_count > 5:
                        break

            # 최종 요약 로그
            try:
                if df_equity.shape[0] > 0:
                    module_UTIL.U0001_logging(
                        f"[{equity_code}] 수급 정보",
                        int(df_equity["eod_date"].min()),
                        int(df_equity["eod_date"].max()),
                        df_equity.shape[0],
                        f"added={total_added}"
                    )
            except Exception as e:
                module_UTIL.U0001_logging("final_log_error", equity_code, str(e))

            return True

        try:
            processed = 0
            for entry in code_batch:
                try:
                    ok = _collect_one_equity(entry)
                    if ok:
                        processed += 1
                except Exception as e:
                    # 개별 실패는 로깅만 하고 계속 진행
                    module_UTIL.U0001_logging("collect_batch", "FAIL", entry, str(e))

            module_UTIL.U0001_logging(ctx["ds_nodash"], "PBCN_000400", "collect_batch", f"done processed={processed}")

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_batch', 'SUCCESS', f'collect_batch is finished. processed={processed}',
                p_section_count=processed, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"batch_size": len(code_batch)},
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
    # 4) 마감 태스크: 전체 요약 로그/DB 기록
    # -----------------------------
    @task(outlets=[module_DATA.DS_N0012])
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
            # total_codes는 "MARKET|CODE" 포맷 리스트이므로 길이는 전체 코드 수
            module_UTIL.U0001_logging(
                ctx["ds_nodash"], "PBCN_000400", "finalize",
                f"started_at={started_ts}",
                f"total_codes={len(total_codes or [])}",
                f"processed={total_processed}",
                f"hist_dir={HIST_DIR}",
            )

            # 수집 결과를 모든 DAG 공통 포맷(dag_collect_result)에 요약 저장한다.
            # (버그 수정: 존재하지 않던 module_UTIL.WAI_save_db(...) 호출을
            #  module_UTIL.U0001_db_save_collect_result(...) 로 교체. 종목 수만 건을 다루므로
            #  종목 단위가 아니라 이번 실행 전체를 요약하는 1건으로 저장한다.)
            module_UTIL.U0001_db_save_collect_result(
                p_job_id=JOB_ID,
                p_category='trading_info',
                p_item_code='KOSPI+KOSDAQ',
                p_item_name='외인보유비율 및 기관,외국인 매매금액 정보 수집',
                p_row_count=total_processed,
                p_file_path=HIST_DIR,
                p_message=f"started_at={started_ts}, total_codes={len(total_codes or [])}, batches={len(batch_counts or [])}",
                p_context=ctx,
                p_extra={"batch_counts": batch_counts, "total_codes": len(total_codes or [])},
            )

            # 완료 마커 파일(optional)
            done_marker = os.path.join(BASE_WORK_DIR, "PBCN_000400.done")
            try:
                with open(done_marker, "w", encoding="utf-8") as f:
                    f.write("ok\n")
            except Exception:
                pass

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'finalize', 'SUCCESS', 'finalize is finished.',
                p_section_count=total_processed, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"done_marker": done_marker, "total_codes": len(total_codes or [])},
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
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, '_start_ts', 'STARTED', '_start_ts is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            import time
            ts = str(time.time())

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, '_start_ts', 'SUCCESS', '_start_ts is finished.',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"started_ts": ts},
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
    codes = get_equity_codes()
    batches = make_code_batches(codes)

    # 배치 단위 Dynamic Mapping (동시 실행 5개 제한)
    mapped_collect = (
        collect_batch
        .override(task_id="collect_batch", max_active_tis_per_dag=5)
        .expand(code_batch=batches)
    )

    # 의존성
    ok >> ts0
    ok >> codes
    ok >> batches

    # 요약/마감
    fin = finalize(batch_counts=mapped_collect, total_codes=codes, started_ts=ts0)
    [mapped_collect, codes, ts0] >> fin


N0012_naver_trading_info()
