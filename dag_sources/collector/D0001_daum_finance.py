import pendulum
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup

import os
import pandas
import time
import numpy

import U0001_functions as module_UTIL
import U0002_datasets as module_DATA

# ===== 공통 상수 =====
KST = "Asia/Seoul"

# 작업(Task) 실행 정보를 DB(dag_job_log)에 저장할 때 사용하는 작업 구분 키.
# 추후 다른 작업(D0002, D0003, ...)을 추가하면 각 DAG 파일에서 이 값만 다르게 지정하면 된다.
JOB_ID = "D0001"

# 서버 부하 방지를 위한 요청 간 유휴시간(초). U0001_collect()가 요청마다 이 시간만큼 sleep 한다.
COLLECT_SLEEP_SEC = 2

# 파일 저장 경로
WORK_PATH = "/opt/apollon-data/finance-data/work/bond/"


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


@dag(
    dag_id="D0001_daum_finance",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[module_DATA.DS_A0003],  # 달력 데이터 세팅 완료 후 실행
    catchup=False,
    tags=["D0001_daum_finance", "Market index"],
)
def D0001_daum_finance():

    list_global_index = [
        {'name':'KRCD',     'code':'BOND-/KRCD=KQ',     'file_name': 'input_bond_BOND_KRCD_KQ'},       # CD(91일)
        {'name':'KRCP',     'code':'BOND-/KRCP=KQ',     'file_name': 'input_bond_BOND_KRCP_KQ'},       # CP(91일)
        {'name':'KRGUCORP', 'code':'BOND-/KRGUCORP=KQ', 'file_name': 'input_bond_BOND_KRGUCORP_KQ'},   # 회사채(무보증AA-)
        {'name':'KRTSY3Y',  'code':'BOND-/KRTSY3Y=KQ',  'file_name': 'input_bond_BOND_KRTSY3Y_KQ'},    # 국고채(3년)
        {'name':'KRCALL',   'code':'BOND-KRCALL=HWH',   'file_name': 'input_bond_BOND_KRCALL_HWHA'},   # 콜금리
        {'name':'US30YT',   'code':'BOND-US30YT=XX',    'file_name': 'input_bond_BOND_US30YT_XX'},     # 미국국채30년
    ]
    base_url = "https://finance.daum.net/api/domestic/exchanges/ITEM_NAME/days"

    @task.short_circuit(inlets=[module_DATA.DS_A0003])
    def check_available(p_dummy=None):
        import json
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'check_available', 'STARTED', 'check_available is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"ds_nodash": ctx.get("ds_nodash")},
        )
        try:
            sample_item_code = list_global_index[0]['code']
            sample_item_name = list_global_index[0]['name']
            param = {
                'symbolCode': sample_item_code,
                'page': 1,
                'perPage': 30,
                'fieldName': 'changeRate',
                'order': 'desc',
                'pagination': 'true',
            }
            results = module_UTIL.U0001_collect(
                base_url.replace('ITEM_NAME', sample_item_name), param, p_sleep_time=COLLECT_SLEEP_SEC
            )
            json_data = json.loads(results.text)['data']
            _ = json_data[0]['date']  # 간단 검증
            module_UTIL.U0001_logging(ctx["ds_nodash"], 'D0001', 'check_available', 'finished')

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_available', 'SUCCESS', 'check_available is finished.',
                p_section_count=len(json_data), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"item": sample_item_name},
            )
            return True
        except Exception as e:
            import sys
            exc_class, val, tb_ob = sys.exc_info()
            module_UTIL.U0001_logging(tb_ob.tb_lineno, val)
            module_UTIL.U0001_logging(ctx["ds_nodash"], 'D0001', 'check_available', f'ERROR: {e}')

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_available', 'FAILED', f'check_available failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            # short_circuit 태스크의 원래 동작(소스 점검 실패 시 False 반환하여 다운스트림을
            # 정상적으로 skip)을 그대로 유지한다. raise 로 바꾸면 일시적 오류에도 DAG 전체가
            # 실패로 처리되어 기존 비즈니스 로직(그레이스풀 skip)이 바뀌므로 여기서는 raise 하지 않는다.
            return False

    @task
    def fetch_data(p_page_from, p_page_to, p_equity_code, p_equity_name, p_file_full_path, p_dummy=None):
        import json
        import pandas as pd

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'fetch_data', 'STARTED', 'fetch_data is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={
                "equity_name": p_equity_name, "equity_code": p_equity_code,
                "file_path": p_file_full_path, "page_from": p_page_from, "page_to": p_page_to,
            },
        )
        try:
            _ensure_dir(os.path.dirname(p_file_full_path))

            if os.path.exists(p_file_full_path):
                df_index = pd.read_csv(p_file_full_path).drop_duplicates()
                if 'eod_date' in df_index: df_index['eod_date'] = df_index['eod_date'].astype(int)
                for col in ['price_close', 'diff', 'rate']:
                    if col in df_index: df_index[col] = df_index[col].astype(float)
                df_index = df_index.sort_values(by=['eod_date'], ascending=True)
                if df_index.shape[0] > 10:
                    df_index = df_index.head(df_index.shape[0] - 10)
                last_count_of_date = df_index.shape[0]
            else:
                df_index = pandas.DataFrame()
                last_count_of_date = 0

            not_change_count = 0
            for page_no in range(p_page_from, p_page_to + 1):
                try:
                    list_price = []
                    param = {
                        'symbolCode': p_equity_code,
                        'page': page_no,
                        'perPage': 30,
                        'fieldName': 'changeRate',
                        'order': 'desc',
                        'pagination': 'true',
                    }
                    results = module_UTIL.U0001_collect(
                        base_url.replace('ITEM_NAME', p_equity_name), param, p_sleep_time=COLLECT_SLEEP_SEC
                    )
                    json_data = json.loads(results.text)['data']
                    for row in json_data:
                        list_price.append({
                            'eod_date': int(str(row['date']).replace('-', '')),
                            'item_code': row['symbolCode'],
                            'price_close': row['tradePrice'],
                            'diff': row['changePrice'],
                            'rate': row['changeRate'],
                        })
                    df_page = pandas.DataFrame(list_price)
                    if df_page.shape[0] > 0:
                        df_page['eod_date'] = df_page['eod_date'].astype(int)
                except Exception as e:
                    module_UTIL.U0001_logging(p_equity_code, page_no, f'parse_error: {e}')
                    not_change_count += 1
                    df_page = pandas.DataFrame()

                # 페이지 수집 직후 shape 로그
                module_UTIL.U0001_logging('D0001', 'shape', f'{p_equity_name}', f'page={page_no}', f'df_page={tuple(df_page.shape)}', f'df_index={tuple(df_index.shape)}')

                if df_page.shape[0] > 0:
                    prev_rows = df_index.shape[0]
                    if prev_rows > 0:
                        df_keep = df_index[~df_index['eod_date'].isin(set(df_page['eod_date'].unique()))]
                    else:
                        df_keep = df_index
                    df_index = pandas.concat([df_keep, df_page], sort=False).drop_duplicates()
                    df_index['eod_date'] = df_index['eod_date'].astype(int)
                    df_index = df_index.sort_values(by=['eod_date'], ascending=True)

                    # 병합 직후 shape 로그
                    module_UTIL.U0001_logging('D0001', 'merged', f'{p_equity_name}', f'page={page_no}', f'df_page={tuple(df_page.shape)}', f'df_index={tuple(df_index.shape)}')

                    module_UTIL.U0001_logging(
                        'PBCD_000100', not_change_count, p_equity_code, last_count_of_date, df_index.shape[0],
                        'Start:', int(df_index['eod_date'].min()) if df_index.shape[0] else None,
                        ',Finish:', int(df_index['eod_date'].max()) if df_index.shape[0] else None
                    )

                    # 증가 없으면 누적
                    if prev_rows == df_index.shape[0]:
                        not_change_count += 1
                    else:
                        not_change_count = 0
                    last_count_of_date = df_index.shape[0]

                    # 증분 저장 (CSV)
                    df_index.to_csv(p_file_full_path, index=None)

                    # 하한 도달 시 조기 종료
                    try:
                        if int(df_page['eod_date'].min()) < 20010101:
                            module_UTIL.U0001_logging(p_equity_name, page_no, "break_on_min_date")
                            break
                    except Exception:
                        pass

                # 5회 이상 정체 시 중단
                if not_change_count > 5:
                    module_UTIL.U0001_logging(p_equity_name, page_no, "break_on_stagnate", f"streak={not_change_count}")
                    break

            # 종료 처리: 결과 요약, 수집 결과 저장(항목별 1건), Task 로깅
            min_dt = int(df_index['eod_date'].min()) if df_index.shape[0] else 0
            max_dt = int(df_index['eod_date'].max()) if df_index.shape[0] else 0

            module_UTIL.U0001_db_save_collect_result(
                JOB_ID, p_category='bond', p_item_code=p_equity_code, p_item_name=p_equity_name,
                p_row_count=int(df_index.shape[0]), p_eod_date_min=min_dt, p_eod_date_max=max_dt,
                p_file_path=p_file_full_path, p_context=ctx,
            )
            module_UTIL.U0001_logging(
                f"[{JOB_ID}][fetch_data] {p_equity_name}({p_equity_code}) rows={df_index.shape[0]} "
                f"range={min_dt}~{max_dt} file={p_file_full_path}"
            )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'fetch_data', 'SUCCESS', 'fetch_data is finished.',
                p_section_count=int(df_index.shape[0]), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={
                    "equity_name": p_equity_name, "equity_code": p_equity_code,
                    "eod_date_min": min_dt, "eod_date_max": max_dt,
                },
            )
            return df_index
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'fetch_data', 'FAILED', f'fetch_data failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={
                    "equity_name": p_equity_name, "equity_code": p_equity_code,
                    "error": str(e), "error_type": type(e).__name__,
                },
            )
            raise

    @task(outlets=[module_DATA.DS_D0001])
    def task_to_close():
        """
        WORK_PATH(예: /opt/apollon-data/finance-data/work/bond/)의 결과 파일을
        /opt/apollon-data/finance-data/work.bond.tar.gz 로 tar.gz 압축 저장.
        - 아카이브 내 경로는 'work/bond/...' 형태의 상대경로로 기록
        - 원자적 저장(os.replace) 적용
        """
        import tarfile
        import os

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'task_to_close', 'STARTED', 'task_to_close is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            out_dir = "/opt/apollon-data/finance-data"
            _ensure_dir(out_dir)
            _ensure_dir(WORK_PATH)

            tar_path = os.path.join(out_dir, "work.bond.tar.gz")
            tmp_path = tar_path + ".tmp"

            # tar 생성
            with tarfile.open(tmp_path, "w:gz") as tar:
                base = "/opt/apollon-data/finance-data/"
                for root, dirs, files in os.walk(WORK_PATH):
                    for fname in files:
                        full_path = os.path.join(root, fname)
                        arcname = os.path.relpath(full_path, start=base)
                        tar.add(full_path, arcname=arcname)

            os.replace(tmp_path, tar_path)

            try:
                fsize = os.path.getsize(tar_path)
            except Exception:
                fsize = 0

            # DAG 종료 시점의 전체 수집 결과 요약 출력 (항목별 요약은 fetch_data 에서 이미 출력됨)
            module_UTIL.U0001_logging(
                f"[{JOB_ID}][task_to_close] DAG finished. archive={tar_path} "
                f"size_bytes={fsize} item_count={len(list_global_index)}"
            )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'task_to_close', 'SUCCESS', 'task_to_close is finished.',
                p_section_value=float(fsize), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"archive": tar_path, "size_bytes": fsize, "item_count": len(list_global_index)},
            )
            return "ok"
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'task_to_close', 'FAILED', f'task_to_close failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task
    def debug_df(df: pandas.DataFrame) -> None:
        import logging

        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'debug_df', 'STARTED', 'debug_df is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            log = logging.getLogger("airflow.task")
            log.info("df.shape = %s", df.shape)
            log.info("HEAD\n%s", df.head().to_string())
            log.info("TAIL\n%s", df.tail().to_string())

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'debug_df', 'SUCCESS', 'debug_df is finished.',
                p_section_count=int(df.shape[0]), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
            )
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'debug_df', 'FAILED', f'debug_df failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    # ───────── 파이프라인 (순차 실행) ─────────
    flag_available = check_available(module_DATA.DS_A0003)  # ShortCircuit 게이트

    groups = []
    group_end_tasks = []  # 각 그룹의 마지막 태스크(여기서는 t_debug)

    for idx, l_equity in enumerate(list_global_index):
        equity_code = l_equity['code']
        equity_name = l_equity['name']
        file_name   = l_equity['file_name']
        file_full_path = f"{WORK_PATH}{file_name}.csv"

        with TaskGroup(group_id=f"G_{equity_name}", tooltip=f"{equity_name} 수집/저장") as grp:
            t_fetch = fetch_data.override(task_id=f"fetch_{equity_name}")(
                1, 300, equity_code, equity_name, file_full_path, p_dummy=None
            )
            t_debug = debug_df.override(task_id=f"debug_{equity_name}")(t_fetch)
            t_fetch >> t_debug

        groups.append(grp)
        group_end_tasks.append(t_debug)

        if idx == 0:
            flag_available >> grp
        else:
            prev_end = group_end_tasks[idx - 1]
            prev_end >> grp

    t_close = task_to_close()
    group_end_tasks[-1] >> t_close

D0001_daum_finance()
