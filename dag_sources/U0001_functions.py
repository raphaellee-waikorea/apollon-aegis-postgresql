import math
NULL_VALUE = math.pi

# =============================================================================================================================================================================================
# 로깅 함수
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def U0001_logging(*messages):
    import datetime
    import sys
    log_time = datetime.datetime.today().strftime('%Y/%m/%d %H:%M:%S')
    log = list()
    for message in messages:
        log.append(str(message))
    print('[' + log_time + ']::[' + ' '.join(log) + ']')
    sys.stdout.flush()
# =============================================================================================================================================================================================


# =============================================================================================================================================================================================
# HTML 수집 함수
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#   input
#       - p_url: 대상 사이트 주소
#       - p_param: 주요 파라미터
#       - p_sleep_time: 수집 간격
#       - p_flag_view_url: 파라미터가 반영된 최종 URL 주소
#       - p_referer: 직전 방문 페이지
#   output: 수집결과
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def U0001_collect(p_url, p_param, p_sleep_time=2, p_flag_view_url=True, p_referer='http://finance.daum.net/domestic/exchange/COMMODITY-%2FCLc1', p_flag_post=False):
    import time
    import urllib
    import requests

    url_full_path = p_url
    if len(p_param) > 0:
        url_full_path = url_full_path + '?' + urllib.parse.urlencode(p_param)
    if p_flag_view_url:
        U0001_logging(url_full_path)
    headers = {
        'content-type': 'application/json, text/javascript, */*; q=0.01',
        'User-Agent': 'Mozilla/5.0 AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15',
        'referer': p_referer,
    }
    try:
        if p_flag_post:
            results = requests.post(p_url, data=p_param, headers=headers)
        else:
            results = requests.get(url_full_path, headers=headers)
        time.sleep(p_sleep_time)
        return results
    except:
        time.sleep(p_sleep_time * 2)
        if p_flag_post:
            results = requests.post(p_url, data=p_param, headers=headers)
        else:
            results = requests.get(url_full_path, headers=headers)
        time.sleep(p_sleep_time)
        return results
# =============================================================================================================================================================================================


# =============================================================================================================================================================================================
# PostgreSQL 접속 정보 조회 (Airflow Variable 사용)
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#   DB 접속 정보는 소스코드에 하드코딩하지 않고, Airflow Variable 에서 조회한다.
#   Airflow UI > Admin > Variables 에 아래 키들을 등록해두어야 한다.
#
#       key                     설명                                예시 값
#       ----------------------  ----------------------------------  --------------------
#       apollon_db_host         PostgreSQL Host                      apollon-db
#       apollon_db_port         PostgreSQL Port                      5432
#       apollon_db_name         Database 명                          apollondb
#       apollon_db_user         접속 계정                             apollon
#       apollon_db_pass         접속 비밀번호                          ********
#       apollon_db_schema       기본 스키마 (선택, 기본값 public)      public
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
AIRFLOW_VAR_DB_HOST = "apollon_db_host"
AIRFLOW_VAR_DB_PORT = "apollon_db_port"
AIRFLOW_VAR_DB_NAME = "apollon_db_name"
AIRFLOW_VAR_DB_USER = "apollon_db_user"
AIRFLOW_VAR_DB_PASS = "apollon_db_pass"
AIRFLOW_VAR_DB_SCHEMA = "apollon_db_schema"


def U0001_get_pg_conn_info():
    """
    Airflow Variable 에서 PostgreSQL 접속 정보를 조회하여 dict 로 반환한다.
        {"host":..., "port":..., "db":..., "user":..., "password":..., "schema":...}
    """
    from airflow.models import Variable

    return {
        "host": Variable.get(AIRFLOW_VAR_DB_HOST),
        "port": int(Variable.get(AIRFLOW_VAR_DB_PORT, default_var=5432)),
        "db": Variable.get(AIRFLOW_VAR_DB_NAME),
        "user": Variable.get(AIRFLOW_VAR_DB_USER),
        "password": Variable.get(AIRFLOW_VAR_DB_PASS),
        "schema": Variable.get(AIRFLOW_VAR_DB_SCHEMA, default_var="public"),
    }


def U0001_get_pg_engine():
    """
    U0001_get_pg_conn_info() 의 접속 정보로 SQLAlchemy Engine 을 생성한다.
    (DataFrame.to_sql 등 SQLAlchemy 기반 저장 작업에 사용)
    """
    from sqlalchemy import create_engine

    info = U0001_get_pg_conn_info()
    url = f"postgresql+psycopg2://{info['user']}:{info['password']}@{info['host']}:{info['port']}/{info['db']}"
    return create_engine(url)
# =============================================================================================================================================================================================


# =============================================================================================================================================================================================
# 작업(Task) 실행 정보 DB 저장
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#   - DB 접속 정보는 U0001_get_pg_conn_info() 를 통해 Airflow Variable 에서 조회한다.
#   - 로그 테이블(JOB_LOG_SCHEMA.JOB_LOG_TABLE)이 없으면 자동으로 생성한다(CREATE TABLE IF NOT EXISTS).
#   - job_id : 작업(DAG)을 구분하는 키. 예) 'A0003'. 추후 다른 작업(A0004, A0005, ...)이 추가되어도
#              동일한 테이블에서 job_id 값으로 구분하여 조회/관리할 수 있다.
#   - run_id : Airflow DAG Run ID. 동일 job_id 내에서 "실행 회차"를 구분하는 키.
#   - job_id + run_id + task_id(+try_number) 조합으로 특정 작업의 특정 실행 회차, 특정 Task 의 실행
#     내역을 정확히 식별할 수 있다. (조회 성능을 위해 해당 조합에 인덱스를 생성한다)
#   - Task 시작/종료(성공/실패) 시점마다 각각 별도의 행(row)으로 저장한다 (Task 별로 각각 저장).
#   - 가능한 한 상세한 정보(dag_id, task_id, run_id, try_number, map_index, logical_date,
#     data_interval, 처리 건수, 소요 시간, 예외/파라미터 등 임의의 부가정보(JSONB))를 함께 저장한다.
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
JOB_LOG_SCHEMA = "public"
JOB_LOG_TABLE = "dag_job_log"

_JOB_LOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {JOB_LOG_SCHEMA}.{JOB_LOG_TABLE}
(
    log_id              BIGSERIAL PRIMARY KEY,
    job_id              VARCHAR(50)      NOT NULL,             -- 작업 구분 키 (예: 'A0003')
    dag_id               VARCHAR(250),
    task_id              VARCHAR(250),
    run_id               VARCHAR(250),                          -- Airflow DAG Run ID (실행 회차 식별자)
    try_number           INTEGER,
    map_index            INTEGER,
    logical_date         TIMESTAMPTZ,
    data_interval_start  TIMESTAMPTZ,
    data_interval_end    TIMESTAMPTZ,
    yyyymmdd             CHAR(8),
    status                VARCHAR(20)     NOT NULL DEFAULT 'INFO',   -- STARTED / SUCCESS / FAILED / INFO
    section_message       TEXT,
    section_count         INTEGER         NOT NULL DEFAULT 0,
    section_value         DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    extra                 JSONB,                                 -- 파라미터, 예외정보 등 부가 상세정보
    hostname               VARCHAR(200),
    start_time             TIMESTAMPTZ,
    end_time               TIMESTAMPTZ,
    duration_sec           DOUBLE PRECISION,
    exec_time               TIMESTAMPTZ    NOT NULL DEFAULT now()
);
"""

_JOB_LOG_INDEX_DDL = [
    f'CREATE INDEX IF NOT EXISTS ix_{JOB_LOG_TABLE}_job_run_task ON {JOB_LOG_SCHEMA}.{JOB_LOG_TABLE} (job_id, run_id, task_id);',
    f'CREATE INDEX IF NOT EXISTS ix_{JOB_LOG_TABLE}_job_id ON {JOB_LOG_SCHEMA}.{JOB_LOG_TABLE} (job_id);',
    f'CREATE INDEX IF NOT EXISTS ix_{JOB_LOG_TABLE}_exec_time ON {JOB_LOG_SCHEMA}.{JOB_LOG_TABLE} (exec_time);',
]


def U0001_db_logging(p_job_id, p_task_name, p_status, p_message,
                      p_section_count=0, p_section_value=0.0,
                      p_yyyymmdd=None, p_context=None, p_extra=None,
                      p_start_time=None, p_end_time=None):
    """
    작업(Task) 실행 정보를 DB(dag_job_log)에 저장한다.

    p_job_id       : 작업 구분 키 (예: 'A0003')
    p_task_name    : Task 명 (예: 'get_calendar'). Airflow context 가 없을 경우 task_id 로도 사용됨
    p_status       : 'STARTED' / 'SUCCESS' / 'FAILED' / 'INFO' 등 상태값
    p_message      : 상세 메시지
    p_section_count: 처리 건수(행 수 등)
    p_section_value: 부가 수치값
    p_yyyymmdd     : 기준일자(YYYYMMDD). 미지정 시 Airflow context 의 ds_nodash 사용
    p_context      : Airflow get_current_context() 결과. dag_id/task_id/run_id/try_number 등 자동 추출
    p_extra        : dict. 그 외 상세정보(파라미터, 예외 stacktrace 등) - JSONB 로 저장됨
    p_start_time   : Task(구간) 시작 시각 (datetime)
    p_end_time     : Task(구간) 종료 시각 (datetime) - p_start_time 과 함께 주면 duration_sec 계산됨
    """
    import json
    import socket
    import psycopg2

    conn_info = U0001_get_pg_conn_info()

    ti = p_context.get('ti') if p_context else None
    dag_run = p_context.get('dag_run') if p_context else None

    dag_id = getattr(ti, 'dag_id', None)
    task_id = getattr(ti, 'task_id', None) or p_task_name
    run_id = getattr(ti, 'run_id', None) or getattr(dag_run, 'run_id', None)
    try_number = getattr(ti, 'try_number', None)
    map_index = getattr(ti, 'map_index', None)

    logical_date = None
    data_interval_start = None
    data_interval_end = None
    yyyymmdd = p_yyyymmdd
    if p_context:
        logical_date = p_context.get('logical_date') or p_context.get('execution_date')
        data_interval_start = p_context.get('data_interval_start')
        data_interval_end = p_context.get('data_interval_end')
        if not yyyymmdd:
            yyyymmdd = p_context.get('ds_nodash')

    hostname = getattr(ti, 'hostname', None) or socket.gethostname()

    duration_sec = None
    if p_start_time is not None and p_end_time is not None:
        try:
            duration_sec = (p_end_time - p_start_time).total_seconds()
        except Exception:
            duration_sec = None

    extra_json = json.dumps(p_extra, ensure_ascii=False, default=str) if p_extra else None

    conn = psycopg2.connect(
        host=conn_info["host"],
        dbname=conn_info["db"],
        user=conn_info["user"],
        password=conn_info["password"],
        port=conn_info["port"],
        application_name="U0001_db_logging",
    )
    try:
        with conn:
            with conn.cursor() as curs:
                # 로그 테이블이 없으면 생성
                curs.execute(_JOB_LOG_DDL)
                for ddl in _JOB_LOG_INDEX_DDL:
                    curs.execute(ddl)

                curs.execute(
                    f"""
                    INSERT INTO {JOB_LOG_SCHEMA}.{JOB_LOG_TABLE}
                        (job_id, dag_id, task_id, run_id, try_number, map_index,
                         logical_date, data_interval_start, data_interval_end, yyyymmdd,
                         status, section_message, section_count, section_value,
                         extra, hostname, start_time, end_time, duration_sec)
                    VALUES
                        (%s, %s, %s, %s, %s, %s,
                         %s, %s, %s, %s,
                         %s, %s, %s, %s,
                         %s, %s, %s, %s, %s)
                    """,
                    (
                        p_job_id, dag_id, task_id, run_id, try_number, map_index,
                        logical_date, data_interval_start, data_interval_end, yyyymmdd,
                        p_status, p_message, int(p_section_count), float(p_section_value),
                        extra_json, hostname, p_start_time, p_end_time, duration_sec,
                    ),
                )
    finally:
        conn.close()
# =============================================================================================================================================================================================

# =============================================================================================================================================================================================
# 수집 결과 DB 저장 (모든 DAG 공통 포맷)
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#   - dag_job_log(Task 실행 상태 로그)와는 별도로, "무엇을 얼마나 수집했는지"를 항목 단위로 기록하는
#     테이블이다. 모든 DAG가 동일한 포맷/테이블(dag_collect_result)을 사용하므로 Apache Superset 등
#     BI 도구에서 하나의 테이블만 조회하면 전체 DAG의 수집 현황을 파악할 수 있다.
#   - 테이블이 없으면 호출 시점에 자동으로 생성한다(CREATE TABLE IF NOT EXISTS).
#   - job_id 로 작업을 구분하고, run_id 로 실행 회차를 구분한다(dag_job_log 와 동일한 규칙).
#   - category/item_code/item_name 단위로 한 행씩 저장한다. 개별 종목 수만 건을 수집하는 DAG
#     (N0011/N0012/N0013 등)는 종목 단위가 아니라 배치/마켓 단위로 요약해서 저장하는 것을 권장한다.
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
COLLECT_RESULT_SCHEMA = "public"
COLLECT_RESULT_TABLE = "dag_collect_result"

_COLLECT_RESULT_DDL = f"""
CREATE TABLE IF NOT EXISTS {COLLECT_RESULT_SCHEMA}.{COLLECT_RESULT_TABLE}
(
    result_id       BIGSERIAL PRIMARY KEY,
    job_id          VARCHAR(50)      NOT NULL,     -- 작업 구분 키 (예: 'D0001')
    dag_id          VARCHAR(250),
    task_id         VARCHAR(250),
    run_id          VARCHAR(250),                   -- Airflow DAG Run ID (실행 회차 식별자)
    logical_date    TIMESTAMPTZ,
    yyyymmdd        CHAR(8),
    category        VARCHAR(100),                   -- 수집 대상 분류 (예: 'bond','fx','oil','equity_price', ...)
    item_code       VARCHAR(200),                    -- 수집 대상 코드
    item_name       VARCHAR(400),                    -- 수집 대상 명칭
    row_count       INTEGER          NOT NULL DEFAULT 0,
    eod_date_min    INTEGER,
    eod_date_max    INTEGER,
    file_path       TEXT,
    message         TEXT,
    extra           JSONB,
    created_at      TIMESTAMPTZ      NOT NULL DEFAULT now()
);
"""

_COLLECT_RESULT_INDEX_DDL = [
    f'CREATE INDEX IF NOT EXISTS ix_{COLLECT_RESULT_TABLE}_job_run ON {COLLECT_RESULT_SCHEMA}.{COLLECT_RESULT_TABLE} (job_id, run_id);',
    f'CREATE INDEX IF NOT EXISTS ix_{COLLECT_RESULT_TABLE}_job_id ON {COLLECT_RESULT_SCHEMA}.{COLLECT_RESULT_TABLE} (job_id);',
    f'CREATE INDEX IF NOT EXISTS ix_{COLLECT_RESULT_TABLE}_created_at ON {COLLECT_RESULT_SCHEMA}.{COLLECT_RESULT_TABLE} (created_at);',
]


def U0001_db_save_collect_result(p_job_id, p_category, p_item_code, p_item_name,
                                  p_row_count=0, p_eod_date_min=None, p_eod_date_max=None,
                                  p_file_path=None, p_message=None, p_context=None, p_extra=None):
    """
    DAG의 수집 결과를 dag_collect_result 테이블에 저장한다 (모든 DAG 공통 포맷).

    p_job_id        : 작업 구분 키 (예: 'D0001')
    p_category      : 수집 대상 분류 (예: 'bond', 'fx', 'oil', 'equity_price', 'etf', 'news' ...)
    p_item_code     : 수집 대상 코드 (종목코드/통화코드/상품코드 등)
    p_item_name     : 수집 대상 명칭
    p_row_count     : 수집/저장된 행 수
    p_eod_date_min  : 수집 데이터의 최소 기준일자(YYYYMMDD, int)
    p_eod_date_max  : 수집 데이터의 최대 기준일자(YYYYMMDD, int)
    p_file_path     : 저장된 파일 경로(있는 경우)
    p_message       : 부가 메시지
    p_context       : Airflow get_current_context() 결과. dag_id/task_id/run_id 등 자동 추출
    p_extra         : dict. 그 외 상세정보 - JSONB 로 저장됨
    """
    import json
    import psycopg2

    conn_info = U0001_get_pg_conn_info()

    ti = p_context.get('ti') if p_context else None
    dag_run = p_context.get('dag_run') if p_context else None

    dag_id = getattr(ti, 'dag_id', None)
    task_id = getattr(ti, 'task_id', None)
    run_id = getattr(ti, 'run_id', None) or getattr(dag_run, 'run_id', None)

    logical_date = None
    yyyymmdd = None
    if p_context:
        logical_date = p_context.get('logical_date') or p_context.get('execution_date')
        yyyymmdd = p_context.get('ds_nodash')

    extra_json = json.dumps(p_extra, ensure_ascii=False, default=str) if p_extra else None

    conn = psycopg2.connect(
        host=conn_info["host"],
        dbname=conn_info["db"],
        user=conn_info["user"],
        password=conn_info["password"],
        port=conn_info["port"],
        application_name="U0001_db_save_collect_result",
    )
    try:
        with conn:
            with conn.cursor() as curs:
                # 결과 테이블이 없으면 생성
                curs.execute(_COLLECT_RESULT_DDL)
                for ddl in _COLLECT_RESULT_INDEX_DDL:
                    curs.execute(ddl)

                curs.execute(
                    f"""
                    INSERT INTO {COLLECT_RESULT_SCHEMA}.{COLLECT_RESULT_TABLE}
                        (job_id, dag_id, task_id, run_id, logical_date, yyyymmdd,
                         category, item_code, item_name, row_count,
                         eod_date_min, eod_date_max, file_path, message, extra)
                    VALUES
                        (%s, %s, %s, %s, %s, %s,
                         %s, %s, %s, %s,
                         %s, %s, %s, %s, %s)
                    """,
                    (
                        p_job_id, dag_id, task_id, run_id, logical_date, yyyymmdd,
                        p_category, p_item_code, p_item_name, int(p_row_count or 0),
                        p_eod_date_min, p_eod_date_max, p_file_path, p_message, extra_json,
                    ),
                )
    finally:
        conn.close()
# =============================================================================================================================================================================================

# =============================================================================================================================================================================================
# 종목 세세 수집 - NAVER
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def U0001_naver_chartdata(p_code, p_count):
    import bs4
    import pandas
    import requests

    url = 'https://fchart.stock.naver.com/sise.nhn?symbol=' + str(p_code) + '&timeframe=day&count=' + str(p_count) + '&requestType=0'
    get_result = requests.get(url)
    bs_obj = bs4.BeautifulSoup(get_result.content, features='xml')

    # information
    inf = bs_obj.select('item')
    columns = ['eod_date', 'price_open' ,'price_high', 'price_low', 'price_close', 'trade_count']
    data = pandas.DataFrame([], columns = columns, index = range(len(inf)))

    for i in range(len(inf)):
        data.iloc[i] = str(inf[i]['data']).split('|')

    return data
# =============================================================================================================================================================================================

# =============================================================================================================================================================================================
# 종목 과거 내역 수집
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
def U0001_save_hist_data(p_file_name, p_equity_code, p_count, p_sleep_time=2):
    import numpy
    import time

    start_time = time.time()
    df_hist = U0001_naver_chartdata(p_equity_code, p_count)
    df_hist.to_csv(p_file_name, index=None)

    exec_time = time.time() - start_time
    U0001_logging('[' + p_equity_code + '] 종목 정보', numpy.min(df_hist['eod_date']), numpy.max(df_hist['eod_date']), df_hist.shape[0], exec_time)

    time.sleep(p_sleep_time)
    # WAI_save_db('[' + p_equity_code + '] 종목 정보', numpy.min(df_hist['eod_date']), numpy.max(df_hist['eod_date']), df_hist.shape[0], exec_time)
# =============================================================================================================================================================================================
