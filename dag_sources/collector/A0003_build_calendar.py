import pendulum
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.python import get_current_context

from datetime import date
from dataclasses import dataclass
from sqlalchemy import create_engine, text

import holidays
import numpy
import os
import pandas
import time

import U0001_functions as module_UTIL
import U0002_datasets as module_DATA

KST = "Asia/Seoul"

# 작업(Task) 실행 정보를 DB(dag_job_log)에 저장할 때 사용하는 작업 구분 키.
# 추후 다른 작업(A0004, A0005, ...)을 추가하면 각 DAG 파일에서 이 값만 다르게 지정하면 된다.
JOB_ID = "A0003"


@dataclass
class PgConfig:
    host: str
    port: int
    db: str
    user: str
    password: str
    schema: str = "public"
    table: str = "m_calendar"   # 원하는 테이블명으로 변경


def build_pg_config(p_schema: str = "public", p_table: str = "m_calendar") -> PgConfig:
    """
    PostgreSQL 접속 정보를 Airflow Variable 로부터 조회하여 PgConfig 를 생성한다.
    (DB 접속정보는 소스코드에 하드코딩하지 않고 U0001_functions.U0001_get_pg_conn_info() 를 통해 조회)
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


@dag(
    dag_id="A0003_build_calendar",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule="55 18 * * 1-5",   # 월(1)~금(5) 18:45 KST
    catchup=False,
    tags=["A0003_build_calendar", "Calendar"],
)
def A0003_build_calendar():

    @task
    def get_calendar(p_start_date: str | date, p_end_date: str | date, p_base_country: str = "KR") -> pandas.DataFrame:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'get_calendar', 'STARTED', 'get_calendar is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"p_start_date": str(p_start_date), "p_end_date": str(p_end_date), "p_base_country": p_base_country},
        )
        """
        공휴일 포함 기준 달력 데이터 생성
        """
        try:
            dates = pandas.date_range(start=p_start_date, end=p_end_date, freq="D")
            if len(dates) == 0:
                df_empty = pandas.DataFrame(columns=["eod_date", "weekday", "yyyy", "mm", "dd", "off_yn", "yyyyww"])
                t_end = pendulum.now(KST)
                module_UTIL.U0001_db_logging(
                    JOB_ID, 'get_calendar', 'SUCCESS', 'get_calendar is finished. (empty date range)',
                    p_section_count=0, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                )
                return df_empty

            # 공휴일(해당 연도들만 생성)
            years = sorted(set(dates.year.tolist()))
            hol = getattr(holidays, p_base_country)(years=years)  # 예: holidays.KR(years=[...])
            holiday_set = set(hol.keys())  # datetime.date 객체 집합

            # vectorized 파생값 계산
            # pandas dayofweek: Mon=0..Sun=6  ->  Sun=1, Mon=2, ..., Sat=7 로 변환
            dow = dates.dayofweek  # 0..6
            weekday = ((dow + 1) % 7) + 1

            # 주말 여부 (토=5, 일=6)
            is_weekend = dow >= 5

            # 공휴일 여부 (Series -> date로 변환하여 set membership)
            date_as_pydate = dates.date  # numpy array of datetime.date
            is_holiday = pandas.Series(date_as_pydate).isin(holiday_set).to_numpy()

            off = (is_weekend | is_holiday)
            off_yn = pandas.Series(["Y" if x else "N" for x in off])

            # yyyyww: %W(월요일 시작, 첫 주=0) + 1 → 첫 주를 1부터 시작
            week_mon0 = dates.strftime("%W").astype(int)
            yyyyww = (week_mon0 + 1).astype(int)

            # 기본 컬럼들
            df = pandas.DataFrame({
                "eod_date": dates.strftime("%Y%m%d").astype(int),
                "weekday": weekday.astype(int),
                "yyyy": dates.year.astype(int),
                "mm": dates.month.astype(int),
                "dd": dates.day.astype(int),
                "off_yn": off_yn,
                "yyyyww": yyyyww,
            })

            # 정렬 및 인덱스 초기화
            df = df.sort_values("eod_date")
            df = df.set_index(['eod_date'])
            module_UTIL.U0001_logging(df)

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'get_calendar', 'SUCCESS', 'get_calendar is finished.',
                p_section_count=df.shape[0], p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"years": years, "base_country": p_base_country, "holiday_count": len(holiday_set)},
            )
            return df
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'get_calendar', 'FAILED', f'get_calendar failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task(outlets=[module_DATA.DS_A0003])
    def save_data_to_db(df: pandas.DataFrame, cfg: PgConfig, if_exists: str = "replace") -> None:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'save_data_to_db', 'STARTED', 'save_data_to_db is started.',
            p_context=ctx, p_start_time=t_start,
            p_extra={"schema": cfg.schema, "table": cfg.table, "if_exists": if_exists, "host": cfg.host, "db": cfg.db},
        )
        """
        DataFrame을 PostgreSQL에 저장. 기본은 테이블 교체(replace).
        """
        try:
            url = f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.db}"
            engine = create_engine(url)

            with engine.begin() as conn:
                # 스키마가 기본 public이 아니라면 미리 생성해둠 (없어도 오류 없이 진행)
                if cfg.schema and cfg.schema != "public":
                    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{cfg.schema}";'))

                df.to_sql(
                    name=cfg.table,
                    con=conn,
                    schema=cfg.schema,
                    if_exists=if_exists,  # 'replace' | 'append' | 'fail'
                    index=True,
                    index_label="eod_date",
                    method="multi",
                    chunksize=2000,
                )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'save_data_to_db', 'SUCCESS', 'save_data_to_db is finished.',
                p_section_count=df.shape[0], p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"schema": cfg.schema, "table": cfg.table, "if_exists": if_exists},
            )
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'save_data_to_db', 'FAILED', f'save_data_to_db failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"schema": cfg.schema, "table": cfg.table, "if_exists": if_exists,
                         "error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task
    def verify_calendar_data(cfg: PgConfig) -> dict:
        """
        저장 결과 모니터링용 검증 Task.
        m_calendar 테이블에 대해 아래 SQL을 수행하고, 결과를 Airflow Task 로그(화면)에 출력한다.

            SELECT MIN(eod_date) AS min_eod_date, MAX(eod_date) AS max_eod_date, COUNT(*) AS cnt
            FROM m_calendar;
        """
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'verify_calendar_data', 'STARTED', 'verify_calendar_data is started.',
            p_context=ctx, p_start_time=t_start,
        )
        try:
            sql = text(
                f'SELECT MIN(eod_date) AS min_eod_date, MAX(eod_date) AS max_eod_date, COUNT(*) AS cnt '
                f'FROM "{cfg.schema}"."{cfg.table}"'
            )
            url = f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.db}"
            engine = create_engine(url)
            with engine.begin() as conn:
                row = conn.execute(sql).mappings().first()
            result = dict(row) if row else {"min_eod_date": None, "max_eod_date": None, "cnt": 0}

            # Airflow 화면(Task 로그)에서 바로 확인할 수 있도록 출력
            module_UTIL.U0001_logging(
                f"[{JOB_ID}][verify_calendar_data] min_eod_date={result.get('min_eod_date')}, "
                f"max_eod_date={result.get('max_eod_date')}, cnt={result.get('cnt')}"
            )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'verify_calendar_data', 'SUCCESS', 'verify_calendar_data is finished.',
                p_section_count=int(result.get('cnt') or 0),
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"schema": cfg.schema, "table": cfg.table, "result": {k: str(v) for k, v in result.items()}},
            )
            return result
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'verify_calendar_data', 'FAILED', f'verify_calendar_data failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"schema": cfg.schema, "table": cfg.table, "error": str(e), "error_type": type(e).__name__},
            )
            raise

    # PostgreSQL 저장 설정 (접속정보는 Airflow Variable 에서 조회)
    cfg = build_pg_config(p_schema="public", p_table="m_calendar")

    # 파이프라인
    start_date = "1999-01-01"
    end_date = pendulum.now(KST).to_date_string()
    df_calendar = get_calendar(start_date, end_date)
    # 실제 저장
    task_save = save_data_to_db(df_calendar, cfg, if_exists="replace")
    # 저장 완료 후 결과 검증(모니터링) - Airflow 화면(Task 로그)에 결과 출력
    task_verify = verify_calendar_data(cfg)
    task_save >> task_verify

A0003_build_calendar()
