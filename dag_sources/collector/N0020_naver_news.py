import pendulum
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup

from datetime import date
from dataclasses import dataclass
from sqlalchemy import create_engine, text

# ⛔️ 전역 selenium import/ webdriver_manager import 제거
# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from webdriver_manager.chrome import ChromeDriverManager

import bs4
import holidays
import numpy
import os
import pandas
import re
import time

import U0001_functions as module_UTIL
import U0002_datasets as module_DATA

KST = "Asia/Seoul"
# 🔧 경로 통일: 한 곳에서만 바꿔도 전 구간 반영
NEWS_BASE_DIR = "/opt/apollon-data/finance-data/news"
os.makedirs(NEWS_BASE_DIR, exist_ok=True)

# 작업(Task) 실행 정보를 DB(dag_job_log)에 저장할 때 사용하는 작업 구분 키.
# 추후 다른 작업(N0021, N0022, ...)을 추가하면 각 DAG 파일에서 이 값만 다르게 지정하면 된다.
JOB_ID = "N0020"


@dataclass
class PgConfig:
    host: str
    port: int
    db: str
    user: str
    password: str
    schema: str = "public"
    table: str = "F0016_master"   # 매체별로 build_pg_config() 호출 시 override


def build_pg_config(p_schema: str = "public", p_table: str = "F0016_master") -> PgConfig:
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


def parse_media_csv(csv_text: str) -> list[dict[str, str]]:
    rows = []
    for line in csv_text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 3:
            rows.append({"use_yn": parts[0], "media_code": parts[1], "media_name": ",".join(parts[2:])})
    return rows

def _to_pct_int(text: str) -> int:
    # "30%" / "60대↑" 등에서 숫자만 뽑기
    m = re.findall(r"\d+", text)
    return int(m[0]) if m else 0


def _build_date_list(p_start_date, p_end_date, p_descending: bool = True) -> list[str]:
    """
    수집 대상 날짜 구간(YYYYMMDD 문자열)을 받아 해당 구간의 날짜 리스트(YYYYMMDD)를 반환한다.
    - 특정 하루만 수집하려면 p_start_date == p_end_date 로 입력하면 된다.
    - start > end 로 입력된 경우에도 자동으로 순서를 맞춰서 처리한다.
    - p_descending=True(기본값): 최근 일자 -> 과거 일자 순으로 역순 정렬해서 반환한다.
      (최근 뉴스를 먼저 수집하고 싶을 때 사용. 오름차순이 필요하면 False 로 지정)
    """
    d_start = pendulum.from_format(str(p_start_date), "YYYYMMDD", tz=KST).date()
    d_end = pendulum.from_format(str(p_end_date), "YYYYMMDD", tz=KST).date()
    if d_end < d_start:
        d_start, d_end = d_end, d_start
    dates = pandas.date_range(start=d_start, end=d_end, freq="D")
    date_list = [d.strftime("%Y%m%d") for d in dates]
    if p_descending:
        date_list = list(reversed(date_list))
    return date_list

LIST_MEDIA_CSV = """
Y,023,조선일보
Y,025,중앙일보
Y,028,한겨레
N,353,중앙SUNDAY
N,079,노컷뉴스
N,119,데일리안
N,081,서울신문
N,082,부산일보
N,087,강원일보
N,088,매일신문
N,001,연합뉴스
N,243,이코노미스트
N,002,프레시안
N,123,조세일보
N,640,코리아중앙데일리
N,003,뉴시스
N,366,조선비즈
N,005,국민일보
N,006,미디어오늘
N,127,기자협회보
N,007,일다
N,008,머니투데이
N,009,매일경제
N,648,비즈워치
N,092,지디넷코리아
N,094,월간 산
N,011,서울경제
N,374,SBS Biz
N,014,파이낸셜뉴스
N,015,한국경제
N,016,헤럴드경제
N,654,강원도민일보
N,138,디지털데일리
N,655,CJB청주방송
N,018,이데일리
N,656,대전일보
N,657,대구MBC
N,658,국제신문
N,417,머니S
N,659,전주MBC
N,020,동아일보
N,262,신동아
N,021,문화일보
N,022,세계일보
N,660,kbc광주방송
N,661,JIBS
N,024,매경이코노미
N,145,레이디경향
N,662,농민신문
N,421,뉴스1
N,422,연합뉴스TV
N,665,더스쿠프
N,666,경기일보
N,029,디지털타임스
N,308,시사IN
N,030,전자신문
N,031,아이뉴스24
N,032,경향신문
N,033,주간경향
N,277,아시아경제
N,310,여성신문
N,036,한겨레21
N,037,주간동아
N,437,JTBC
N,044,코리아헤럴드
N,047,오마이뉴스
N,448,TV조선
N,449,채널A
N,607,뉴스타파
N,050,한경비즈니스
N,293,블로터
N,052,YTN
N,053,주간조선
N,296,코메디닷컴
N,055,SBS
N,056,KBS
N,057,MBN
N,214,MBC
N,215,한국경제TV
N,584,동아사이언스
N,586,시사저널
N,346,헬스조선
N,469,한국일보
N,629,더팩트
"""

@dag(
    dag_id="N0020_naver_news",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[module_DATA.DS_A0003],  # 달력/트리거 Dataset
    catchup=False,
    tags=["N0020_naver_news", "News"],
    params={
        # 뉴스 수집 대상 날짜 구간 (YYYYMMDD). Airflow 화면에서 "Trigger DAG w/ config" 로 직접 입력 가능.
        # - 둘 다 미입력 시: DAG 실행일(logical date) 하루만 수집한다.
        # - 특정 하루만 수집하려면 start_date/end_date 를 동일한 값으로 입력한다.
        "start_date": Param(None, type=["null", "string"],
                             description="수집 시작일자 (YYYYMMDD). 미입력 시 DAG 실행일을 사용."),
        "end_date": Param(None, type=["null", "string"],
                           description="수집 종료일자 (YYYYMMDD). 미입력 시 start_date 와 동일(=하루만 수집)."),
    },
)
def N0020_naver_news():
    list_media = parse_media_csv(LIST_MEDIA_CSV)

    @task
    def check_available(p_dummy=None) -> bool:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'check_available', 'STARTED', 'check_available is started.',
            p_context=ctx, p_start_time=t_start,
        )
        module_UTIL.U0001_logging(ctx["ds_nodash"], 'N0020', 'check_available', 'check_available is started.')
        try:
            # 🔧 통일된 경로 사용
            os.makedirs(NEWS_BASE_DIR, exist_ok=True)

            raw_params = ctx.get("params") or {}
            base_date = raw_params.get("start_date") or ctx["ds_nodash"]
            oid = '023'  # 샘플
            base_url = 'https://news.naver.com/main/list.naver'
            param = {'mode': 'LPOD','mid': 'sec','listType': 'title','oid': str(oid),'date': str(base_date),'page': '1'}
            results = module_UTIL.U0001_collect(base_url, param)
            soup = bs4.BeautifulSoup(results.content, 'html.parser')
            news_table = soup.find_all('ul')
            module_UTIL.U0001_logging(f"len(news_table)={len(news_table)}")

            module_UTIL.U0001_logging(ctx["ds_nodash"], 'N0020', 'check_available', 'check_available is finished.')

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_available', 'SUCCESS', 'check_available is finished.',
                p_section_count=len(news_table), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
            )
            return True
        except Exception as e:
            import sys
            exc_class, val, tb_ob = sys.exc_info()
            module_UTIL.U0001_logging(tb_ob.tb_lineno, val)
            module_UTIL.U0001_logging(ctx["ds_nodash"], 'N0020', 'check_available', f'ERROR: {e}')

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'check_available', 'FAILED', f'check_available failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"error": str(e), "error_type": type(e).__name__},
            )
            # 🔧 원본 동작 유지: check_available 은 short_circuit 태스크가 아니라 일반 @task 이며,
            # 원래부터 실패 시 예외를 올리지 않고 False 를 반환한다. 로깅만 추가하고 이 동작은 그대로 둔다.
            return False

    @task
    def collect_news_list(p_code=None, p_file_full_path_title: str = "", p_dummy=None) -> pandas.DataFrame:
        ctx = get_current_context()
        t_start = pendulum.now(KST)

        # 🔧 날짜 구간 입력: Airflow "Trigger DAG w/ config" 의 params.start_date/end_date 로 지정.
        # 둘 다 미입력이면 DAG 실행일(logical date) 하루만 수집한다.
        # 특정 하루만 수집하려면 start_date == end_date 로 입력하면 된다.
        raw_params = ctx.get("params") or {}
        p_start_date = raw_params.get("start_date") or ctx["ds_nodash"]
        p_end_date = raw_params.get("end_date") or p_start_date
        # 🔧 수집 순서: 최근 일자 -> 과거 일자 역순으로 수집한다 (p_descending=True, 기본값).
        date_list = _build_date_list(p_start_date, p_end_date, p_descending=True)

        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_news_list', 'STARTED',
            f'collect_news_list is started. (media={p_code}, start={p_start_date}, end={p_end_date}, '
            f'days={len(date_list)}, order=desc(recent-first))',
            p_context=ctx, p_start_time=t_start,
            p_extra={"media_code": p_code, "p_start_date": p_start_date, "p_end_date": p_end_date,
                     "date_list": date_list, "order": "desc(recent-first)"},
        )
        try:
            list_news = list()
            for p_date in date_list:
                for page in range(1, 20):
                    base_url = 'https://news.naver.com/main/list.naver'
                    param = {'mode': 'LPOD','mid': 'sec','listType': 'title','oid': str(p_code),'date': str(p_date),'page': str(page)}
                    results = module_UTIL.U0001_collect(base_url, param)
                    soup = bs4.BeautifulSoup(results.content, 'html.parser')
                    news_table = soup.find_all('ul')

                    for tag_ul in news_table:
                        if 'https://n.news.naver.com/' not in str(tag_ul):
                            continue
                        for a_html in str(tag_ul).split('</a>'):
                            if '<a' not in a_html:
                                continue
                            tag_a = '<a' + a_html.split('<a')[1] + '</a>'
                            html_a = bs4.BeautifulSoup(tag_a, 'html.parser')
                            a_tag = html_a.find('a')
                            if not a_tag or not a_tag.get('href'):
                                continue

                            article_link = a_tag['href']
                            article_title = html_a.get_text(strip=True)

                            list_news.append({
                                'news_date': p_date,
                                'news_code': p_code,
                                'news_url': article_link,
                                'news_title': article_title,
                                'news_collect_yn': 'N',
                            })
                    # 🔧 서버 부하 방지를 위한 요청 간 유휴시간 (원본 유지)
                    time.sleep(0.5)

            df_list = pandas.DataFrame(list_news)
            # 🔧 제목 parquet 저장
            if p_file_full_path_title:
                os.makedirs(os.path.dirname(p_file_full_path_title), exist_ok=True)
                df_list.to_parquet(p_file_full_path_title, index=False, compression='gzip')

            module_UTIL.U0001_logging(
                f"[{p_code}] 수집대상 뉴스 건수={len(list_news)} (기간: {p_start_date}~{p_end_date}, 저장: {p_file_full_path_title})"
            )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_news_list', 'SUCCESS', 'collect_news_list is finished.',
                p_section_count=len(list_news), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"media_code": p_code, "p_start_date": p_start_date, "p_end_date": p_end_date,
                         "date_list": date_list, "file_path": p_file_full_path_title},
            )
            return df_list
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_news_list', 'FAILED', f'collect_news_list failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"media_code": p_code, "p_start_date": p_start_date, "p_end_date": p_end_date,
                         "error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task
    def collect_news_contents(
        df_links: pandas.DataFrame,
        p_file_full_path_title: str,
        p_file_full_path_contents: str,
        media_code: str,
        media_name: str,
        p_dummy=None
    ) -> pandas.DataFrame:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_news_contents', 'STARTED', f'collect_news_contents is started. (media={media_code})',
            p_context=ctx, p_start_time=t_start,
            p_extra={"media_code": media_code, "media_name": media_name},
        )
        try:
            # 🔧 제목 parquet 존재 여부 검사 (필터링 없이 URL 전체 세트로 사용)
            if os.path.exists(p_file_full_path_title):
                df_news_titles = pandas.read_parquet(p_file_full_path_title)
                set_existing_titles = set(df_news_titles.get('news_url', pandas.Series(dtype=str)).tolist())
            else:
                set_existing_titles = set()
                module_UTIL.U0001_logging(f"[{media_code}] 기존 목록 파일 없음: {p_file_full_path_title}")

            if os.path.exists(p_file_full_path_contents):
                df_news_contents = pandas.read_parquet(p_file_full_path_contents)
                set_existing_contents = set(df_news_contents.get('article_link', pandas.Series(dtype=str)).tolist())
                module_UTIL.U0001_logging(f"[{media_code}] 기존 본문 {len(set_existing_contents)}개 URL 확인")
            else:
                df_news_contents = pandas.DataFrame()
                set_existing_contents = set()
                module_UTIL.U0001_logging(f"[{media_code}] 기존 본문 파일 없음: {p_file_full_path_contents}")

            list_news = []
            for _, r in df_links.iterrows():
                # 이미 수집된 목록/본문은 스킵
                if r['news_url'] in set_existing_titles:
                    continue
                if r['news_url'] in set_existing_contents:
                    continue

                try:
                    contents = module_UTIL.U0001_collect(r['news_url'], {})
                    contents_html = bs4.BeautifulSoup(contents.content, 'html.parser')

                    try:
                        news_section = contents_html.find('em', {'class': 'media_end_categorize_item'}).get_text(strip=True)
                    except Exception:
                        news_section = '미분류'
                    try:
                        news_author = contents_html.find('em', {'class': 'media_end_head_journalist_name'}).get_text(strip=True)
                    except Exception:
                        try:
                            news_author = contents_html.find('span', {'class': 'byline_s'}).get_text(strip=True)
                        except Exception:
                            news_author = '미지정'
                    try:
                        news_timestamp = contents_html.find('span', {'class': 'media_end_head_info_datestamp_time'}).get_text(strip=True)
                    except Exception:
                        news_timestamp = ''
                    try:
                        art = contents_html.find('article', {'id': 'dic_area'})
                        for br in art.find_all('br'):
                            br.replace_with('\n')
                        news_article = art.get_text('\n', strip=False)
                    except Exception:
                        news_article = ''
                    try:
                        news_comments_link = ''
                        s = str(contents_html)
                        if '/mnews/article/comment' in s:
                            news_comments_link = 'https://n.news.naver.com/mnews/article/comment' + s.split('/mnews/article/comment')[1].split('"')[0]
                    except Exception:
                        news_comments_link = ''

                    list_news.append({
                        'media_code': media_code,
                        'media_name': media_name,
                        'article_link': r['news_url'],
                        'article_title': r['news_title'],
                        'article_section': news_section,
                        'article_author': news_author,
                        'article_time': news_timestamp,
                        'article_body': news_article,
                        'article_comments_link': news_comments_link,
                        'news_collect_yn': r.get('news_collect_yn', 'N'),
                    })
                except Exception as e:
                    module_UTIL.U0001_logging(f"[{media_code}] 본문 수집 실패: {r['news_url']} ({e})")
                # 🔧 서버 부하 방지를 위한 요청 간 유휴시간 (원본 유지)
                time.sleep(0.2)
                # 🔧 break 삭제 (모든 기사 수집)

            df_collected = pandas.DataFrame(list_news)
            if not df_collected.empty:
                df_news_contents = pandas.concat([df_news_contents, df_collected], ignore_index=True)
                df_news_contents.drop_duplicates(subset=['article_link'], keep='last', inplace=True)

            os.makedirs(os.path.dirname(p_file_full_path_contents), exist_ok=True)
            df_news_contents.to_parquet(p_file_full_path_contents, index=False, compression='gzip')
            module_UTIL.U0001_logging(f"[{media_code}] 신규 본문 수집={len(list_news)} (누적 저장: {p_file_full_path_contents})")

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_news_contents', 'SUCCESS', 'collect_news_contents is finished.',
                p_section_count=len(list_news), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"media_code": media_code, "media_name": media_name, "file_path": p_file_full_path_contents},
            )
            return df_news_contents
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_news_contents', 'FAILED', f'collect_news_contents failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"media_code": media_code, "media_name": media_name, "error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task(retries=2, retry_delay=pendulum.duration(minutes=2))
    def collect_news_comments(
        df_links: pandas.DataFrame,
        p_file_full_path_comments: str,
        media_code: str,
        media_name: str,
        p_dummy=None
    ) -> pandas.DataFrame:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_news_comments', 'STARTED', f'collect_news_comments is started. (media={media_code})',
            p_context=ctx, p_start_time=t_start,
            p_extra={"media_code": media_code, "media_name": media_name},
        )
        try:
            if os.path.exists(p_file_full_path_comments):
                df_news_comments = pandas.read_parquet(p_file_full_path_comments)
                set_existing_urls = set(df_news_comments.get('article_comments_link', pandas.Series(dtype=str)).tolist())
                module_UTIL.U0001_logging(f"[{media_code}] 기존 댓글 {len(set_existing_urls)}개 URL 확인")
            else:
                df_news_comments = pandas.DataFrame()
                set_existing_urls = set()
                module_UTIL.U0001_logging(f"[{media_code}] 기존 댓글 파일 없음: {p_file_full_path_comments}")

            # 셀레니움 옵션 (가능하면 이미지/폰트 로딩 최소화)
            def _build_driver():
                # 🔸 로컬 임포트 (DAG 파싱 시 ImportError 방지)
                from selenium import webdriver
                from selenium.webdriver.common.by import By  # noqa: F401 (외부에서 사용)
                from selenium.webdriver.chrome.service import Service
                from selenium.webdriver.chrome.options import Options
                from selenium.webdriver.support.ui import WebDriverWait  # noqa: F401
                from selenium.webdriver.support import expected_conditions as EC  # noqa: F401
                import os as _os

                options = Options()
                options.add_argument("--headless=new")
                options.add_argument("--disable-gpu")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--window-size=1280,2400")
                options.add_argument("--lang=ko-KR")
                options.add_argument(
                    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
                )
                options.page_load_strategy = "eager"

                # (선택) 명시적 바이너리 지정
                chrome_bin = _os.environ.get("CHROME_BIN", "/usr/bin/chromium")
                if _os.path.exists(chrome_bin):
                    options.binary_location = chrome_bin

                # 1차: Selenium Manager 자동 탐지
                try:
                    return webdriver.Chrome(options=options)
                except Exception as e1:
                    # 2차: 시스템 chromedriver 직접 지정
                    try:
                        return webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=options)
                    except Exception as e2:
                        raise RuntimeError(
                            f"Chrome 실행 실패: Selenium Manager 실패: {e1}; /usr/bin/chromedriver 실패: {e2}"
                        )

            # 외부에서 참조할 수 있게 로컬 심볼 바인딩
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            all_rows = []

            for _, link_row in df_links.iterrows():
                # 🔧 저장 여부 스킵
                article_comments_link = link_row.get('article_comments_link', '')
                if not article_comments_link or article_comments_link in set_existing_urls:
                    continue

                driver = _build_driver()
                wait = WebDriverWait(driver, 15)
                try:
                    driver.get(article_comments_link)

                    # 🔧 팝업/동의 버튼 (XPath 사용)
                    try:
                        agree_btns = driver.find_elements(By.XPATH, "//button[contains(., '동의')]")
                        if agree_btns:
                            agree_btns[0].click()
                            time.sleep(0.3)
                    except Exception:
                        pass

                    # 댓글 영역 대기
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "ul.u_cbox_list")))

                    # 🔧 선택자 확인 (.u_cbox_chart_cont)
                    _ = driver.find_elements(By.CSS_SELECTOR, ".u_cbox_chart_cont")

                    # 성별
                    try:
                        male_text = driver.find_element(By.CSS_SELECTOR, ".u_cbox_chart_sex .u_cbox_chart_male .u_cbox_chart_per").text
                        female_text = driver.find_element(By.CSS_SELECTOR, ".u_cbox_chart_sex .u_cbox_chart_female .u_cbox_chart_per").text
                        male = _to_pct_int(male_text); female = _to_pct_int(female_text)
                    except Exception:
                        male = 0; female = 0

                    # 연령대
                    age_pct = {}
                    for prog in driver.find_elements(By.CSS_SELECTOR, ".u_cbox_chart_age .u_cbox_chart_progress"):
                        try:
                            lbl = prog.find_element(By.CSS_SELECTOR, ".u_cbox_chart_cnt span").text.strip().replace("↑", "")
                            try:
                                per_text = prog.find_element(By.CSS_SELECTOR, ".u_cbox_chart_per").text
                            except Exception:
                                height = prog.find_element(By.CSS_SELECTOR, ".u_cbox_chart_progress_in").get_attribute("style")
                                per_text = "".join(re.findall(r"\d+", height))
                            age_pct[lbl] = _to_pct_int(per_text)
                        except Exception:
                            continue

                    # 더보기 반복
                    def _comment_count():
                        return len(driver.find_elements(By.CSS_SELECTOR, "li.u_cbox_comment"))

                    max_clicks, clicks = 50, 0
                    last_count = _comment_count()
                    while clicks < max_clicks:
                        # 여러 버튼 유형 시도
                        more_buttons = driver.find_elements(
                            By.XPATH,
                            "//a[contains(@class,'u_cbox_btn_more') or contains(@class,'u_cbox_btn_viewmore') or contains(@class,'u_cbox_more_wrap')] | "
                            "//button[contains(.,'더보기')]"
                        )
                        clicked = False
                        for btn in more_buttons:
                            try:
                                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                                time.sleep(0.3)
                                if btn.is_displayed() and btn.is_enabled():
                                    btn.click()
                                    clicked = True; clicks += 1
                                    for _ in range(24):
                                        time.sleep(0.25)
                                        cur = _comment_count()
                                        if cur > last_count:
                                            last_count = cur
                                            break
                                    break
                            except Exception:
                                continue
                        if not clicked:
                            break

                    # 댓글 파싱
                    items = driver.find_elements(By.CSS_SELECTOR, "li.u_cbox_comment")

                    for li in items:
                        try:
                            contents = li.find_element(By.CSS_SELECTOR, ".u_cbox_contents").text.strip()
                        except Exception:
                            contents = ""
                        if not contents:
                            continue

                        try: nick = li.find_element(By.CSS_SELECTOR, ".u_cbox_nick").text.strip()
                        except Exception: nick = ""
                        try: up = li.find_element(By.CSS_SELECTOR, ".u_cbox_cnt_recomm").text.strip()
                        except Exception: up = "0"
                        try: down = li.find_element(By.CSS_SELECTOR, ".u_cbox_cnt_unrecomm").text.strip()
                        except Exception: down = "0"
                        try: reg_dt = li.find_element(By.CSS_SELECTOR, ".u_cbox_date").text.strip()
                        except Exception: reg_dt = ""

                        # 🔧 기사행(link_row) 사용, 변수 충돌 제거
                        all_rows.append({
                            'media_code': media_code,
                            'media_name': media_name,
                            'article_link': link_row['article_link'] if 'article_link' in link_row else link_row.get('news_url',''),
                            'article_comments_link': article_comments_link,
                            'gender_m': male, 'gender_f': female,
                            'age_10': age_pct.get("10대", 0),
                            'age_20': age_pct.get("20대", 0),
                            'age_30': age_pct.get("30대", 0),
                            'age_40': age_pct.get("40대", 0),
                            'age_50': age_pct.get("50대", 0),
                            'age_60': age_pct.get("60대", 0),
                            'nick': nick,
                            'contents': contents,
                            'sympathyCount': up,
                            'antipathyCount': down,
                            'regTime': reg_dt,
                        })

                    module_UTIL.U0001_logging(f"Collected {len(items)} comments (clicks={clicks}).")

                finally:
                    driver.quit()

            if all_rows:
                df_new = pandas.DataFrame(all_rows)
                df_news_comments = pandas.concat([df_news_comments, df_new], ignore_index=True)
                # 🔧 댓글은 중복 가능성이 높음 → 더 강한 키로 중복 제거
                df_news_comments.drop_duplicates(
                    subset=['article_comments_link','nick','contents','regTime'],
                    keep='last', inplace=True
                )
                os.makedirs(os.path.dirname(p_file_full_path_comments), exist_ok=True)
                df_news_comments.to_parquet(p_file_full_path_comments, index=False, compression='gzip')
                module_UTIL.U0001_logging("Saved -> " + p_file_full_path_comments)
            else:
                module_UTIL.U0001_logging("댓글을 찾지 못했습니다. (로그인 요구/정책 변경 가능)")

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_news_comments', 'SUCCESS', 'collect_news_comments is finished.',
                p_section_count=len(all_rows), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"media_code": media_code, "media_name": media_name, "file_path": p_file_full_path_comments},
            )
            return df_news_comments
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_news_comments', 'FAILED', f'collect_news_comments failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"media_code": media_code, "media_name": media_name, "error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task
    def collect_news_finalize(
        df_titles: pandas.DataFrame,
        df_contents: pandas.DataFrame,
        df_comments: pandas.DataFrame,
        media_code: str,
        media_name: str,
        p_dummy=None
    ) -> pandas.DataFrame:
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'collect_news_finalize', 'STARTED', f'collect_news_finalize is started. (media={media_code})',
            p_context=ctx, p_start_time=t_start,
            p_extra={"media_code": media_code, "media_name": media_name},
        )
        try:
            out = df_contents.copy()
            if 'comment_collect_yn' not in out.columns:
                out['comment_collect_yn'] = 'N'
            if not df_comments.empty and 'article_link' in df_comments.columns:
                have_comments = set(df_comments['article_link'].dropna().tolist())
                out.loc[out['article_link'].isin(have_comments), 'comment_collect_yn'] = 'Y'

            article_count = int(out.shape[0])
            comment_count = int(df_comments.shape[0]) if df_comments is not None else 0

            # 이번 실행에서 실제로 수집된 뉴스 날짜 구간(YYYYMMDD -> int) 산출
            # (요청한 start_date~end_date 구간 중 실제 기사가 존재했던 날짜 기준)
            eod_date_min = None
            eod_date_max = None
            if 'news_date' in out.columns and not out['news_date'].dropna().empty:
                news_dates_int = pandas.to_numeric(out['news_date'], errors='coerce').dropna().astype(int)
                if not news_dates_int.empty:
                    eod_date_min = int(news_dates_int.min())
                    eod_date_max = int(news_dates_int.max())

            # 매체별 수집 결과를 모든 DAG 공통 포맷(dag_collect_result)에 저장 (Superset 등 BI 조회용)
            module_UTIL.U0001_db_save_collect_result(
                p_job_id=JOB_ID,
                p_category='news',
                p_item_code=media_code,
                p_item_name=media_name,
                p_row_count=article_count,
                p_eod_date_min=eod_date_min,
                p_eod_date_max=eod_date_max,
                p_message=f"article={article_count}, comment={comment_count}",
                p_context=ctx,
                p_extra={
                    "media_code": media_code, "media_name": media_name,
                    "article_count": article_count, "comment_count": comment_count,
                },
            )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_news_finalize', 'SUCCESS', 'collect_news_finalize is finished.',
                p_section_count=article_count, p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"media_code": media_code, "media_name": media_name, "comment_count": comment_count},
            )
            return out
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'collect_news_finalize', 'FAILED', f'collect_news_finalize failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"media_code": media_code, "media_name": media_name, "error": str(e), "error_type": type(e).__name__},
            )
            raise

    @task
    def save_data_to_db(df: pandas.DataFrame, cfg: PgConfig, media_code: str, media_name: str, if_exists: str = "replace") -> bool:
        """
        DataFrame을 PostgreSQL에 저장
        """
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'save_data_to_db', 'STARTED', f'save_data_to_db is started. (media={media_code})',
            p_context=ctx, p_start_time=t_start,
            p_extra={
                "media_code": media_code, "media_name": media_name,
                "schema": cfg.schema, "table": cfg.table, "if_exists": if_exists,
                "host": cfg.host, "db": cfg.db,
            },
        )
        try:
            url = f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.db}"
            engine = create_engine(url)

            with engine.begin() as conn:
                if cfg.schema and cfg.schema != "public":
                    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{cfg.schema}";'))

                df.to_sql(
                    name=cfg.table,
                    con=conn,
                    schema=cfg.schema,
                    if_exists=if_exists,
                    index=False,          # 인덱스 컬럼 저장 방지
                    method="multi",
                    chunksize=2000,
                )

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'save_data_to_db', 'SUCCESS', 'save_data_to_db is finished.',
                p_section_count=int(df.shape[0]), p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"media_code": media_code, "media_name": media_name, "schema": cfg.schema, "table": cfg.table, "if_exists": if_exists},
            )
            return True
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'save_data_to_db', 'FAILED', f'save_data_to_db failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={
                    "media_code": media_code, "media_name": media_name,
                    "schema": cfg.schema, "table": cfg.table, "if_exists": if_exists,
                    "error": str(e), "error_type": type(e).__name__,
                },
            )
            raise

    @task(outlets=[module_DATA.DS_N0020])
    def task_to_close(p_dummy):
        ctx = get_current_context()
        t_start = pendulum.now(KST)
        module_UTIL.U0001_db_logging(
            JOB_ID, 'task_to_close', 'STARTED', f'task_to_close is started. (p_dummy={p_dummy})',
            p_context=ctx, p_start_time=t_start,
            p_extra={"p_dummy": str(p_dummy)},
        )
        try:
            module_UTIL.U0001_logging(f"CLOSE: {p_dummy}")

            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'task_to_close', 'SUCCESS', 'task_to_close is finished.',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"p_dummy": str(p_dummy)},
            )
        except Exception as e:
            t_end = pendulum.now(KST)
            module_UTIL.U0001_db_logging(
                JOB_ID, 'task_to_close', 'FAILED', f'task_to_close failed: {e}',
                p_context=ctx, p_start_time=t_start, p_end_time=t_end,
                p_extra={"p_dummy": str(p_dummy), "error": str(e), "error_type": type(e).__name__},
            )
            raise

    # -------- 파이프라인 구성 --------
    flag_available = check_available(module_DATA.DS_A0003)

    group_closers = []
    for l_media in list_media:
        if l_media['use_yn'] != 'Y':
            continue

        media_code = l_media['media_code']
        media_name = l_media['media_name']

        file_full_path_title    = os.path.join(NEWS_BASE_DIR, f"NEWS0_{media_code}_title.gzip")
        file_full_path_contents = os.path.join(NEWS_BASE_DIR, f"NEWS0_{media_code}_contents.gzip")
        file_full_path_comments = os.path.join(NEWS_BASE_DIR, f"NEWS0_{media_code}_comments.gzip")

        # 한글 테이블명 이슈를 피하기 위해 code 기반으로 저장(원하시면 media_name으로 변경하세요)
        # DB 접속 정보는 하드코딩하지 않고 Airflow Variable(U0001_get_pg_conn_info()) 에서 조회한다.
        cfg = build_pg_config(p_schema="public", p_table=f"F0016_{media_code}")

        with TaskGroup(group_id=f"G_{media_code}", tooltip=f"{media_name} 수집 파이프라인") as grp:
            t_list = collect_news_list.override(task_id=f"list_{media_code}")(
                p_code=media_code,
                p_file_full_path_title=file_full_path_title,
                p_dummy=flag_available
            )

            t_contents = collect_news_contents.override(task_id=f"contents_{media_code}")(
                t_list, file_full_path_title, file_full_path_contents, media_code, media_name, flag_available
            )

            t_comments = collect_news_comments.override(task_id=f"comments_{media_code}")(
                t_contents, file_full_path_comments, media_code, media_name, flag_available
            )

            # 파이널라이즈는 반드시 "호출"해서 태스크 생성
            t_finalize = collect_news_finalize.override(task_id=f"finalize_{media_code}")(
                t_list, t_contents, t_comments, media_code, media_name, flag_available
            )

            t_save = save_data_to_db.override(task_id=f"save_{media_code}")(
                df=t_finalize, cfg=cfg, media_code=media_code, media_name=media_name, if_exists="replace"
            )

            t_close = task_to_close.override(task_id=f"close_{media_code}")(
                p_dummy=media_code
            )

            # 의존성
            t_list >> t_contents >> t_comments >> t_finalize >> t_save >> t_close

        flag_available >> grp
        group_closers.append(t_close)

    close_all = task_to_close.override(task_id="close_all")(p_dummy="ALL_DONE")
    for c in group_closers:
        c >> close_all


N0020_naver_news()
