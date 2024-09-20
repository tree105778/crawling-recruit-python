import logging
import re
import requests
import os
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta
from math import ceil
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='scraping.log'
)

def parse_recruit_date(date_str):
    if not date_str:
        return None

    date_match = re.search(r'~?(\d{1,2}\.\d{1,2})', date_str)

    if date_match:
        month, day = date_match.group(1).split('.')
        current_year = datetime.now().year
        try:
            return datetime.strptime(f"{current_year}-{month}-{day}", "%Y-%m-%d")
        except ValueError:
            return None

    dday_match = re.search(r'D-(\d+)', date_str)
    if dday_match:
        dday = int(dday_match.group(1))
        return datetime.now() + timedelta(days=dday)

    if type(date_str) == str:
        return date_str

    return None

def fetch_site_info(pagenum):
    url = f"https://www.saramin.co.kr/zf_user/jobs/list/domestic?page={pagenum}&loc_mcd=101000&search_optional_item=n&search_done=y&panel_count=y&preview=y&isAjaxRequest=0&page_count=50&sort=RD&type=domestic&is_param=1&isSearchResultEmpty=1&isSectionHome=0&searchParamCount=1&tab=domestic"
    try :
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser")
    except requests.RequestException as e :
        logging.error(f"Error fetching page {pagenum}: {e}")
        return None

def fetch_site_info_session(pagenum, session):
    url = f"https://www.saramin.co.kr/zf_user/jobs/list/domestic?page={pagenum}&loc_mcd=101000&search_optional_item=n&search_done=y&panel_count=y&preview=y&isAjaxRequest=0&page_count=50&sort=RD&type=domestic&is_param=1&isSearchResultEmpty=1&isSectionHome=0&searchParamCount=1&tab=domestic"
    try:
        response = session.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser")

    except requests.RequestException as e:
        logging.error(f"Error fetching page {pagenum}: {e}")
        return None

def insert_with_retry(data, retries=3):
    for attempt in range(retries):
        try:
            supabase.table("seoul_recruit_info").insert(data).execute()
            break
        except Exception as e:
            logging.error(f"Error inserting data: {e}, attempt {attempt + 1}")
            if attempt == retries - 1:
                logging.error("Max retries reached. Skipping this batch.")


def get_recruit_info_by_page(pagenum, session = None):
    soup = fetch_site_info_session(pagenum, session) if session else fetch_site_info(pagenum)

    if soup is None:
        return []

    recruit_list = soup.select("div.list_item")
    page_data = []

    for recruit in recruit_list:
        corp = recruit.select_one("div.col.company_nm > :first-child")
        corp_text = corp.text.strip() if corp else '회사 정보 없음'

        info = recruit.select_one("div.job_tit a.str_tit")
        info_text = info.text if info else "채용 정보 없음"
        link = info.attrs['href'] if info else "#"

        work_place = recruit.select_one("p.work_place")
        work_place_text = work_place.text if work_place else "근무지 정보 없음"
        career = recruit.select_one("p.career")
        career_text = career.text.split("·") if career else "경력 정보 없음"

        education = recruit.select_one("p.education")
        education_text = education.text if education else "학력 정보 없음"

        date = recruit.select_one("p.support_detail > span.date")
        if date:
            recruit_date_info = parse_recruit_date(date.text.strip())
        else:
            recruit_date_info = None

        if isinstance(recruit_date_info, datetime):
            recruit_date = recruit_date_info.date().strftime("%Y-%m-%d")
        elif type(recruit_date_info) == str:
            recruit_date = recruit_date_info
        else:
            recruit_date = "날짜 정보 없음"

        data = {
            "company_name": corp_text,
            "job_info": info_text,
            "work_place": work_place_text,
            "career": career_text[0].strip(),
            "work_type": career_text[1].strip() if len(career_text) > 1 else None,
            "education": education_text,
            "recruit_date": recruit_date,
            "link": f"https://www.saramin.co.kr{link}"
        }
        page_data.append(data)

    if page_data:
        insert_with_retry(page_data)

    logging.info(f"Successfully processed page {pagenum}")
    print(f"-----------------------{pagenum}-----------------------------")
    return page_data

def fetch_total_page(page_count):
    soup = fetch_site_info(1)
    total_count = int(soup.select_one("span.total_count > em").text.replace(",", ""))
    return ceil(total_count / page_count)


total_page = fetch_total_page(50)

# all_data = []
with requests.Session() as session:
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_recruit_info_by_page, pagenum, session):
                       pagenum for pagenum in range(1, total_page + 1)}

        for future in as_completed(futures):
            pass
