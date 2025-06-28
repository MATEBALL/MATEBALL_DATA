import mysql.connector
import yaml
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

def load_db_config(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
        return config.get("database")

def get_connection(config):
    try:
        return mysql.connector.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )
    except mysql.connector.Error as e:
        print(f"DB 연결 오류: {e}")
        return None

STADIUM_MAP = {
    "잠실": "잠실구장",
    "문학": "문학구장",
    "고척": "고척돔",
    "사직": "사직구장",
    "창원": "창원몰",
    "수원": "수원몰",
    "대전(신)": "신구장",
    "대구": "대주장",
    "광주": "광주경기장"
}

def parse_game_row(row, game_date):
    try:
        time_td = row.find("td", class_="time")
        if not time_td:
            return None
        time_str = time_td.text.strip()
        if ":" not in time_str:
            return None
        game_time = datetime.strptime(time_str, "%H:%M").time()

        play_td = row.find("td", class_="play")
        if not play_td:
            return None
        team_spans = play_td.find_all("span")
        if len(team_spans) < 2:
            return None
        away_team = team_spans[0].text.strip()
        home_team = team_spans[-1].text.strip()

        cols = row.find_all("td")
        valid_stadiums = set(STADIUM_MAP.keys())
        stadium_raw = None
        for td in reversed(cols):
            text = td.text.strip()
            if text in valid_stadiums:
                stadium_raw = text
                break

        if not stadium_raw:
            return None

        stadium = STADIUM_MAP.get(stadium_raw, stadium_raw)

        return {
            "game_date": game_date,
            "game_time": game_time,
            "away_team_name": away_team,
            "home_team_name": home_team,
            "stadium_name": stadium
        }

    except Exception as e:
        print(f"행 파싱 중 오류 발생: {e}")
        return None

def insert_game_info(conn, game_info):
    try:
        sql = """
            INSERT INTO game (home_team_name, away_team_name, game_date, game_time, stadium_name)
            VALUES (%s, %s, %s, %s, %s)
        """
        values = (
            game_info["home_team_name"],
            game_info["away_team_name"],
            game_info["game_date"],
            game_info["game_time"],
            game_info["stadium_name"]
        )
        with conn.cursor() as cursor:
            cursor.execute(sql, values)
    except Exception as e:
        print(f"DB 저장 오류: {e}")

def click_next_month(driver):
    next_btn = driver.find_element(By.ID, "btnNext")
    driver.execute_script("arguments[0].click();", next_btn)
    time.sleep(1.5)

def get_current_year_month(driver):
    year = Select(driver.find_element(By.ID, "ddlYear")).first_selected_option.text.strip()
    month = Select(driver.find_element(By.ID, "ddlMonth")).first_selected_option.text.strip()
    return int(year), int(month)

def crawl_games(conn):
    options = Options()
    options.add_argument("--headless=new")
    driver = webdriver.Chrome(options=options)

    try:
        driver.get("https://www.koreabaseball.com/Schedule/Schedule.aspx")
        Select(driver.find_element(By.ID, "ddlYear")).select_by_value("2025")
        time.sleep(1.5)

        target_months = [6, 7, 8]
        for target_month in target_months:
            while True:
                current_year, current_month = get_current_year_month(driver)
                if current_year == 2025 and current_month == target_month:
                    break
                click_next_month(driver)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            rows = soup.select("table#tblScheduleList tbody tr")
            current_date = None

            for row in rows:
                day_td = row.find("td", class_="day")
                if day_td:
                    day_text = day_td.text.strip()[:5]
                    current_date = datetime.strptime(f"2025.{day_text}", "%Y.%m.%d").date()

                game_info = parse_game_row(row, current_date)
                if game_info:
                    insert_game_info(conn, game_info)

        conn.commit()
    finally:
        driver.quit()

if __name__ == "__main__":
    db_config_path = "C:\\mateball_crawling\\MATEBALL_DATA\\db_config.yaml"
    db_config = load_db_config(db_config_path)
    conn = get_connection(db_config)
    if conn:
        crawl_games(conn)
        conn.close()