import pandas as pd
import pymysql
import yaml
from pathlib import Path
from glob import glob

def load_db_config(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f).get("database")

def get_connection(cfg):
    return pymysql.connect(
        host=cfg["host"],
        user=cfg["user"],
        password=cfg["password"],
        db=cfg["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

def read_chatting_csv(csv_path: str) -> pd.DataFrame:
    return pd.read_csv(csv_path)

def insert_urls(conn, urls, batch_size=1000):
    if not urls:
        return
    sql = "INSERT IGNORE INTO chatting (chatting_url) VALUES (%s)"
    with conn.cursor() as cur:
        for i in range(0, len(urls), batch_size):
            cur.executemany(sql, [(u,) for u in urls[i:i+batch_size]])

def files_to_process(path_str: str):
    p = Path(path_str)
    if p.is_file():
        return [str(p)]
    if p.is_dir():
        return sorted(glob(str(p / "*.csv")))
    return []

def main():
    csv_path_or_dir = "C:\\mateball_crawling\\chatting.csv"
    db_config_path = "C:\\mateball_crawling\\MATEBALL_DATA\\db_config.yaml"

    cfg = load_db_config(db_config_path)
    conn = get_connection(cfg)

    try:
        for fp in files_to_process(csv_path_or_dir):
            df = read_chatting_csv(fp)
            if "chatting_url" not in df.columns:
                raise ValueError("CSV에 chatting_url 컬럼이 없습니다.")
            urls = df["chatting_url"].dropna().astype(str).str.strip().tolist()
            insert_urls(conn, urls, batch_size=1000)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
