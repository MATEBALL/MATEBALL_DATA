import mysql.connector
import yaml

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

if __name__ == "__main__":
    db_config_path = "C:\\mateball_crawling\\MATEBALL_DATA\\db_config.yaml"
    db_config = load_db_config(db_config_path)