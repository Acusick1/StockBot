from datetime import datetime
from src.db.main import DatabaseApi


if __name__ == "__main__":

    db = DatabaseApi()
    print("Updating database:", datetime.now())
    db.update_daily()