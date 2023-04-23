from datetime import datetime
from src.time_db.update import update_daily


if __name__ == "__main__":

    print("Updating database:", datetime.now())
    update_daily()