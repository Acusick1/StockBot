from datetime import datetime, timezone

from src.time_db.update import update_daily

if __name__ == "__main__":
    print("Updating database:", datetime.now(tz=timezone.utc))
    update_daily()
