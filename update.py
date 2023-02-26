from src.db.main import DatabaseApi


if __name__ == "__main__":

    db = DatabaseApi()
    db.update_daily()