from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from src.time_db.database import Base, engine


def get_record_by_filter(model: type[Base], data: dict):  # type: ignore
    with Session(engine) as session:
        return session.query(model).filter_by(**data).first()


def get_record_by_id(model: type[Base], record_id: int) -> type[Base] | None:  # type: ignore
    with Session(engine) as session:
        return session.query(model).get(record_id)


def create_record(model: type[Base], data: dict) -> type[Base]:  # type: ignore
    obj = model(**data)

    with Session(engine) as session:
        session.add(obj)
        session.commit()
        session.refresh(obj)

    return obj


def create_records(model: type[Base], data_list: list[dict]) -> list[Base]:  # type: ignore
    with Session(engine) as session:
        table = model.__table__

        # Generate insert statement with conflict handling
        stmt = insert(table).values(data_list).on_conflict_do_nothing()

        # Execute the insert statement
        session.execute(stmt)
        session.commit()


def update_record(model: type[Base], record_id: int, data: dict) -> type[Base] | None:  # type: ignore
    with Session(engine) as session:
        obj = session.query(model).get(record_id)
        if not obj:
            return None
        for field, value in data.items():
            setattr(obj, field, value)
        session.commit()
        session.refresh(obj)
        return obj


def delete_record(model: type[Base], record_id: int) -> None:  # type: ignore
    with Session(engine) as session:
        obj = session.query(model).get(record_id)
        session.delete(obj)
        session.commit()
