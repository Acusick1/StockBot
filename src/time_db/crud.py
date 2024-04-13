from typing import Type, Optional
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from src.time_db.database import engine
from src.time_db.database import Base


def get_record_by_filter(model: Type[Base], data: dict):  # type: ignore
    with Session(engine) as session:
        instance = session.query(model).filter_by(**data).first()
        return instance


def get_record_by_id(model: Type[Base], record_id: int) -> Optional[Type[Base]]:  # type: ignore
    with Session(engine) as session:
        result = session.query(model).get(record_id)
        return result


def create_record(model: Type[Base], data: dict) -> Type[Base]:  # type: ignore
    obj = model(**data)

    with Session(engine) as session:
        session.add(obj)
        session.commit()
        session.refresh(obj)

    return obj


def create_records(model: Type[Base], data_list: list[dict]) -> list[Base]:  # type: ignore
    with Session(engine) as session:
        table = model.__table__

        # Generate insert statement with conflict handling
        stmt = insert(table).values(data_list).on_conflict_do_nothing()

        # Execute the insert statement
        session.execute(stmt)
        session.commit()


def update_record(
    model: Type[Base], record_id: int, data: dict
) -> Optional[Type[Base]]:  # type: ignore
    with Session(engine) as session:
        obj = session.query(model).get(record_id)
        if not obj:
            return None
        for field, value in data.items():
            setattr(obj, field, value)
        session.commit()
        session.refresh(obj)
        return obj


def delete_record(model: Type[Base], record_id: int) -> None:  # type: ignore
    with Session(engine) as session:
        obj = session.query(model).get(record_id)
        session.delete(obj)
        session.commit()
