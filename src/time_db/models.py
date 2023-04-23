import sqlalchemy as sql
from sqlalchemy import text
from sqlalchemy.orm import Session, relationship
from src.time_db.database import Base, engine


class Stock(Base):
    __tablename__ = "stock"

    ticker = sql.Column(sql.String(10), primary_key=True)
    daily = relationship("Daily", back_populates="stock")


class Daily(Base):
    __tablename__ = "daily"
    
    stock_id = sql.Column(sql.ForeignKey("stock.ticker", ondelete="CASCADE"))
    timestamp = sql.Column(sql.DateTime(timezone=True), nullable=False)
    open = sql.Column(sql.Float)
    high = sql.Column(sql.Float)
    low = sql.Column(sql.Float)
    close = sql.Column(sql.Float)
    adj_close = sql.Column(sql.Float)
    volume = sql.Column(sql.Integer)
    stock = relationship("Stock", back_populates="daily")
    
    __table_args__ = (
        sql.UniqueConstraint('stock_id', 'timestamp'),
    )
    __mapper_args__ = {
        "primary_key": [timestamp, stock_id]
    }


def recreate_tables(force: bool = False):
    
    if not force:
        confirm = input('Are you sure you want to drop and recreate the tables? (y/n): ')
    else:
        confirm = 'y'

    if confirm.lower() == 'y':
        Base.metadata.drop_all(engine, checkfirst=True)
        Base.metadata.create_all(engine)
        with Session(engine) as session:
            session.execute(text("SELECT create_hypertable('{table}', 'timestamp');".format(table=Daily.__tablename__)))
            session.commit()


if __name__ == "__main__":
    recreate_tables()