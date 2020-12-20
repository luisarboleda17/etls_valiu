from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, DateTime, Numeric, String

Base = declarative_base()
metadata = Base.metadata


def create_session(
        password: str,
        host: str = 'localhost',
        port: int = 5432,
        user: str = 'postgres',
        database: str = 'postgres'
):
    """Initialize database session

    :param host: Instance host
    :type host: str
    :param port: Instance port
    :type port: int
    :param user: Connection username
    :type user: str
    :param password: Connection password
    :type password: str
    :param database: Database name
    :type database: str
    :rtype: :class:`sqlalchemy.sessionmaker`
    """
    engine = create_engine(f'postgres://{user}:{password}@{host}:{port}/{database}', echo=False)
    return sessionmaker(bind=engine)()


class Transaction(Base):
    __tablename__ = 'transaction'

    id = Column(String(36), primary_key=True)
    id2 = Column(String(24))
    account_id_dst = Column(String(36))
    account_id_src = Column(String(36))
    amount_dst = Column(String(24))
    amount_dst_usd = Column(Numeric())
    amount_src = Column(Numeric())
    amount_src_usd = Column(Numeric())
    asset_dst = Column(String(4))
    asset_src = Column(String(4))
    contact_dst = Column(String(100))
    created_at = Column(DateTime())
    description = Column(String(200))
    order_id = Column(String(2436))
    service_name = Column(String(100))
    short_id = Column(String(15))
    state = Column(String(10))
    sync_date = Column(DateTime())
    transaction_type = Column('type', String(12))
    updated_at = Column(DateTime())
    user_id = Column(String(36))
    user_type = Column(String(6))
    v = Column(String(3))
    wallet_dst = Column(String(36))
    wallet_src = Column(String(36))
