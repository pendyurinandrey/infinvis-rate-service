from datetime import datetime
from decimal import Decimal

from sqlalchemy import TIMESTAMP, String, Numeric
from sqlalchemy.orm import DeclarativeBase, Mapped
from sqlalchemy.testing.schema import mapped_column


class Base(DeclarativeBase):
    pass


class FxRate(Base):
    __tablename__ = 'fx_rates'

    date: Mapped[datetime] = mapped_column(TIMESTAMP)
    currency_code_from: Mapped[str] = mapped_column(String(3))
    currency_code_to: Mapped[str] = mapped_column(String(3))
    rate: Mapped[Decimal] = mapped_column(Numeric(38, 10))