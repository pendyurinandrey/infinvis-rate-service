from datetime import datetime
from decimal import Decimal

from sqlalchemy import TIMESTAMP, String, Numeric, DateTime, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped
from sqlalchemy.testing.schema import mapped_column

from src.models import Base


class FxRate(Base):
    __tablename__ = 'fx_rates'

    date: Mapped[datetime] = mapped_column(DateTime)
    currency_code_from: Mapped[str] = mapped_column(String(3))
    currency_code_to: Mapped[str] = mapped_column(String(3))
    rate: Mapped[Decimal] = mapped_column(Numeric(38, 10))

    __table_args__ = (
        PrimaryKeyConstraint('date', 'currency_code_from', 'currency_code_to', name='fx_rates_unique_idx'),)

class FxTrackingPair(Base):
    __tablename__ = 'fx_tracking_pairs'

    currency_code_from: Mapped[str] = mapped_column(String(3))
    currency_code_to: Mapped[str] = mapped_column(String(3))
    sources_config: Mapped[JSONB] = mapped_column(JSONB)
    last_sync_date: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP(True))
    last_sync_status: Mapped[str] = mapped_column(String(10))
    last_rate_date: Mapped[datetime] = mapped_column(DateTime)

    __table_args__ = (
        PrimaryKeyConstraint('currency_code_from', 'currency_code_to', name='fx_tracking_pairs_unique_idx'),)