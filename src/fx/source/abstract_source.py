from abc import ABC, abstractmethod
from datetime import date
from typing import Optional, Dict
import polars as pl

SCHEMA = ['date', 'currencyCodeFrom', 'currencyCodeTo', 'rate']


class AbstractExchangeRatesSource(ABC):

    def __init__(self, supported_pairs: Optional[Dict[str, set[str]]]):
        self._supported_pairs = supported_pairs

    @abstractmethod
    async def get_exchange_rates(self,
                                 from_currency_code: str,
                                 to_currency_code: str,
                                 from_date: date,
                                 to_date: date) -> pl.LazyFrame:
        pass

    def is_pair_supported(self, from_currency_code: str, to_currency_code: str) -> bool:
        fcc = from_currency_code.lower()
        tcc = to_currency_code.lower()
        if (fcc in self._supported_pairs) and (tcc in self._supported_pairs[fcc]):
            return True
        return False

def create_empty_df() -> pl.LazyFrame:
    return pl.LazyFrame(data=[], schema=SCHEMA)