from datetime import date
from decimal import Decimal
from typing import Dict, Any

import polars as pl

from src.config import AlphavantageConfig
from src.fx.currency_helpers import is_crypto, is_fiat
from src.fx.source.abstract_source import AbstractExchangeRatesSource, create_empty_df, DECIMAL_MONEY_TYPE


class AlphavantageFiatExchangeRatesSource(AbstractExchangeRatesSource):
    def __init__(self, config: AlphavantageConfig, client_session):
        super().__init__({
            'usd': {
                'rub',
                'eur',
                'uzs',
                'amd',
                'thb',
                'aed',
                'rsd',
            },
            'eur': {
                'rub'
            }
        })
        self._config = config
        self._client_session = client_session

    async def get_exchange_rates(self,
                                 from_currency_code: str,
                                 to_currency_code: str,
                                 from_date: date,
                                 to_date: date) -> pl.LazyFrame:
        url = (self._config.fiat_url_pattern
               .format(curr_from=from_currency_code,
                       curr_to=to_currency_code,
                       key=self._config.api_key))
        return await _get_fx_rates(self._client_session, url, from_currency_code, to_currency_code)


class AlphavantageCryptoExchangeRatesSource(AbstractExchangeRatesSource):
    def __init__(self, config: AlphavantageConfig, client_session):
        super().__init__({
            'usd': {
                'btc',
                'eth',
                'usdt'
            }
        })
        self._config = config
        self._client_session = client_session

    async def get_exchange_rates(self,
                                 from_currency_code: str,
                                 to_currency_code: str,
                                 from_date: date,
                                 to_date: date) -> pl.LazyFrame:
        if is_crypto(from_currency_code) and is_fiat(to_currency_code):
            url = self._config.crypto_url_pattern.format(
                symbol=from_currency_code,
                market=to_currency_code,
                key=self._config.api_key)
            return await _get_fx_rates(self._client_session, url, from_currency_code, to_currency_code)
        elif is_fiat(from_currency_code) and is_crypto(to_currency_code):
            url = self._config.crypto_url_pattern.format(
                symbol=to_currency_code,
                market=from_currency_code,
                key=self._config.api_key)
            pldf: pl.LazyFrame = await _get_fx_rates(self._client_session, url, to_currency_code, from_currency_code)
            return pldf.select(date=pl.col('date'),
                               currencyCodeFrom=pl.col('currencyCodeTo'),
                               currencyCodeTo=pl.col('currencyCodeFrom'),
                               rate=pl.lit(1).truediv(pl.col('rate')).cast(DECIMAL_MONEY_TYPE))
        return create_empty_df()


async def _get_fx_rates(client_session,
                        url: str,
                        from_currency_code: str,
                        to_currency_code: str) -> pl.LazyFrame:
    async with client_session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            return _parse_response(data, from_currency_code, to_currency_code)
        else:
            return create_empty_df()


def _parse_response(data: Dict[str, Any],
                    from_currency_code: str,
                    to_currency_code: str) -> pl.LazyFrame:
    key = _find_time_series_key(data)
    converted_data = []
    for k, v in data[key].items():
        converted_data.append([k, Decimal(v["4. close"])])
    return (pl.LazyFrame(data=converted_data, schema=['date', 'rate'], orient='row')
            .select(pl.col('date').str.to_date('%Y-%m-%d'),
                    pl.lit(from_currency_code).alias('currencyCodeFrom'),
                    pl.lit(to_currency_code).alias('currencyCodeTo'),
                    pl.col('rate').cast(DECIMAL_MONEY_TYPE)))


def _find_time_series_key(data: Dict[str, Any]) -> str:
    expected_key = 'Time Series'
    for k, _ in data.items():
        if k.startswith(expected_key):
            return k
    raise ValueError("Key, which starts with '%s' not found" % expected_key)
