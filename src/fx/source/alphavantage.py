import json
from datetime import date
from decimal import Decimal
from typing import Dict, Any

import polars as pl

from src.config import AlphavantageConfig
from src.fx.source.abstract_source import AbstractExchangeRatesSource, create_empty_df


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


async def _get_fx_rates(client_session,
                        url: str,
                        from_currency_code: str,
                        to_currency_code: str) -> pl.LazyFrame:
    async with client_session.get(url) as response:
        if response.status == 200:
            text = await response.text()
            return _parse_response(json.loads(text), from_currency_code, to_currency_code)
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
                    'rate'))


def _find_time_series_key(data: Dict[str, Any]) -> str:
    expected_key = 'Time Series'
    for k, _ in data.items():
        if k.startswith(expected_key):
            return k
    raise ValueError("Key, which starts with '%s' not found" % expected_key)
