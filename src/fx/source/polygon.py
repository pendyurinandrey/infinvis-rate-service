from datetime import date, datetime
from decimal import Decimal
from typing import Dict, Tuple

import polars as pl
import pytz

from src.config import PolygonConfig
from src.fx.currency_helpers import is_fiat, is_crypto
from src.fx.source.abstract_source import AbstractExchangeRatesSource, create_empty_df, DECIMAL_MONEY_TYPE


def _build_allowed_crypto_fiat_pairs(config: PolygonConfig) -> Dict[str, set[str]]:
    base = {
        'btc': {
            'usd'
        },
        'eth': {
            'usd'
        },
        'usdt': {
            'usd'
        }
    }

    if config.ignore_spread:
        cross_pairs = dict()
        for k, v in base.items():
            for code in v:
                if code in cross_pairs:
                    cross_pairs[code].add(k)
                else:
                    cross_pairs[code] = {k}
        base.update(cross_pairs)
    return base


class PolygonCryptoExchangeRatesSource(AbstractExchangeRatesSource):

    def __init__(self, config: PolygonConfig, client_session):
        super().__init__(_build_allowed_crypto_fiat_pairs(config))
        self._config = config
        self._base_provider = _BasePolygonFxProvider(config, client_session)

    async def get_exchange_rates(self,
                                 from_currency_code: str,
                                 to_currency_code: str,
                                 from_date: date,
                                 to_date: date) -> pl.LazyFrame:
        if is_fiat(from_currency_code) and self._config.ignore_spread:
            # Polygon doesn't work if first currency is fiat, need to swap currencies and adjust rate
            ticker = f'X:{to_currency_code}{from_currency_code}'
            res = await self._base_provider.get_exchange_rates(ticker,
                                                               (to_currency_code, from_currency_code),
                                                               from_date,
                                                               to_date)
            return res.select('date',
                              pl.col('currencyCodeTo').alias('currencyCodeFrom'),
                              pl.col('currencyCodeFrom').alias('currencyCodeTo'),
                              pl.lit(1).truediv(pl.col('rate')).cast(DECIMAL_MONEY_TYPE).alias('rate'))
        elif is_crypto(from_currency_code):
            ticker = f'X:{from_currency_code}{to_currency_code}'
            return await self._base_provider.get_exchange_rates(ticker,
                                                                (from_currency_code, to_currency_code),
                                                                from_date,
                                                                to_date)
        else:
            return create_empty_df()


class PolygonFiatExchangeRatesSource(AbstractExchangeRatesSource):

    def __init__(self, conf: PolygonConfig, client_session):
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
        self._base_provider = _BasePolygonFxProvider(conf, client_session)

    async def get_exchange_rates(self, from_currency_code: str, to_currency_code: str, from_date: date,
                                 to_date: date) -> pl.LazyFrame:
        ticker = f'C:{from_currency_code}{to_currency_code}'
        return await self._base_provider.get_exchange_rates(ticker,
                                                            (from_currency_code, to_currency_code),
                                                            from_date,
                                                            to_date)


class _BasePolygonFxProvider:
    def __init__(self, config: PolygonConfig, client_session):
        self._config = config
        self._client_session = client_session

    async def get_exchange_rates(self,
                                 ticker: str,
                                 from_to_codes: Tuple[str, str],
                                 from_date: date,
                                 to_date: date) -> pl.LazyFrame:
        from_dt_str = from_date.strftime('%Y-%m-%d')
        to_dt_str = to_date.strftime('%Y-%m-%d')
        formatted_url = self._config.url_pattern.format(
            ticker=ticker,
            from_dt=from_dt_str,
            to_dt=to_dt_str,
            api_key=self._config.api_key
        )
        async with self._client_session.get(formatted_url) as response:
            if response.status == 200:
                text = await response.json()
                return self._parse_response(text, from_to_codes)
            else:
                return create_empty_df()

    def _parse_response(self, data, from_to_codes: Tuple[str, str]) -> pl.LazyFrame:
        currency_code_from = from_to_codes[0]
        currency_code_to = from_to_codes[1]
        rows = []
        if 'results' not in data:
            return create_empty_df()
        for item in data['results']:
            dt = datetime.fromtimestamp(item['t'] / 1000, tz=pytz.UTC).replace(tzinfo=None).date()
            r = [dt, Decimal(str(item['c']))]
            rows.append(r)
        return (pl.LazyFrame(data=rows, schema=['date', 'rate'], orient='row')
                .select(pl.col('date'),
                        pl.lit(currency_code_from).alias('currencyCodeFrom'),
                        pl.lit(currency_code_to).alias('currencyCodeTo'),
                        pl.col('rate').cast(DECIMAL_MONEY_TYPE)))
