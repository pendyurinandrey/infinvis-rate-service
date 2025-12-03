from datetime import date
from decimal import Decimal

import aiohttp
import pytest
import polars as pl
from polars.testing import assert_frame_equal

from src.config import PolygonConfig
from src.fx.source.abstract_source import SCHEMA
from src.fx.source.polygon import PolygonFiatExchangeRatesSource, PolygonCryptoExchangeRatesSource
from tests import util


@pytest.mark.asyncio(loop_scope="session")
async def test_fiat_source_that_fiat_response_with_2_records_will_be_parsed(wm_server, datadir):
    async with aiohttp.ClientSession() as session:
        payload = await util.read_file(f'{datadir}/fiat_to_fiat_2_records.json')
        url = util.create_mock_random_uri(wm_server, payload)
        conf = PolygonConfig(url, '123', True)
        source = PolygonFiatExchangeRatesSource(conf, session)
        pldf = await source.get_exchange_rates('USD','EUR',
                                               date(2025, 12, 1), date(2025, 12, 2))
        actual = await pldf.sort('date').collect_async()
        expected = pl.DataFrame([
            [date(2025, 12, 1), 'USD', 'EUR', Decimal('0.86152')],
            [date(2025, 12, 2), 'USD', 'EUR', Decimal('0.85993')],
        ], schema=SCHEMA, orient='row')
        assert_frame_equal(actual, util.cast_rate(expected))


@pytest.mark.asyncio(loop_scope="session")
async def test_if_currency_code_to_is_invalid_then_empty_df_will_be_returned(wm_server, datadir):
    async with aiohttp.ClientSession() as session:
        payload = await util.read_file(f'{datadir}/fiat_to_fiat_invalid_fiat_code.json')
        url = util.create_mock_random_uri(wm_server, payload)
        conf = PolygonConfig(url, '123', True)
        source = PolygonFiatExchangeRatesSource(conf, session)
        pldf = await source.get_exchange_rates('USD', 'RW',
                                               date(2025, 12, 1), date(2025, 12, 2))
        actual = await pldf.sort('date').collect_async()
        assert actual.is_empty()


@pytest.mark.asyncio(loop_scope="session")
async def test_crypto_source_that_3_crypto_to_fiat_records_will_be_parsed(wm_server, datadir):
    async with aiohttp.ClientSession() as session:
        payload = await util.read_file(f'{datadir}/crypto_to_fiat_3_records.json')
        url = util.create_mock_random_uri(wm_server, payload)
        conf = PolygonConfig(url, '123', True)
        source = PolygonCryptoExchangeRatesSource(conf, session)
        pldf = await source.get_exchange_rates('USDT', 'USD',
                                               date(2025, 11, 30), date(2025, 12, 2))
        actual = await pldf.sort('date').collect_async()
        expected = pl.DataFrame([
            [date(2025, 11, 30), 'USDT', 'USD', Decimal('1.00017')],
            [date(2025, 12, 1), 'USDT', 'USD', Decimal('1.00009')],
            [date(2025, 12, 2), 'USDT', 'USD', Decimal('1.00023')],
        ], schema=SCHEMA, orient='row')
        assert_frame_equal(actual, util.cast_rate(expected))


@pytest.mark.asyncio(loop_scope="session")
async def test_crypto_source_that_3_fiat_to_crypto_records_will_be_parsed_if_spread_is_allowed(wm_server, datadir):
    async with aiohttp.ClientSession() as session:
        payload = await util.read_file(f'{datadir}/btc_to_usd_3_records.json')
        url = util.create_mock_random_uri(wm_server, payload)
        conf = PolygonConfig(url, '123', True)
        source = PolygonCryptoExchangeRatesSource(conf, session)
        pldf = await source.get_exchange_rates('USD', 'BTC',
                                               date(2025, 11, 30), date(2025, 12, 2))
        actual = await pldf.sort('date').collect_async()
        expected = pl.DataFrame([
            [date(2025, 11, 30), 'USD', 'BTC', Decimal('1')/Decimal('90369.51')],
            [date(2025, 12, 1), 'USD', 'BTC', Decimal('1')/Decimal('86447')],
            [date(2025, 12, 2), 'USD', 'BTC', Decimal('1')/Decimal('91313.5')],
        ], schema=SCHEMA, orient='row')
        assert_frame_equal(actual, util.cast_rate(expected))


@pytest.mark.asyncio(loop_scope="session")
async def test_crypto_source_that_empty_df_will_be_returned_if_spread_is_not_allowed(wm_server, datadir):
    async with aiohttp.ClientSession() as session:
        payload = await util.read_file(f'{datadir}/btc_to_usd_3_records.json')
        url = util.create_mock_random_uri(wm_server, payload)
        conf = PolygonConfig(url, '123', False)
        source = PolygonCryptoExchangeRatesSource(conf, session)
        pldf = await source.get_exchange_rates('USD', 'BTC',
                                               date(2025, 11, 30), date(2025, 12, 2))
        actual = await pldf.sort('date').collect_async()
        assert actual.is_empty()