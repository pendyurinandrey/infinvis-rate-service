from datetime import date
from decimal import Decimal

import aiohttp
import pytest

from polars.testing import assert_frame_equal
import polars as pl

from src.config import AlphavantageConfig
from src.fx.source.abstract_source import SCHEMA
from src.fx.source.alphavantage import AlphavantageFiatExchangeRatesSource, AlphavantageCryptoExchangeRatesSource
from tests import util


@pytest.mark.asyncio(loop_scope="session")
async def test_that_fiat_response_with_3_records_will_be_parsed(wm_server, datadir):
    async with aiohttp.ClientSession() as session:
        payload = await util.read_file(f'{datadir}/fiat_3_records.json')
        url = util.create_mock_random_uri(wm_server, payload)
        conf = AlphavantageConfig(fiat_url_pattern=url, crypto_url_pattern='', api_key='123')
        source = AlphavantageFiatExchangeRatesSource(conf, session)
        pldf = await source.get_exchange_rates('EUR', 'USD',
                                               date(2025, 4, 9), date(2025, 4, 11))
        actual = await pldf.sort('date').collect_async()
        expected = pl.DataFrame([
            [date(2025, 4, 9), 'EUR', 'USD', Decimal('21')],
            [date(2025, 4, 10), 'EUR', 'USD', Decimal('17')],
            [date(2025, 4, 11), 'EUR', 'USD', Decimal('13')],
        ], schema=SCHEMA, orient='row')
        assert_frame_equal(actual, util.cast_rate(expected))


@pytest.mark.asyncio(loop_scope="session")
async def test_that_500_status_code_will_be_converted_to_empty_df(wm_server):
    async with aiohttp.ClientSession() as session:
        payload = '{}'
        url = util.create_mock_random_uri(wm_server, payload, http_status_code=500)
        conf = AlphavantageConfig(fiat_url_pattern=url, crypto_url_pattern='', api_key='123')
        source = AlphavantageFiatExchangeRatesSource(conf, session)
        pldf = await source.get_exchange_rates('EUR', 'USD',
                                               date(2025, 4, 9), date(2025, 4, 11))
        actual = await pldf.collect_async()
        assert actual.is_empty()


@pytest.mark.asyncio(loop_scope="session")
async def test_that_crypto_to_fiat_with_3_records_will_be_parsed(wm_server, datadir):
    async with aiohttp.ClientSession() as session:
        payload = await util.read_file(f'{datadir}/crypto_3_records.json')
        url = util.create_mock_random_uri(wm_server, payload)
        conf = AlphavantageConfig(fiat_url_pattern='', crypto_url_pattern=url, api_key='123')
        source = AlphavantageCryptoExchangeRatesSource(conf, session)
        pldf = await source.get_exchange_rates('BTC', 'USD',
                                               date(2025, 4, 24), date(2025, 4, 26))
        actual = await pldf.sort('date').collect_async()
        expected = pl.DataFrame([
            [date(2025, 4, 24), 'BTC', 'USD', Decimal('10')],
            [date(2025, 4, 25), 'BTC', 'USD', Decimal('4')],
            [date(2025, 4, 26), 'BTC', 'USD', Decimal('2')],
        ], schema=SCHEMA, orient='row')
        assert_frame_equal(actual, util.cast_rate(expected))


@pytest.mark.asyncio(loop_scope="session")
async def test_that_fiat_to_crypto_with_3_records_will_be_parsed(wm_server, datadir):
    async with aiohttp.ClientSession() as session:
        payload = await util.read_file(f'{datadir}/crypto_3_records.json')
        url = util.create_mock_random_uri(wm_server, payload)
        conf = AlphavantageConfig(fiat_url_pattern='', crypto_url_pattern=url, api_key='123')
        source = AlphavantageCryptoExchangeRatesSource(conf, session)
        pldf = await source.get_exchange_rates('USD', 'BTC',
                                               date(2025, 4, 24), date(2025, 4, 26))
        actual = await pldf.sort('date').collect_async()
        expected = pl.DataFrame([
            [date(2025, 4, 24), 'USD', 'BTC', Decimal('0.1')],
            [date(2025, 4, 25), 'USD', 'BTC', Decimal('0.25')],
            [date(2025, 4, 26), 'USD', 'BTC', Decimal('0.5')],
        ], schema=SCHEMA, orient='row')
        assert_frame_equal(actual, util.cast_rate(expected))

