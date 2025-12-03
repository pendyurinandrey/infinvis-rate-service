from datetime import date
from decimal import Decimal

import aiohttp
import pytest
import polars as pl
from polars.testing import assert_frame_equal

from src.config import PolygonConfig
from src.fx.source.abstract_source import SCHEMA
from src.fx.source.polygon import PolygonFiatExchangeRatesSource
from tests import util


@pytest.mark.asyncio(loop_scope="session")
async def test_that_fiat_response_with_2_records_will_be_parsed(wm_server, datadir):
    async with aiohttp.ClientSession() as session:
        payload = await util.read_file(f'{datadir}/fiat_2_records.json')
        url = util.create_mock_random_uri(wm_server, payload)
        conf = PolygonConfig(url, '123')
        source = PolygonFiatExchangeRatesSource(conf, session)
        pldf = await source.get_exchange_rates('USD','EUR',
                                               date(2025, 12, 1), date(2025, 12, 2))
        actual = await pldf.sort('date').collect_async()
        expected = pl.DataFrame([
            [date(2025, 12, 1), 'USD', 'EUR', Decimal('0.86152')],
            [date(2025, 12, 2), 'USD', 'EUR', Decimal('0.85993')],
        ], schema=SCHEMA, orient='row')
        assert_frame_equal(actual, util.cast_rate(expected))