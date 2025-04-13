from datetime import date
from decimal import Decimal

import aiohttp
import pytest
from wiremock.resources.mappings import Mapping, MappingRequest, HttpMethods, MappingResponse
from wiremock.resources.mappings.resource import Mappings

import polars as pl

from src.config import AlphavantageConfig
from src.fx.source.abstract_source import SCHEMA
from src.fx.source.alphavantage import AlphavantageFiatExchangeRatesSource
from tests import util


@pytest.mark.asyncio(loop_scope="session")
async def test_that_fiat_response_with_3_records_will_be_parsed(wm_server, datadir):
    async with aiohttp.ClientSession() as session:
        url = 'http://localhost:{}/query'.format(wm_server.port)
        payload = await util.read_file(f'{datadir}/fiat_3_records.json')
        _create_mock('/query', payload)
        conf = AlphavantageConfig(fiat_url_pattern=url, api_key='123')
        source = AlphavantageFiatExchangeRatesSource(conf, session)
        pldf = await source.get_exchange_rates('EUR', 'USD',
                                               date(2025, 4, 9), date(2025, 4, 11))
        actual = await pldf.sort('date').collect_async()
        expected = pl.DataFrame([
            [date(2025, 4, 9), 'EUR', 'USD', Decimal('21')],
            [date(2025, 4, 10), 'EUR', 'USD', Decimal('17')],
            [date(2025, 4, 11), 'EUR', 'USD', Decimal('13')],
        ], schema=SCHEMA, orient='row')
        assert actual.equals(expected)


def _create_mock(uri: str, payload: str):
    Mappings.create_mapping(
        Mapping(
            request=MappingRequest(method=HttpMethods.GET, url=uri),
            response=MappingResponse(status=200, body=payload),
            persistent=False,
        )
    )
