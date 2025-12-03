import uuid

import aiofiles
import polars as pl
from wiremock.resources.mappings import MappingRequest, Mapping, HttpMethods, MappingResponse
from wiremock.resources.mappings.resource import Mappings

from src.fx.source.abstract_source import DECIMAL_MONEY_TYPE


async def read_file(path: str) -> str:
    async with aiofiles.open(path, 'r') as f:
        return await f.read()

def cast_rate(pldf):
    return pldf.with_columns([pl.col('rate').cast(DECIMAL_MONEY_TYPE).alias('rate')])

def create_mock_random_uri(wm_server, payload: str, http_status_code: int = 200) -> str:
    uri = f'/{str(uuid.uuid4())}'
    create_mock(uri, payload, http_status_code)
    return wm_server.get_url(uri)


def create_mock(uri: str, payload: str, http_status_code: int = 200):
    Mappings.create_mapping(
        Mapping(
            request=MappingRequest(method=HttpMethods.GET, url=uri),
            response=MappingResponse(status=http_status_code, body=payload, headers={"Content-Type": "application/json"}),
            persistent=False,
        )
    )