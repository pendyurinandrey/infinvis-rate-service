import aiofiles
import polars as pl

from src.fx.source.abstract_source import DECIMAL_MONEY_TYPE


async def read_file(path: str) -> str:
    async with aiofiles.open(path, 'r') as f:
        return await f.read()

def cast_rate(pldf):
    return pldf.with_columns([pl.col('rate').cast(DECIMAL_MONEY_TYPE).alias('rate')])