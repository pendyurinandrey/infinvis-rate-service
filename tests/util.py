import aiofiles


async def read_file(path: str) -> str:
    async with aiofiles.open(path, 'r') as f:
        return await f.read()
