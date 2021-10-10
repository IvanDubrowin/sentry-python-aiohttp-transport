import asyncio
import logging
import os

import sentry_sdk

from sentry_aiohttp_transport import AiohttpTransport

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DSN = os.getenv('SENTRY_DSN', '')


def setup(loop: asyncio.AbstractEventLoop) -> None:
    sentry_sdk.init(
        DSN,
        transport=AiohttpTransport(dsn=DSN, loop=loop),
        traces_sample_rate=1.0,
    )


async def error() -> None:
    try:
        division_by_zero = 1 / 0
    except Exception as err:
        sentry_sdk.capture_exception(err)
        await asyncio.sleep(3)


def main() -> None:
    logger.debug('test')
    loop = asyncio.get_event_loop()
    setup(loop)
    loop.run_until_complete(error())


if __name__ == '__main__':
    main()
