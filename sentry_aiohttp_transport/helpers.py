from datetime import datetime, timedelta
from typing import Iterable, Optional, Tuple


def parse_rate_limits(
    header: str,
    now: Optional[datetime] = None,
) -> Iterable[Tuple[Optional[str], datetime]]:
    if now is None:
        now = datetime.utcnow()

    for limit in header.split(','):  # noqa: WPS327
        try:  # noqa: WPS229
            after, categories, _ = limit.strip().split(':', 2)
            retry_after = now + timedelta(seconds=int(after))

            for category in categories and categories.split(';') or (None,):  # noqa: WPS526
                yield category, retry_after
        except (LookupError, ValueError):
            continue
