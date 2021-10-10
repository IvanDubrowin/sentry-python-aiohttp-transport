import asyncio
import gzip
import io
import logging
import socket
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, DefaultDict, Literal, Optional, Type

import aiohttp
from sentry_sdk import Hub
from sentry_sdk.consts import VERSION
from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_sdk.transport import Transport
from sentry_sdk.utils import Dsn, capture_internal_exceptions, json_dumps

from sentry_aiohttp_transport.helpers import parse_rate_limits

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10
TOO_MANY_REQUESTS = 429
IS_SUCCESS = 200
IS_REDIRECT = 300

EndpointType = Literal['store', 'envelope']


class AiohttpTransport(Transport):
    """Transport uses aiohttp client"""
    def __init__(
        self,
        dsn: str,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        keepalive: bool = False,
        socket_family: int = socket.AF_INET,
        timeout: int = DEFAULT_TIMEOUT,
        verify_ssl: bool = True,
    ) -> None:
        super().__init__(options={'dsn': dsn})
        if loop is None:
            loop = asyncio.get_event_loop()

        self._loop: asyncio.AbstractEventLoop = loop
        self._keepalive = keepalive
        self._family = socket_family
        self._timeout = timeout
        self._verify_ssl = verify_ssl
        self._auth = self.dsn.to_auth(f'sentry.python/{VERSION}')
        self._tasks: set[asyncio.Task] = set()
        self._disabled_until: dict[Optional[str], datetime] = {}
        self._last_client_report_sent = time.time()
        self._discarded_events: DefaultDict[tuple[str, str], int] = defaultdict(int)
        self._hub: Type[Hub] = Hub

        self._session: Optional[aiohttp.ClientSession] = None

        if self._keepalive:
            self._session = self._make_session()

    def capture_event(self, event: dict[str, Any]) -> None:
        with self._hub.current:
            with capture_internal_exceptions():
                self._send_event(event)
                self._flush_client_reports()

    def _send_event(self, event: dict[str, Any]) -> None:
        if self._check_disabled('error'):
            self.record_lost_event('ratelimit_backoff', data_category='error')
            return None

        body = io.BytesIO()
        with gzip.GzipFile(fileobj=body, mode='w') as file:
            file.write(json_dumps(event))

        logger.debug(
            'Sending event, type:%s level:%s event_id:%s project:%s host:%s'
            % (
                event.get('type') or 'null',
                event.get('level') or 'null',
                event.get('event_id') or 'null',
                self.dsn.project_id,
                self.dsn.host,
            ),
        )
        self._send_request(
            body.getvalue(),
            headers={
                'Content-Type': 'application/json',
                'Content-Encoding': 'gzip',
            },
        )
        return None

    def _check_disabled(self, category: str) -> bool:
        ts = self._disabled_until.get(category)
        return ts is not None and ts > datetime.utcnow()

    def capture_envelope(self, envelope: Envelope) -> None:
        with self._hub.current:
            with capture_internal_exceptions():
                self._send_envelope(envelope)
                self._flush_client_reports()

    def _flush_client_reports(self, force: bool = False) -> None:
        client_report = self._fetch_pending_client_report(force=force, interval=60)
        if client_report is not None:
            self.capture_envelope(Envelope(items=[client_report]))

    def _fetch_pending_client_report(self, force: bool = False, interval: int = 60) -> Optional[Item]:
        if not (force or self._last_client_report_sent < time.time() - interval):
            return None

        discarded_events = self._discarded_events
        self._discarded_events = defaultdict(int)
        self._last_client_report_sent = time.time()

        if not discarded_events:
            return None

        return Item(
            PayloadRef(
                json={
                    'timestamp': time.time(),
                    'discarded_events': [
                        {'reason': reason, 'category': category, 'quantity': quantity}
                        for ((category, reason), quantity) in discarded_events.items()
                    ],
                },
            ),
            type='client_report',
        )

    def _make_session(self) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(
            verify_ssl=self._verify_ssl,
            family=self._family,
            loop=self._loop,
        )
        return aiohttp.ClientSession(connector=connector, loop=self._loop)

    def _get_session(self) -> aiohttp.ClientSession:
        if self._keepalive:
            if self._session is None:
                self._session = self._make_session()
            return self._session
        return self._make_session()

    def record_lost_event(
        self,
        reason: str,
        data_category: Optional[str] = None,
        item: Optional[Item] = None,
    ) -> None:
        quantity = 1
        if item is not None:
            data_category = item.data_category
            if data_category == 'attachment':
                # quantity of 0 is actually 1 as we do not want to count
                # empty attachments as actually empty.
                quantity = len(item.get_bytes()) or 1
        elif data_category is None:
            raise TypeError('data category not provided')

        self._discarded_events[data_category, reason] += quantity

    def _send_request(
        self,
        body: bytes,
        headers: dict[str, str],
        endpoint_type: EndpointType = 'store',
        envelope: Optional[Envelope] = None,
    ) -> None:
        send = self._do_send(body, headers, endpoint_type, envelope)
        task = asyncio.ensure_future(send, loop=self._loop)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.remove)

    def _send_envelope(self, envelope: Envelope) -> None:
        new_items = []

        for item in envelope.items:
            if self._check_disabled(item.data_category):
                if item.data_category in ('transaction', 'error', 'default'):  # noqa: WPS510
                    self.on_dropped_event('self_rate_limits')
                self.record_lost_event('ratelimit_backoff', item=item)
            else:
                new_items.append(item)

        envelope = Envelope(headers=envelope.headers, items=new_items)

        if not envelope.items:
            return None

        client_report_item = self._fetch_pending_client_report(interval=30)  # noqa: WPS432

        if client_report_item is not None:
            envelope.items.append(client_report_item)

        body = io.BytesIO()
        with gzip.GzipFile(fileobj=body, mode='w') as file:
            envelope.serialize_into(file)

        logger.debug(
            f'Sending envelope [{envelope.description}] '
            f'project:{self.dsn.project_id} '
            f'host:{self.dsn.host}',
        )

        self._send_request(
            body.getvalue(),
            headers={
                'Content-Type': 'application/x-sentry-envelope',
                'Content-Encoding': 'gzip',
            },
            endpoint_type='envelope',
            envelope=envelope,
        )
        return None

    @staticmethod
    def on_dropped_event(reason: str) -> None:
        logger.debug(f'On dropped event reason: {reason}')

    def _update_rate_limits(self, response: aiohttp.ClientResponse) -> None:
        header = response.headers.get('x-sentry-rate-limits')

        if header is not None:
            logger.warning('Rate-limited via x-sentry-rate-limits')
            self._disabled_until.update(parse_rate_limits(header))

        # TODO 429 response code logic?

    async def _close(self) -> None:
        await asyncio.gather(
            *self._tasks,
            return_exceptions=True,
            loop=self._loop,
        )

        if self._session is not None:
            await self._session.close()

        if self._tasks:
            raise AssertionError

    async def _do_send(
        self,
        body: bytes,
        headers: dict[str, str],
        endpoint_type: EndpointType = 'store',
        envelope: Optional[Envelope] = None,
    ) -> None:
        session = self._get_session()
        url = self._auth.get_api_url(endpoint_type)

        headers.update(
            {
                'User-Agent': str(self._auth.client),
                'X-Sentry-Auth': self._auth.to_header(),
            },
        )

        try:
            await self._send(session, url, body, headers, envelope)
        except Exception as err:
            logger.error(err)
            self.on_dropped_event('network')
            self._record_loss('network_error', envelope=envelope)
            return None
        finally:
            if not self._keepalive:
                await session.close()

    async def _send(
        self,
        session: aiohttp.ClientSession,
        url: str,
        body: bytes,
        headers: dict[str, str],
        envelope: Optional[Envelope] = None,
    ) -> None:
        response = await session.post(url, data=body, headers=headers)
        self._update_rate_limits(response)

        if response.status == TOO_MANY_REQUESTS:
            self.on_dropped_event(f'status_{response.status}')
            response.close()
            return None
        elif response.status >= IS_REDIRECT or response.status < IS_SUCCESS:
            response_body = await response.read()

            logger.error(
                f'Unexpected status code: {response.status} (body: {str(response_body)})',
            )

            self.on_dropped_event(f'status_{response.status}')
            self._record_loss('network_error', envelope=envelope)
            response.release()
        return None

    def _record_loss(self, reason: str, envelope: Optional[Envelope] = None) -> None:
        if envelope is None:
            self.record_lost_event(reason, data_category='error')
        else:
            for item in envelope.items:
                self.record_lost_event(reason, item=item)

    def kill(self) -> None:
        logger.debug('Killing HTTP transport')
        self._loop.run_until_complete(self._close())

    def flush(self, timeout: float, callback: Optional[Callable] = None) -> None:
        logger.debug('Flushing HTTP transport')

    @property
    def dsn(self) -> Dsn:
        if self.parsed_dsn is None:
            raise AssertionError('Parsed dsn is none')
        return self.parsed_dsn
