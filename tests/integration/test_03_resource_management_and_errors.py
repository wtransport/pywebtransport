"""Integration tests for resource management and error handling."""

import pytest

from pywebtransport import ClientConfig, ClientError, TimeoutError, WebTransportClient, WebTransportSession

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(argnames="server_config", argvalues=[{"max_connections": 1}], indirect=True)
async def test_max_connections_limit_rejection(
    sessions: tuple[WebTransportSession, WebTransportSession], client_config: ClientConfig, url: str
) -> None:
    async with WebTransportClient(config=client_config) as second_client:
        with pytest.raises(expected_exception=ClientError):
            await second_client.connect(url=url)


@pytest.mark.parametrize(
    argnames="client_config, server_config",
    argvalues=[({"stream_create_timeout": 0.5}, {"initial_max_streams_bidi": 2})],
    indirect=True,
)
async def test_max_streams_limit(sessions: tuple[WebTransportSession, WebTransportSession]) -> None:
    client_session, _ = sessions

    for _ in range(2):
        await client_session.create_bidirectional_stream()

    with pytest.raises(expected_exception=TimeoutError):
        await client_session.create_bidirectional_stream()
