"""Unit tests for the pywebtransport.server.cluster module."""

import asyncio
import logging
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, cast

import pytest
from pytest_asyncio import fixture as asyncio_fixture
from pytest_mock import MockerFixture

from pywebtransport import ServerConfig, ServerError
from pywebtransport.server import ServerCluster, WebTransportServer
from pywebtransport.types import ConnectionState, SessionState


class TestServerCluster:

    @asyncio_fixture
    async def cluster(
        self, server_configs: list[ServerConfig], mock_webtransport_server_class: Any
    ) -> AsyncGenerator[ServerCluster, None]:
        cluster_instance = ServerCluster(configs=server_configs)
        async with cluster_instance:
            yield cluster_instance

    @pytest.fixture
    def mock_webtransport_server_class(self, mocker: MockerFixture) -> Any:
        mock_server_class = mocker.patch("pywebtransport.server.cluster.WebTransportServer")

        def new_server_instance(*args: Any, **kwargs: Any) -> Any:
            instance = mocker.create_autospec(WebTransportServer, instance=True)
            instance.__aenter__ = mocker.AsyncMock(return_value=instance)
            instance.listen = mocker.AsyncMock()
            instance.close = mocker.AsyncMock()

            mock_stats = mocker.Mock()
            mock_stats.connections_accepted = 1
            mock_stats.connections_rejected = 1
            mock_diagnostics = mocker.Mock()
            mock_diagnostics.stats = mock_stats
            mock_diagnostics.connection_states = {ConnectionState.CONNECTED: 1}
            mock_diagnostics.session_states = {SessionState.CONNECTED: 1}
            instance.diagnostics = mocker.AsyncMock(return_value=mock_diagnostics)

            if "config" in kwargs:
                instance.config = kwargs["config"]
                if hasattr(kwargs["config"], "bind_port"):
                    local_address = ("127.0.0.1", kwargs["config"].bind_port)
                    type(instance).local_address = mocker.PropertyMock(return_value=local_address)

            return instance

        mock_server_class.side_effect = new_server_instance
        return mock_server_class

    @pytest.fixture
    def server_configs(self, tmp_path: Path) -> list[ServerConfig]:
        c1 = tmp_path / "c1.pem"
        k1 = tmp_path / "k1.pem"
        c2 = tmp_path / "c2.pem"
        k2 = tmp_path / "k2.pem"
        c1.touch()
        k1.touch()
        c2.touch()
        k2.touch()

        return [
            ServerConfig(bind_host="127.0.0.1", bind_port=8001, certfile=str(c1), keyfile=str(k1)),
            ServerConfig(bind_host="127.0.0.1", bind_port=8002, certfile=str(c2), keyfile=str(k2)),
        ]

    @pytest.mark.asyncio
    async def test_add_server(self, cluster: ServerCluster, tmp_path: Path) -> None:
        c3 = tmp_path / "c3.pem"
        k3 = tmp_path / "k3.pem"
        c3.touch()
        k3.touch()
        new_config = ServerConfig(bind_port=8003, certfile=str(c3), keyfile=str(k3))

        result = await cluster.add_server(config=new_config)

        assert result is not None
        assert len(cluster._servers) == 3
        cast(Any, result.listen).assert_awaited_once()

    @pytest.mark.asyncio
    async def test_add_server_failure_during_listen(
        self, cluster: ServerCluster, mocker: MockerFixture, mock_webtransport_server_class: Any, tmp_path: Path
    ) -> None:
        mock_fail_instance = mocker.create_autospec(WebTransportServer, instance=True)
        mock_fail_instance.__aenter__ = mocker.AsyncMock(return_value=mock_fail_instance)
        mock_fail_instance.listen = mocker.AsyncMock(side_effect=ValueError("Listen failed"))
        mock_fail_instance.close = mocker.AsyncMock()
        mock_webtransport_server_class.side_effect = [mock_fail_instance]

        c3 = tmp_path / "c3.pem"
        k3 = tmp_path / "k3.pem"
        c3.touch()
        k3.touch()
        new_config = ServerConfig(bind_port=8003, certfile=str(c3), keyfile=str(k3))

        result = await cluster.add_server(config=new_config)

        assert result is None
        cast(Any, mock_fail_instance.close).assert_awaited_once()

    @pytest.mark.asyncio
    async def test_add_server_failure_on_creation(
        self, cluster: ServerCluster, mocker: MockerFixture, tmp_path: Path
    ) -> None:
        initial_count = await cluster.get_server_count()
        mocker.patch.object(cluster, "_create_and_start_server", side_effect=IOError("Failed to bind"))
        c3 = tmp_path / "c3.pem"
        k3 = tmp_path / "k3.pem"
        c3.touch()
        k3.touch()
        new_config = ServerConfig(bind_port=8003, certfile=str(c3), keyfile=str(k3))

        result = await cluster.add_server(config=new_config)

        assert result is None
        assert await cluster.get_server_count() == initial_count

    @pytest.mark.asyncio
    async def test_add_server_race_condition_on_stop(
        self, cluster: ServerCluster, mocker: MockerFixture, mock_webtransport_server_class: Any, tmp_path: Path
    ) -> None:
        c3 = tmp_path / "c3.pem"
        k3 = tmp_path / "k3.pem"
        c3.touch()
        k3.touch()
        new_config = ServerConfig(bind_port=8003, certfile=str(c3), keyfile=str(k3))

        mock_instance = mocker.create_autospec(WebTransportServer, instance=True)
        mock_instance.__aenter__ = mocker.AsyncMock(return_value=mock_instance)
        mock_instance.listen = mocker.AsyncMock()
        mock_instance.close = mocker.AsyncMock()

        async def create_and_stop_cluster(*args: Any, **kwargs: Any) -> Any:
            if cluster._lock:
                async with cluster._lock:
                    cluster._running = False
            return mock_instance

        mocker.patch.object(cluster, "_create_and_start_server", side_effect=create_and_stop_cluster)

        result = await cluster.add_server(config=new_config)

        assert result is None
        cast(Any, mock_instance.close).assert_awaited_once()

    @pytest.mark.asyncio
    async def test_add_server_while_stopped(self, cluster: ServerCluster, tmp_path: Path) -> None:
        await cluster.stop_all()
        assert not cluster.is_running

        c3 = tmp_path / "c3.pem"
        k3 = tmp_path / "k3.pem"
        c3.touch()
        k3.touch()
        new_config = ServerConfig(bind_port=8003, certfile=str(c3), keyfile=str(k3))

        result = await cluster.add_server(config=new_config)

        assert result is None
        assert len(cluster._servers) == 0
        assert len(cluster._configs) == 3

    @pytest.mark.asyncio
    async def test_async_context_manager(self, server_configs: list[ServerConfig], mocker: MockerFixture) -> None:
        cluster = ServerCluster(configs=server_configs)
        mock_start = mocker.patch.object(cluster, "start_all", new_callable=mocker.AsyncMock)
        mock_stop = mocker.patch.object(cluster, "stop_all", new_callable=mocker.AsyncMock)

        async with cluster:
            mock_start.assert_awaited_once()
            mock_stop.assert_not_called()

        mock_stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_cluster_stats(self, cluster: ServerCluster) -> None:
        stats = await cluster.get_cluster_stats()

        assert stats["server_count"] == 2
        assert stats["total_connections_accepted"] == 2
        assert stats["total_connections_rejected"] == 2
        assert stats["total_connections_active"] == 2
        assert stats["total_sessions_active"] == 2

    @pytest.mark.asyncio
    async def test_get_cluster_stats_empty(self, mocker: MockerFixture) -> None:
        cluster = ServerCluster(configs=[])
        async with cluster:
            stats = await cluster.get_cluster_stats()
            assert stats == {
                "server_count": 0,
                "total_connections_accepted": 0,
                "total_connections_rejected": 0,
                "total_connections_active": 0,
                "total_sessions_active": 0,
            }

    @pytest.mark.asyncio
    async def test_get_cluster_stats_with_partial_failure(self, cluster: ServerCluster) -> None:
        server_to_fail = cluster._servers[0]
        cast(Any, server_to_fail.diagnostics).side_effect = ValueError("Stats failed")

        with pytest.raises(ExceptionGroup):
            await cluster.get_cluster_stats()

    @pytest.mark.asyncio
    async def test_get_server_count(self, cluster: ServerCluster) -> None:
        assert await cluster.get_server_count() == 2

    @pytest.mark.asyncio
    async def test_get_servers(self, cluster: ServerCluster) -> None:
        servers_copy = await cluster.get_servers()

        assert len(servers_copy) == 2
        assert servers_copy is not cluster._servers

    def test_init(self, server_configs: list[ServerConfig]) -> None:
        cluster = ServerCluster(configs=server_configs)

        assert cluster._configs is not server_configs
        assert not cluster.is_running
        assert not cluster._servers

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method_name, method_args, expected_match",
        [
            ("start_all", {}, "ServerCluster has not been activated"),
            ("stop_all", {}, "ServerCluster has not been activated"),
            ("add_server", {"config": None}, "ServerCluster has not been activated"),
            ("remove_server", {"host": "localhost", "port": 8000}, "ServerCluster has not been activated"),
            ("get_cluster_stats", {}, "ServerCluster has not been activated"),
            ("get_server_count", {}, "Cluster not activated"),
            ("get_servers", {}, "Cluster not activated"),
        ],
    )
    async def test_public_methods_raise_if_not_activated(
        self,
        server_configs: list[ServerConfig],
        method_name: str,
        method_args: Any,
        expected_match: str,
        tmp_path: Path,
    ) -> None:
        cluster = ServerCluster(configs=server_configs)
        method = getattr(cluster, method_name)

        if method_name == "add_server":
            c = tmp_path / "c_p.pem"
            k = tmp_path / "k_p.pem"
            c.touch()
            k.touch()
            method_args = {"config": ServerConfig(certfile=str(c), keyfile=str(k))}

        with pytest.raises(ServerError, match=expected_match):
            await method(**method_args)

    @pytest.mark.asyncio
    async def test_remove_server(self, cluster: ServerCluster) -> None:
        initial_servers = await cluster.get_servers()
        assert len(initial_servers) == 2

        removed = await cluster.remove_server(host="127.0.0.1", port=8001)

        assert removed is True
        assert await cluster.get_server_count() == 1
        remaining_servers = await cluster.get_servers()
        assert remaining_servers[0] is initial_servers[1]
        cast(Any, initial_servers[0].close).assert_awaited_once()

        removed_again = await cluster.remove_server(host="127.0.0.1", port=9999)

        assert removed_again is False
        assert await cluster.get_server_count() == 1

    @pytest.mark.asyncio
    async def test_serve_forever(self, cluster: ServerCluster) -> None:
        await cluster.start_all()
        serve_task = asyncio.create_task(cluster.serve_forever())

        await asyncio.sleep(0.01)
        assert not serve_task.done()

        cluster._shutdown_event.set()

        await serve_task
        assert serve_task.done()

    @pytest.mark.asyncio
    async def test_serve_forever_cancellation(self, cluster: ServerCluster, caplog: pytest.LogCaptureFixture) -> None:
        await cluster.start_all()
        serve_task = asyncio.create_task(cluster.serve_forever())
        await asyncio.sleep(0.01)

        with caplog.at_level(logging.INFO):
            serve_task.cancel()
            try:
                await serve_task
            except asyncio.CancelledError:
                pass

        assert "serve_forever cancelled" in caplog.text

    @pytest.mark.asyncio
    async def test_serve_forever_not_activated(self, server_configs: list[ServerConfig]) -> None:
        cluster = ServerCluster(configs=server_configs)
        with pytest.raises(ServerError, match="Cluster not activated"):
            await cluster.serve_forever()

    @pytest.mark.asyncio
    async def test_serve_forever_not_running(self, cluster: ServerCluster) -> None:
        await cluster.stop_all()
        with pytest.raises(ServerError, match="Cluster is not running"):
            await cluster.serve_forever()

    @pytest.mark.asyncio
    async def test_serve_forever_wait_exception(self, cluster: ServerCluster, mocker: MockerFixture) -> None:
        await cluster.start_all()
        mocker.patch.object(cluster._shutdown_event, "wait", side_effect=ValueError("Unexpected error"))
        mock_logger = mocker.patch("pywebtransport.server.cluster.logger")

        await cluster.serve_forever()

        mock_logger.error.assert_called_with("Error during serve_forever wait: %s", mocker.ANY)

    @pytest.mark.asyncio
    async def test_start_all_failure(
        self, server_configs: list[ServerConfig], mock_webtransport_server_class: Any, mocker: MockerFixture
    ) -> None:
        mock_server_fail = mocker.create_autospec(WebTransportServer, instance=True)
        mock_server_fail.__aenter__ = mocker.AsyncMock(return_value=mock_server_fail)
        mock_server_fail.listen = mocker.AsyncMock(side_effect=ValueError("Listen failed"))
        mock_server_fail.close = mocker.AsyncMock()

        mock_server_ok = mocker.create_autospec(WebTransportServer, instance=True)
        mock_server_ok.__aenter__ = mocker.AsyncMock(return_value=mock_server_ok)
        mock_server_ok.listen = mocker.AsyncMock()
        mock_server_ok.close = mocker.AsyncMock()

        mock_webtransport_server_class.side_effect = [mock_server_fail, mock_server_ok]
        cluster = ServerCluster(configs=server_configs)
        cluster._lock = asyncio.Lock()

        await cluster.start_all()

        assert cluster.is_running
        assert len(cluster._servers) == 1
        assert cluster._servers[0] is mock_server_ok
        cast(Any, mock_server_fail.close).assert_awaited()

    @pytest.mark.asyncio
    async def test_start_all_failure_during_cleanup(
        self, server_configs: list[ServerConfig], mock_webtransport_server_class: Any, mocker: MockerFixture
    ) -> None:
        mock_server_fail = mocker.create_autospec(WebTransportServer, instance=True)
        mock_server_fail.__aenter__ = mocker.AsyncMock(return_value=mock_server_fail)
        mock_server_fail.listen = mocker.AsyncMock(side_effect=ValueError("Listen failed"))
        mock_server_fail.close = mocker.AsyncMock(side_effect=IOError("Cleanup failed"))

        mock_server_ok = mocker.create_autospec(WebTransportServer, instance=True)
        mock_server_ok.__aenter__ = mocker.AsyncMock(return_value=mock_server_ok)
        mock_server_ok.listen = mocker.AsyncMock()
        mock_server_ok.close = mocker.AsyncMock()

        mock_webtransport_server_class.side_effect = [mock_server_fail, mock_server_ok]
        cluster = ServerCluster(configs=server_configs)
        cluster._lock = asyncio.Lock()

        await cluster.start_all()

        assert cluster.is_running
        assert len(cluster._servers) == 1

    @pytest.mark.asyncio
    async def test_start_and_stop_all(self, cluster: ServerCluster) -> None:
        assert cluster.is_running
        assert len(cluster._servers) == 2
        for server in cluster._servers:
            cast(Any, server.listen).assert_awaited_once()

        servers_to_stop = await cluster.get_servers()
        await cluster.stop_all()

        for server in servers_to_stop:
            cast(Any, server.close).assert_awaited_once()

        assert await cluster.get_server_count() == 0
        assert not cluster.is_running

    @pytest.mark.asyncio
    async def test_start_and_stop_idempotency(self, cluster: ServerCluster) -> None:
        assert cluster.is_running

        await cluster.start_all()
        assert len(cluster._servers) == 2

        await cluster.stop_all()
        assert not cluster._servers

        await cluster.stop_all()
        assert not cluster._servers

    @pytest.mark.asyncio
    async def test_stop_all_failure(self, cluster: ServerCluster) -> None:
        server_to_fail = cluster._servers[0]
        cast(Any, server_to_fail.close).side_effect = ValueError("Close failed")

        with pytest.raises(ExceptionGroup):
            await cluster.stop_all()

        assert not cluster.is_running
        assert not cluster._servers

    @pytest.mark.asyncio
    async def test_stop_all_with_no_servers(self, cluster: ServerCluster) -> None:
        await cluster.stop_all()
        assert not cluster.is_running

        await cluster.stop_all()
        assert not cluster.is_running
