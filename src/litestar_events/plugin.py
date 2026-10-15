"""Litestar plugin wiring the emitter backend and the ``events`` CLI group."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from litestar.exceptions import ImproperlyConfiguredException
from litestar.plugins import CLIPlugin, InitPluginProtocol

from litestar_events._queue import RoleAwareEmitter
from litestar_events.codec import JSONCodec, listener_name
from litestar_events.config import EventsConfig

if TYPE_CHECKING:
    from collections.abc import Sequence

    from click import Group
    from litestar.config.app import AppConfig
    from litestar.events import BaseEventEmitterBackend, EventListener

__all__ = ["EventsPlugin"]


class EventsPlugin(InitPluginProtocol, CLIPlugin):
    """Install an emitter backend and add the ``litestar events`` commands.

    Use the same plugin and config in the web app and the worker. The worker
    runs the same Litestar app through ``litestar events run``, which
    always consumes only; set ``EventsConfig(role="producer")`` so the web
    process publishes only.

    Plugin order matters for the request id: with ``request_id_header`` set,
    the middleware goes first in the app's list when this plugin initializes,
    so list ``EventsPlugin`` last in ``plugins=[...]`` to keep it outermost.
    """

    __slots__ = ("config",)

    def __init__(self, config: EventsConfig | None = None) -> None:
        self.config = config or EventsConfig()

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        if self.config.request_id_header:
            from litestar_events.middleware import RequestIdMiddleware

            # First in the app's list, so it wraps the middleware registered
            # on the app and on lower layers. A plugin that runs later may
            # still insert ahead of it.
            app_config.middleware.insert(
                0,
                RequestIdMiddleware(
                    header=self.config.request_id_header,
                    echo=self.config.echo_request_id,
                    exclude_path_pattern=self.config.request_id_exclude_path_pattern,
                ),
            )
        local = tuple(self.config.local_listeners)
        self._validate_local(local, app_config)
        if self.config.backend is None and not local:
            return app_config
        backend = self.config.backend or app_config.event_emitter_backend
        options: dict[str, Any] = dict(self.config.backend_options)
        if self.config.backend is not None:
            self._reject_shadowed_options(options)
            options["codec"] = self.config.codec or JSONCodec(
                typed=self.config.typed_decoding
            )
            if self.config.role is not None:
                options["role"] = self.config.role

        # Litestar builds the emitter as ``backend(listeners=...)`` with no way
        # to pass options or configure it afterwards, so install a subclass
        # whose constructor supplies them. It stays a class, and an instance of
        # ``backend``, for anything that inspects ``event_emitter_backend``.
        def init(
            self: BaseEventEmitterBackend, listeners: Sequence[EventListener]
        ) -> None:
            backend.__init__(self, listeners=listeners, **options)  # pylint: disable=unnecessary-dunder-call  # type() subclass: no class body for super()
            if local:
                if not isinstance(self, RoleAwareEmitter):
                    msg = f"{backend.__name__} does not support local_listeners"
                    raise ImproperlyConfiguredException(msg)
                self.set_local_listeners(local)

        configured = type(
            backend.__name__,
            (backend,),
            {"__init__": init, "__module__": backend.__module__},
        )
        app_config.event_emitter_backend = configured
        return app_config

    def _reject_shadowed_options(self, options: dict[str, Any]) -> None:
        # ``codec`` and ``role`` are set from the config; the same key in
        # ``backend_options`` would be silently overwritten.
        clash = sorted({"codec", "role"} & options.keys())
        if clash:
            msg = (
                f"backend_options must not contain {', '.join(clash)}; "
                "set them on EventsConfig instead"
            )
            raise ImproperlyConfiguredException(msg)

    @staticmethod
    def _validate_local(local: Sequence[EventListener], app_config: AppConfig) -> None:
        for listener in local:
            if listener not in app_config.listeners:
                name = listener_name(listener.fn)
                msg = (
                    f"local_listeners contains {name!r}, which is not in the app's "
                    "listeners; register it with Litestar(listeners=[...])"
                )
                raise ImproperlyConfiguredException(msg)

    def on_cli_init(self, cli: Group) -> None:
        from litestar_events._cli import events_group

        cli.add_command(events_group)
