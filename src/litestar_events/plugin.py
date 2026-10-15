"""Litestar plugin wiring the emitter backend and the ``events`` CLI group."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from litestar.exceptions import ImproperlyConfiguredException
from litestar.plugins import CLIPlugin, InitPluginProtocol

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
    """

    __slots__ = ("config",)

    def __init__(self, config: EventsConfig | None = None) -> None:
        self.config = config or EventsConfig()

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        if self.config.request_id_header:
            from litestar.middleware import DefineMiddleware

            from litestar_events.middleware import RequestIdMiddleware

            # Outermost, so every other middleware and handler already runs
            # with the request's correlation id.
            app_config.middleware.insert(
                0,
                DefineMiddleware(
                    RequestIdMiddleware,
                    header=self.config.request_id_header,
                    echo=self.config.echo_request_id,
                ),
            )
        local = tuple(self.config.local_listeners)
        self._validate_local(local, app_config)
        if self.config.backend is None and not local:
            return app_config
        backend = self.config.backend or app_config.event_emitter_backend
        options: dict[str, Any] = dict(self.config.backend_options)
        if self.config.backend is not None:
            options["codec"] = self.config.codec or JSONCodec(
                typed=self.config.typed_decoding
            )
            if self.config.role is not None:
                options["role"] = self.config.role

        # Litestar builds the emitter as ``backend(listeners=...)`` with no way
        # to pass options or configure it afterwards, so build it in a factory.
        def factory(listeners: Sequence[EventListener]) -> BaseEventEmitterBackend:
            emitter = backend(listeners=listeners, **options)
            if local:
                set_local = getattr(emitter, "set_local_listeners", None)
                if set_local is None:
                    msg = f"{type(emitter).__name__} does not support local_listeners"
                    raise ImproperlyConfiguredException(msg)
                set_local(local)
            return emitter

        app_config.event_emitter_backend = cast(
            "type[BaseEventEmitterBackend]", factory
        )
        return app_config

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
