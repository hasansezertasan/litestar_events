.. A 7-character "=" underline (the length of "Modules") is treated as a
   merge-conflict separator by ``git diff --check`` / ``check-merge-conflict``;
   keep this underline longer than the title to avoid the false positive.

Modules
=========

An overview of the packages that make up ``litestar_events``.
The API reference below is generated automatically from the source docstrings.

Package (``litestar_events``)
---------------------------------------

Shared emitter machinery, configuration, and the Litestar plugin.

.. automodule:: litestar_events

.. automodule:: litestar_events.config

.. automodule:: litestar_events.plugin

Backends (``litestar_events.contrib``)
---------------------------------------

Drop-in ``BaseEventEmitterBackend`` implementations, one subpackage per
transport. See each backend's docstring for its delivery semantics and
constructor arguments.

.. automodule:: litestar_events.contrib
