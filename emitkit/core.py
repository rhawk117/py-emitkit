from __future__ import annotations
import inspect
import abc
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Generic, Protocol, TypeVar, TypedDict

from collections.abc import Callable

from pydantic import BaseModel, ValidationError, Field
from emitkit.exceptions import (
    RegistryFrozenError,
    EventExistsError,
    EventNotRegisteredError,
)

E = TypeVar("E", bound=StrEnum)

SchemaType = type[BaseModel] | None


class SyncEventHandler(Protocol):
    def __call__(self, **kwargs: Any) -> Any: ...


class AsyncEventHandler(Protocol):
    async def __call__(self, **kwargs: Any) -> Any: ...


EventHandler = SyncEventHandler | AsyncEventHandler


class EventDict(TypedDict):
    name: str
    handler: EventHandler
    schema: type[BaseModel] | None
    is_async: bool


class EventRegister(Generic[E]):
    """
    Abstract base class for event registries, provides an easy to
    use interface for registering and unregistering event handlers
    in a memory efficient manner with guaranteed immutability after
    `build()` is called and passed to an `EventEmitterAdapter`.
    """

    __slots__ = (
        "__registry",
        "__frozen",
    )

    def __init__(self) -> None:
        self.__registry: dict[E, EventDict] = {}
        self.__frozen: bool = False

    def _register(
        self,
        event: E,
        handler: EventHandler,
        *,
        schema: type[BaseModel] | None = None,
        is_async: bool = False,
        override: bool = False,
    ) -> None:
        if self.__frozen:
            raise RegistryFrozenError()

        if not override and event in self.__registry:
            raise EventExistsError(event=event)

        self.__registry[event] = {
            "name": event.value,
            "handler": handler,
            "schema": schema,
            "is_async": is_async,
        }

    def contains(self, event_name: E, *, schema: type[BaseModel] | None = None) -> bool:
        if not (event := self.__registry.get(event_name)):
            return False

        return event["schema"] is schema

    def initialize(self) -> MappingProxyType[E, EventDict]:
        reg = MappingProxyType(dict(self.__registry))
        self.__registry.clear()
        self.__frozen = True
        return reg

    @property
    def is_frozen(self) -> bool:
        return self.__frozen

    def has_event_name(self, event: E) -> bool:
        return event in self.__registry

    @property
    def registered(self) -> list[str]:
        if self.__frozen:
            raise RegistryFrozenError()
        return list(self.__registry.keys())

    def require(self, *events: E) -> None:
        missing = [e for e in events if e not in self.__registry]
        if missing:
            raise EventNotRegisteredError(
                event=", ".join(missing),
                registered_events=list(self.__registry.keys()),
            )

    def off(self, event: E) -> None:
        if self.__frozen:
            raise RegistryFrozenError()
        self.__registry.pop(event, None)

    def on_sync(
        self, event: E, *, schema: type[BaseModel] | None = None, override: bool = False
    ):
        def decorator(func: EventHandler) -> EventHandler:
            if inspect.iscoroutinefunction(func):
                raise ValueError(
                    f"Async function {func.__name__} cannot be registered with 'on'. Use 'on_async' instead."
                )

            self._register(
                event,
                func,
                schema=schema,
                is_async=False,
                override=override,
            )
            return func

        return decorator

    def on_async(
        self, event: E, *, schema: type[BaseModel] | None = None, override: bool = False
    ):
        def decorator(func: EventHandler):
            if not inspect.iscoroutinefunction(func):
                raise ValueError(
                    f"Function {func.__name__} must be async to be registered with 'on_async'."
                )
            self._register(
                event,
                func,
                schema=schema,
                is_async=True,
                override=override,
            )
            return func

        return decorator

    def on(
        self, event: E, *, schema: type[BaseModel] | None = None, override: bool = False
    ) -> Callable[[EventHandler], EventHandler]:
        def decorator(func: EventHandler) -> EventHandler:
            is_async = inspect.iscoroutinefunction(func)
            self._register(
                event,
                func,
                schema=schema,
                is_async=is_async,
                override=override,
            )
            return func

        return decorator


class EventEmitter(Generic[E], abc.ABC):
    __slots__ = ("__registrar",)

    def __init__(self, event_mappings: MappingProxyType[E, EventDict]) -> None:
        self.__registrar: MappingProxyType[E, EventDict] = event_mappings

    def __get_event(self, event_name: E) -> EventDict:
        try:
            return self.__registrar[event_name]
        except KeyError:
            raise EventNotRegisteredError(
                event=event_name,
                registered_events=list(self.__registrar.keys()),
            )

    def __check_event_schema(self, event_dict: EventDict, **kwargs: Any) -> dict | None:
        if not (event_schema := event_dict["schema"]):
            return None

        try:
            arguments = event_schema(**kwargs)
        except ValidationError as exc:
            raise RuntimeValidationError(exc)

        return {
            **kwargs,
            **arguments.model_dump()
        }

    def emit(
        self,
        event_name: E,
        /,
        **kwargs: Any,
    ) -> Any:
        event = self.__get_event(event_name)
        if event["is_async"]:
            raise TypeError(
                f"Event '{event_name}' is async; use 'emit_async' to emit it."
            )

        handler = event["handler"]
        if updated_kwargs := self.__check_event_schema(event, **kwargs):
            return handler(**updated_kwargs)

        return handler(**kwargs)

    async def emit_async(
        self,
        event_name: E,
        /,
        **kwargs: Any,
    ) -> Any:
        event = self.__get_event(event_name)
        if not event["is_async"]:
            raise TypeError(
                f"Event '{event_name}' is not async; use 'emit' to emit it."
            )
        handler = event["handler"]
        if updated_kwargs := self.__check_event_schema(event, **kwargs):
            return await handler(**updated_kwargs)

        return await handler(**kwargs)
