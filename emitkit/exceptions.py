class EventExistsError(Exception):
    def __init__(self, event: str) -> None:
        super().__init__(f"Event {event!s} already registered, and override is False")


class RegistryFrozenError(Exception):
    def __init__(self) -> None:
        super().__init__("Registry is frozen; cannot mutate EventRegistry")


class EventNotRegisteredError(Exception):
    def __init__(self, event: str, registered_events: list[str]) -> None:
        super().__init__(
            f"Cannot access event {event!s} when its not registered, "
            "registered events: "
            f"{', '.join(registered_events)}",
        )
