from quickbelog import Log
from inspect import getfullargspec
from abc import ABC, abstractmethod

EVENT_TYPE_KEY = 'event-type'


class EventListener(ABC):

    def __init__(self, *args, **kwargs):
        self._run_switch = True

    @abstractmethod
    def fetch(self) -> list:
        pass

    @abstractmethod
    def event_handling_error(self, event: dict):
        pass

    @abstractmethod
    def event_handling_done(self, event: dict):
        pass

    def run(self):
        while self._run_switch:
            for event in self.fetch():
                event_type = event[EVENT_TYPE_KEY]
                try:
                    event_processing(event_type=event_type, event=event)
                except Exception as ex:
                    Log.exception(f'Processing event type {event_type} failed with {ex}')
                    self.event_handling_error(event=event)


EVENT_TYPE_HANDLERS = {}


def register_handler(event_type: str, func):
    global EVENT_TYPE_HANDLERS

    if is_valid_event_handler(func=func):
        if event_type in EVENT_TYPE_HANDLERS:
            EVENT_TYPE_HANDLERS.get(event_type).append(func)
        else:
            EVENT_TYPE_HANDLERS[event_type] = [func]


def is_valid_event_handler(func) -> bool:
    args_spec = getfullargspec(func=func)
    try:
        args_spec.annotations.pop('return')
    except KeyError:
        pass
    arg_types = args_spec.annotations.values()
    if len(arg_types) == 1 and issubclass(list(arg_types)[0], dict):
        return True
    else:
        raise TypeError(
            f'Function {func.__qualname__} needs one argument, type dict. Instead got the following spec: {args_spec}'
        )


def event_handler(event_type: str):
    def inner_func(func):
        register_handler(event_type=event_type, func=func)
        return func

    return inner_func


def event_processing(event_type: str, event: dict):
    handlers = EVENT_TYPE_HANDLERS.get(event_type, [])
    for e_handler in handlers:
        e_handler(event)

