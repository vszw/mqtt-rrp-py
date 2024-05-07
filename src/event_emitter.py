import asyncio
from typing import Dict, Set, Callable, TypeVar, Generic
from collections import defaultdict


T = TypeVar('T')


class EventEmitter(Generic[T]):
    listeners: Dict[str, Set[Callable[[T], None]]]
    
    def __init__(self):
        """
        Initializes a new instance of the EventEmitter class.

        Parameters:
            None

        Returns:
            None
        """
        self.listeners = defaultdict(set)
    
    def on(self, event: str, listener: Callable[[T], None]) -> None:
        """
        Registers a listener function to be called when the specified event is emitted.

        Args:
            event (str): The name of the event to listen for.
            listener (Callable[[T], None]): The function to be called when the event is emitted.
                The function should accept a single argument of type T.

        Returns:
            None
        """
        self.listeners[event].add(listener)
    
    def remove_listener(self, event: str, listener: Callable[[T], None]) -> None:
        """
        Removes a listener function from the specified event.

        Args:
            event (str): The name of the event.
            listener (Callable[[T], None]): The listener function to be removed.
                The function should accept a single argument of type T.

        Returns:
            None
        """
        self.listeners[event].remove(listener)
    
    def remove_all_listeners(self, event: str) -> None:
        """
        Removes all listeners for the specified event.

        Parameters:
            event (str): The name of the event.

        Returns:
            None
        """
        self.listeners.pop(event, None)
    
    def emit(self, event: str, payload: T) -> None:
        """
        Emits an event with the given payload to all registered listeners.

        Args:
            event (str): The name of the event to emit.
            payload (T): The payload to pass to the listeners.

        Returns:
            None
        """
        for listener in self.listeners[event]:
            asyncio.create_task(listener(payload))
    
    def once(self, event: str, listener: Callable[[T], None]) -> None:
        """
        Registers a listener function to be called only once when the specified event is emitted.

        Args:
            event (str): The name of the event to listen for.
            listener (Callable[[T], None]): The function to be called when the event is emitted.
                The function should accept a single argument of type T.

        Returns:
            None
        """
        def wrapper(payload: T) -> None:
            self.remove_listener(event, wrapper)
            asyncio.create_task(listener(payload))
        self.on(event, wrapper)
