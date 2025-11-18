"""Common pattern protocol definitions.

This module defines protocols for common design patterns that can be
implemented across various components in the MedalFlow framework.
"""

from typing import Protocol, Any, Optional, Callable, runtime_checkable
from datetime import datetime, timedelta


@runtime_checkable
class Cacheable(Protocol):
    """Protocol for objects that support caching.
    
    This protocol defines the interface for components that can
    cache their data or computation results.
    """
    
    def invalidate_cache(self) -> None:
        """Invalidate all cached data."""
        ...
    
    def is_cache_valid(self) -> bool:
        """Check if the current cache is valid.
        
        Returns:
            True if cache is valid, False otherwise
        """
        ...


@runtime_checkable
class Observable(Protocol):
    """Protocol for objects that support the observer pattern.
    
    This protocol defines the interface for components that can
    notify observers about state changes.
    """
    
    def attach(self, observer: Callable[[Any], None]) -> None:
        """Attach an observer to be notified of changes.
        
        Args:
            observer: Callable that will be invoked on changes
        """
        ...
    
    def detach(self, observer: Callable[[Any], None]) -> None:
        """Remove an observer.
        
        Args:
            observer: The observer to remove
        """
        ...
    
    def notify(self, event: Any) -> None:
        """Notify all observers of an event.
        
        Args:
            event: The event data to pass to observers
        """
        ...


@runtime_checkable
class Retryable(Protocol):
    """Protocol for operations that support retry logic.
    
    This protocol defines the interface for components that can
    retry operations on failure.
    """
    
    @property
    def max_retries(self) -> int:
        """Maximum number of retry attempts."""
        ...
    
    @property
    def retry_delay(self) -> timedelta:
        """Delay between retry attempts."""
        ...
    
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """Determine if an operation should be retried.
        
        Args:
            exception: The exception that occurred
            attempt: Current attempt number
            
        Returns:
            True if should retry, False otherwise
        """
        ...