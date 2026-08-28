

import asyncio
import functools
import time
from typing import Any, Callable, Dict, Optional, Tuple, Type, TypeVar, Union

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode



F = TypeVar('F', bound=Callable[..., Any])
T = TypeVar('T')

logger = None


def _get_logger():
    """Get logger instance lazily."""
    global logger
    if logger is None:
        from medalflow.logging import get_logger
        logger = get_logger(__name__)
    return logger


def traced(
    span_name: Optional[str] = None,
    *,
    kind: SpanKind = SpanKind.INTERNAL,
    attributes: Optional[Dict[str, Any]] = None,
    attribute_getter: Optional[Callable[..., Optional[Dict[str, Any]]]] = None,
) -> Callable[[F], F]:
    """Instrument a function with an OpenTelemetry span.

    Args:
        span_name: Optional explicit span name. Defaults to module-qualified function name.
        kind: Span kind, defaults to INTERNAL.
        attributes: Static span attributes to attach.
        attribute_getter: Callable returning additional attributes at call time.
    """

    def decorator(func: F) -> F:
        is_coroutine = asyncio.iscoroutinefunction(func)

        def _collect_attributes(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Dict[str, Any]:
            collected: Dict[str, Any] = {}
            if attributes:
                collected.update({k: v for k, v in attributes.items() if v is not None})

            if attribute_getter:
                try:
                    dynamic_attrs = attribute_getter(*args, **kwargs)
                except Exception as exc:  # pragma: no cover - defensive
                    _get_logger().warning("trace attribute getter failed: %s", exc)
                    dynamic_attrs = None

                if dynamic_attrs:
                    collected.update({k: v for k, v in dynamic_attrs.items() if v is not None})

            return collected

        if is_coroutine:

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                tracer = trace.get_tracer(func.__module__)
                name = span_name or f"{func.__module__}.{func.__qualname__}"

                with tracer.start_as_current_span(name, kind=kind) as span:
                    for key, value in _collect_attributes(args, kwargs).items():
                        span.set_attribute(key, value)

                    try:
                        result = await func(*args, **kwargs)
                    except Exception as exc:
                        span.record_exception(exc)
                        span.set_status(Status(StatusCode.ERROR, str(exc)))
                        raise

                    return result

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            tracer = trace.get_tracer(func.__module__)
            name = span_name or f"{func.__module__}.{func.__qualname__}"

            with tracer.start_as_current_span(name, kind=kind) as span:
                for key, value in _collect_attributes(args, kwargs).items():
                    span.set_attribute(key, value)

                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    raise

        return sync_wrapper  # type: ignore[return-value]

    return decorator


def _should_retry_exception(
    exc: Exception,
    retry_on: Optional[Tuple[Type[Exception], ...]],
    retry_condition: Optional[Callable[[Exception], bool]],
) -> bool:
    """Decide whether an exception is eligible for another retry attempt.

    Args:
        exc: The exception raised by the decorated function.
        retry_on: Tuple of exception types to retry on. If None, retry on all.
        retry_condition: Optional predicate applied when the type check passes.

    Returns:
        True if the operation should be retried.
    """
    if retry_on is None:
        # Retry on any exception if retry_on not specified
        should_retry = True
    elif isinstance(exc, retry_on):
        should_retry = True
    else:
        should_retry = False

    # Apply custom retry condition if provided
    if should_retry and retry_condition:
        should_retry = retry_condition(exc)

    return should_retry


def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    retry_on: Optional[Tuple[Type[Exception], ...]] = None,
    retry_condition: Optional[Callable[[Exception], bool]] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for retrying operations with exponential backoff.
    
    This decorator automatically retries failed operations with an exponentially
    increasing delay between attempts. It works with both synchronous and
    asynchronous functions. The delay between retries follows the formula:
    delay = min(initial_delay * (exponential_base ** attempt), max_delay)

    Args:
        max_retries: Maximum number of retry attempts. Default is 3.
        initial_delay: Initial delay in seconds between retries. Default is 1.0.
        max_delay: Maximum delay in seconds (caps exponential growth). Default is 60.0.
        exponential_base: Base for exponential backoff calculation. Default is 2.0.
        retry_on: Tuple of exception types to retry on. If None, retries on all 
            exceptions. Use this to limit retries to specific error types like
            (ConnectionError, TimeoutError).
        retry_condition: Optional function that takes an exception and returns True
            if the operation should be retried. This allows for custom retry logic
            based on exception content.

    Returns:
        Decorator function that can be applied to both sync and async functions.

    Raises:
        The last exception encountered if all retry attempts fail.

    Example:
        Basic usage with default settings:
        >>> @retry_with_backoff()
        >>> def unreliable_operation():
        ...     # May fail occasionally
        ...     return fetch_data()
        
        Retry only on specific exceptions:
        >>> @retry_with_backoff(
        ...     max_retries=5,
        ...     retry_on=(ConnectionError, TimeoutError)
        ... )
        >>> async def fetch_data():
        ...     return await api_call()
        
        Custom retry condition based on exception content:
        >>> def should_retry(exc: Exception) -> bool:
        ...     return "temporary" in str(exc).lower()
        ... 
        >>> @retry_with_backoff(
        ...     retry_condition=should_retry,
        ...     initial_delay=2.0,
        ...     max_delay=120.0
        ... )
        >>> def database_operation():
        ...     return db.execute_query()
        
        Combining with other decorators:
        >>> @retry_with_backoff(max_retries=3)
        >>> async def complex_operation():
        ...     return await process_data()
    
    Notes:
        - The decorator automatically detects if the decorated function is async
        - Retry attempts are logged at WARNING level
        - Final failure is logged at ERROR level
        - Total attempts = max_retries + 1 (initial attempt + retries)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> T:
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    should_retry = _should_retry_exception(
                        e, retry_on, retry_condition
                    )
                    
                    if should_retry and attempt < max_retries:
                        _get_logger().warning(
                            f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay} seconds..."
                        )
                        await asyncio.sleep(delay)
                        delay = min(delay * exponential_base, max_delay)
                    else:
                        if attempt == max_retries:
                            _get_logger().error(
                                f"All {max_retries + 1} attempts failed for {func.__name__}"
                            )
                        raise

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
            
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> T:
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    should_retry = _should_retry_exception(
                        e, retry_on, retry_condition
                    )
                    
                    if should_retry and attempt < max_retries:
                        _get_logger().warning(
                            f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay} seconds..."
                        )
                        time.sleep(delay)
                        delay = min(delay * exponential_base, max_delay)
                    else:
                        if attempt == max_retries:
                            _get_logger().error(
                                f"All {max_retries + 1} attempts failed for {func.__name__}"
                            )
                        raise

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator
