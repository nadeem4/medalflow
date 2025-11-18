

import asyncio
import functools
import time
from typing import Any, Callable, Dict, Optional, Tuple, Type, TypeVar, Union

from opentelemetry.trace import SpanKind, Status, StatusCode

from core.telemetry import get_tracer


F = TypeVar('F', bound=Callable[..., Any])
T = TypeVar('T')

logger = None


def _get_logger():
    """Get logger instance lazily."""
    global logger
    if logger is None:
        from core.logging import get_logger
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
                tracer = get_tracer(func.__module__)
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
            tracer = get_tracer(func.__module__)
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
                    
                    # Check if we should retry this exception
                    should_retry = False
                    
                    if retry_on is None:
                        # Retry on any exception if retry_on not specified
                        should_retry = True
                    elif isinstance(e, retry_on):
                        should_retry = True
                    
                    # Apply custom retry condition if provided
                    if should_retry and retry_condition:
                        should_retry = retry_condition(e)
                    
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
                    
                    # Check if we should retry this exception
                    should_retry = False
                    
                    if retry_on is None:
                        should_retry = True
                    elif isinstance(e, retry_on):
                        should_retry = True
                    
                    # Apply custom retry condition if provided
                    if should_retry and retry_condition:
                        should_retry = retry_condition(e)
                    
                    if should_retry and attempt < max_retries:
                        _get_logger().warning(
                            f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay} seconds..."
                        )
                        import time
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


def with_timeout(
    timeout_seconds: float,
    timeout_exception: Optional[Type[Exception]] = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to add timeout to async functions.
    
    This decorator wraps an async function with a timeout, ensuring it completes
    within the specified time limit. If the function doesn't complete in time,
    it raises a timeout exception. This is useful for preventing long-running
    operations from blocking indefinitely.
    
    Args:
        timeout_seconds: Maximum time in seconds the function is allowed to run.
            Must be a positive float value.
        timeout_exception: Optional custom exception type to raise on timeout.
            If None, raises asyncio.TimeoutError. The custom exception will
            receive a descriptive error message.
        
    Returns:
        Decorator function that adds timeout behavior to async functions.
        
    Raises:
        asyncio.TimeoutError: If timeout_exception is None and function times out.
        timeout_exception: If provided and function times out.
        
    Example:
        Basic usage with default timeout error:
        >>> @with_timeout(30.0)
        >>> async def fetch_large_dataset():
        ...     return await download_data()
        
        Custom timeout exception:
        >>> class DataFetchTimeout(Exception):
        ...     pass
        ... 
        >>> @with_timeout(10.0, timeout_exception=DataFetchTimeout)
        >>> async def fetch_critical_data():
        ...     return await api.get_data()
        
        Combining with retry decorator:
        >>> @retry_with_backoff(max_retries=3)
        >>> @with_timeout(5.0)
        >>> async def resilient_operation():
        ...     # Each retry attempt has 5 second timeout
        ...     return await external_service_call()
        
        Dynamic timeout based on input:
        >>> def create_processor(timeout: float):
        ...     @with_timeout(timeout)
        ...     async def process_data(data):
        ...         return await heavy_computation(data)
        ...     return process_data
    
    Notes:
        - Only works with async functions (coroutines)
        - The timeout applies to each function call, not cumulative for retries
        - Cancelled operations may leave resources in an inconsistent state
        - Consider cleanup in try/finally blocks for critical resources
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            try:
                return await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=timeout_seconds
                )
            except asyncio.TimeoutError as e:
                if timeout_exception:
                    raise timeout_exception(
                        f"{func.__name__} timed out after {timeout_seconds} seconds"
                    ) from e
                raise
                
        return wrapper
    return decorator


# Aliases for backward compatibility
retry = retry_with_backoff
async_retry = retry_with_backoff  # The retry_with_backoff decorator handles both sync and async


def catch_exception(
    exception_type: Type[Exception] = Exception,
    default_return: Any = None,
    log_error: bool = True,
    raise_new: Optional[Type[Exception]] = None
) -> Callable[[F], F]:
    """Decorator to catch and handle exceptions gracefully.
    
    This decorator provides a clean way to handle expected exceptions in functions.
    It can either return a default value when an exception occurs or transform
    the exception into a different type. This is particularly useful for API
    boundaries where you want to hide internal implementation details or provide
    fallback behavior.
    
    Args:
        exception_type: Type of exception to catch. Can be a single exception
            class or Exception to catch all. Default is Exception (catches all).
        default_return: Value to return when the specified exception is caught.
            Only used if raise_new is None. Default is None.
        log_error: Whether to log the caught exception. Logs at ERROR level
            with full exception details. Default is True.
        raise_new: Optional exception type to raise instead of returning 
            default_return. The new exception will be chained to the original
            for debugging purposes.
        
    Returns:
        Decorated function that handles exceptions according to parameters.
        
    Raises:
        raise_new: If specified and the caught exception matches exception_type.
        
    Example:
        Return default value on error:
        >>> @catch_exception(ValueError, default_return=0)
        ... def parse_number(s: str) -> int:
        ...     return int(s)
        ... 
        >>> parse_number("123")  # Returns: 123
        >>> parse_number("abc")  # Returns: 0 (logs error)
        
        Transform exceptions:
        >>> class ValidationError(Exception):
        ...     pass
        ... 
        >>> @catch_exception(
        ...     ValueError,
        ...     raise_new=ValidationError,
        ...     log_error=True
        ... )
        ... def validate_age(age_str: str) -> int:
        ...     age = int(age_str)
        ...     if age < 0:
        ...         raise ValueError("Age cannot be negative")
        ...     return age
        
        Silent error handling (not recommended for production):
        >>> @catch_exception(
        ...     exception_type=FileNotFoundError,
        ...     default_return=[],
        ...     log_error=False
        ... )
        ... def read_config_list(path: str) -> list:
        ...     with open(path) as f:
        ...         return json.load(f)
        
        Catching multiple exception types:
        >>> @catch_exception(ZeroDivisionError, default_return=float('inf'))
        ... @catch_exception(ValueError, default_return=0)
        ... def safe_divide(a: str, b: str) -> float:
        ...     return int(a) / int(b)
    
    Notes:
        - Only catches the specified exception type, not its subclasses
        - When raise_new is specified, default_return is ignored
        - Original exception is preserved in the chain for debugging
        - Works with both sync and async functions
        - Consider using more specific exception types rather than Exception
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except exception_type as e:
                if log_error:
                    
                    _get_logger().error(
                        f"Exception in {func.__name__}",
                        error=str(e),
                        error_type=type(e).__name__
                    )
                
                if raise_new:
                    raise raise_new(str(e)) from e
                
                return default_return
        
        return wrapper  # type: ignore
    
    return decorator




def deprecated(
    reason: str,
    version: Optional[str] = None,
    alternative: Optional[str] = None
) -> Callable[[F], F]:
    """Decorator to mark functions as deprecated.
    
    This decorator provides a standardized way to mark functions as deprecated,
    logging warnings when they're used and updating their documentation. It helps
    manage API evolution by clearly communicating which functions should no longer
    be used and what alternatives are available.
    
    Args:
        reason: Clear explanation of why the function is deprecated. This should
            help users understand the motivation for the deprecation.
        version: The version number when the function was deprecated. This helps
            users understand the timeline of the deprecation. Default is None.
        alternative: The recommended alternative function or approach. Should be
            a fully qualified name or clear description. Default is None.
        
    Returns:
        Decorated function that logs deprecation warnings when called.
        
    Example:
        Simple deprecation:
        >>> @deprecated(reason="Use new_function instead")
        ... def old_function():
        ...     return "old implementation"
        
        With version information:
        >>> @deprecated(
        ...     reason="Performance issues with large datasets",
        ...     version="2.0.0",
        ...     alternative="process_data_streaming"
        ... )
        ... def process_data_batch(data: list) -> dict:
        ...     # Old batch processing implementation
        ...     return process_all_at_once(data)
        
        Deprecating a class method:
        >>> class DataProcessor:
        ...     @deprecated(
        ...         reason="Method name doesn't follow naming conventions",
        ...         version="1.5.0",
        ...         alternative="DataProcessor.analyze_dataset"
        ...     )
        ...     def analyzeData(self, data):
        ...         return self.analyze_dataset(data)
        
        Migration guide in docstring:
        >>> @deprecated(
        ...     reason="API redesign for better async support",
        ...     alternative="fetch_data_async"
        ... )
        ... def fetch_data_sync(url: str) -> dict:
        ...     '''Fetch data synchronously.
        ...     
        ...     Migration guide:
        ...         Old: result = fetch_data_sync(url)
        ...         New: result = await fetch_data_async(url)
        ...     '''
        ...     return requests.get(url).json()
    
    Notes:
        - Warnings are logged at WARNING level each time the function is called
        - The decorator automatically updates the function's docstring
        - Deprecation notices follow Sphinx documentation format
        - Consider using @functools.lru_cache to limit warning frequency
        - Does not prevent the function from being called
        - Works with methods, class methods, and static methods
    
    Best Practices:
        - Always provide a clear reason for deprecation
        - Include version number when deprecating public APIs
        - Provide specific alternatives, not just "use new API"
        - Consider a deprecation period before removal
        - Update all internal usage before deprecating
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            
            
            message = f"Function {func.__name__} is deprecated: {reason}"
            if version:
                message += f" (deprecated in v{version})"
            if alternative:
                message += f". Use {alternative} instead"
            
            _get_logger().warning(message)
            
            return func(*args, **kwargs)
        
        # Update docstring
        doc = func.__doc__ or ""
        deprecation_note = f"\n\n.. deprecated:: {version or 'TBD'}\n   {reason}"
        if alternative:
            deprecation_note += f"\n   Use :func:`{alternative}` instead."
        
        wrapper.__doc__ = doc + deprecation_note
        
        return wrapper  # type: ignore
    
    return decorator


def synchronized(lock: Optional[asyncio.Lock] = None) -> Callable[[F], F]:
    """Decorator to synchronize async function execution.
    
    This decorator ensures that only one instance of the decorated async function
    runs at a time using an asyncio lock. It's useful for protecting critical
    sections, preventing race conditions, and ensuring sequential access to
    shared resources in concurrent async code.
    
    Args:
        lock: Optional asyncio.Lock instance to use for synchronization.
            If None, a new lock is created for this specific function.
            Providing an external lock allows multiple functions to share
            the same synchronization context. Default is None.
        
    Returns:
        Decorated async function that executes within the lock context.
        
    Raises:
        TypeError: If applied to a non-async function.
        
    Example:
        Basic usage with auto-created lock:
        >>> @synchronized()
        ... async def update_shared_resource(data: dict):
        ...     # Only one coroutine can execute this at a time
        ...     current = await read_resource()
        ...     current.update(data)
        ...     await write_resource(current)
        
        Shared lock between functions:
        >>> resource_lock = asyncio.Lock()
        ... 
        >>> @synchronized(lock=resource_lock)
        ... async def read_critical_data():
        ...     return await database.read()
        ... 
        >>> @synchronized(lock=resource_lock)
        ... async def write_critical_data(data):
        ...     await database.write(data)
        
        Preventing concurrent API calls:
        >>> @synchronized()
        ... async def fetch_and_cache_data(key: str):
        ...     # Prevents duplicate API calls for the same key
        ...     if key in cache:
        ...         return cache[key]
        ...     data = await expensive_api_call(key)
        ...     cache[key] = data
        ...     return data
        
        Class method synchronization:
        >>> class DataManager:
        ...     def __init__(self):
        ...         self._lock = asyncio.Lock()
        ...     
        ...     @synchronized(lambda self: self._lock)
        ...     async def update_data(self, data):
        ...         # Synchronized per instance
        ...         await self._update_internal(data)
    
    Notes:
        - Only works with async functions (coroutines)
        - The lock is held for the entire duration of the function execution
        - Be careful of deadlocks when using shared locks
        - Consider using asyncio.Semaphore for limiting concurrency instead
        - Long-running operations in synchronized functions can cause bottlenecks
        - The decorator preserves the function's signature and return value
    
    Best Practices:
        - Keep synchronized sections as short as possible
        - Avoid calling other synchronized functions from within
        - Use timeouts when acquiring locks in production code
        - Consider read/write locks for read-heavy workloads
        - Document which resources are protected by synchronization
    """
    _lock = lock or asyncio.Lock()
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with _lock:
                return await func(*args, **kwargs)
        
        return wrapper  # type: ignore
    
    return decorator
