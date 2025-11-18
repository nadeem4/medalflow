"""Lazy loading descriptors.

This module provides base descriptors for implementing lazy loading patterns.
These can be extended for specific use cases like secrets, configuration values,
or expensive computations.
"""

from typing import Optional, Any, Dict, Callable, Generic, TypeVar

T = TypeVar('T')


class LazyField(Generic[T]):
    """Base descriptor for lazy-loaded values.
    
    This descriptor provides a generic lazy loading pattern that can be
    extended for specific use cases. Values are computed once per instance
    and cached for subsequent accesses.
    
    Attributes:
        loader: Function to compute the value when first accessed
        attr_name: The attribute name this descriptor is assigned to
        _cache: Dictionary mapping instance IDs to cached values
        
    Example:
        >>> class MyClass:
        >>>     @LazyField
        >>>     def expensive_property(self):
        >>>         # This computation only happens once per instance
        >>>         return perform_expensive_computation()
        >>>     
        >>> obj = MyClass()
        >>> value = obj.expensive_property  # Computed here
        >>> value2 = obj.expensive_property  # Retrieved from cache
    """
    
    def __init__(self, loader: Optional[Callable[[Any], T]] = None):
        """Initialize the lazy field descriptor.
        
        Args:
            loader: Optional function to compute the value. If not provided,
                   the descriptor can be used as a decorator.
        """
        self.loader = loader
        self.attr_name: Optional[str] = None
        self._cache: Dict[int, T] = {}
    
    def __set_name__(self, owner: type, name: str) -> None:
        """Store the attribute name when descriptor is attached to a class.
        
        Args:
            owner: The class that owns this descriptor
            name: The attribute name in the class
        """
        self.attr_name = name
        
    def __get__(self, obj: Optional[Any], objtype: Optional[type] = None) -> Any:
        """Get the lazy-loaded value.
        
        Args:
            obj: The instance, or None if accessed on class
            objtype: The type of the instance
            
        Returns:
            The computed/cached value, or the descriptor if accessed on class
        """
        if obj is None:
            return self  # Accessing via class
        
        obj_id = id(obj)
        
        # Check cache
        if obj_id not in self._cache:
            if self.loader:
                # Compute value using loader function
                self._cache[obj_id] = self.loader(obj)
            else:
                raise ValueError(
                    f"No loader function defined for LazyField '{self.attr_name}'"
                )
                
        return self._cache[obj_id]
    
    def __call__(self, loader: Callable[[Any], T]) -> 'LazyField[T]':
        """Allow LazyField to be used as a decorator.
        
        Args:
            loader: The function to use for loading the value
            
        Returns:
            Self with the loader set
            
        Example:
            >>> class MyClass:
            >>>     @LazyField
            >>>     def computed_value(self):
            >>>         return expensive_computation()
        """
        self.loader = loader
        return self
    
    def clear_cache(self, obj: Optional[Any] = None) -> None:
        """Clear cached values.
        
        Args:
            obj: If provided, clear only for this instance.
                 If None, clear all cached values.
        """
        if obj is not None:
            obj_id = id(obj)
            if obj_id in self._cache:
                del self._cache[obj_id]
        else:
            self._cache.clear()
    
    def invalidate(self, obj: Any) -> None:
        """Invalidate the cached value for a specific instance.
        
        Args:
            obj: The instance whose cached value should be invalidated
        """
        self.clear_cache(obj)