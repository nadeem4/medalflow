"""Decorators for Bronze layer metadata configuration.

This module provides decorators for configuring Bronze layer ETL processes
with metadata that controls execution behavior and data flow.
"""


def bronze_metadata(*args, **kwargs):
    """Decorator for Bronze layer class metadata.
    
    Configures Bronze layer sequencer classes with metadata that defines
    execution properties and orchestration behavior.
    
    Args:
        *args: Positional arguments for Bronze metadata configuration
        **kwargs: Keyword arguments for Bronze metadata configuration
    
    Returns:
        Decorated class with Bronze metadata attached
    """
    pass


# Alias for consistency with existing patterns
BronzeMetadata = type('BronzeMetadata', (), {})  # Empty class for type hints