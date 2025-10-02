"""
Decorators module for PerformansKafka.

This module provides decorators for Kafka consumers and producers.
"""
import asyncio
import functools
import inspect
from typing import Callable, Any, Optional, Dict

from .registry import registry


def consumer(topic: str, 
            group_id: str, 
            **kafka_config) -> Callable:
    """
    Decorator to register a function as a Kafka consumer.
    
    The decorated function will be registered in the global registry
    and will be called when messages are received from the specified topic.
    
    Args:
        topic: The Kafka topic to consume from
        group_id: The consumer group ID
        **kafka_config: Additional configuration for the AIOKafkaConsumer
        
    Returns:
        The decorated function
        
    Raises:
        TypeError: If the decorated function is not an async function
    """
    def decorator(func: Callable) -> Callable:
        if not asyncio.iscoroutinefunction(func):
            raise TypeError(
                f"@consumer decorator can only be applied to async functions. "
                f"{func.__name__} is not an async function."
            )
        
        # Register the consumer with the global registry
        registry.register_consumer(func, topic, group_id, **kafka_config)
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            # The decorated function will be called directly by the consumer loop
            return await func(*args, **kwargs)
            
        return wrapper
    
    return decorator


def producer(topic: str, 
            key: Optional[str] = None,
            **kafka_config) -> Callable:
    """
    Decorator to register a function as a Kafka producer.
    
    The decorated function's return value will be sent to the specified topic.
    
    Args:
        topic: The Kafka topic to produce to
        key: Optional key to use for all messages produced by this function
        **kafka_config: Additional configuration for the AIOKafkaProducer
        
    Returns:
        The decorated function
    """
    def decorator(func: Callable) -> Callable:
        # Register the producer topic with the global registry
        registry.register_producer(topic, **kafka_config)
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            # Call the original function
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            # Ensure the Kafka service is started
            if not registry.started or not registry.producer_client:
                raise RuntimeError(
                    "KafkaService must be started before producing messages. "
                    "Please ensure you've called KafkaService.start()"
                )
            
            # Send the result to Kafka
            message_key = key
            # If result is a dict and contains a specific 'key' field, use it
            if isinstance(result, dict) and 'key' in result:
                message_key = result.pop('key')
                message_value = result
            else:
                message_value = result
                
            # Send the message asynchronously
            await registry.producer_client.send(
                topic, 
                value=message_value, 
                key=message_key.encode('utf-8') if message_key else None
            )
            
            # Return the original result
            return result
            
        return wrapper
    
    return decorator