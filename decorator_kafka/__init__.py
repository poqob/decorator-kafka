"""
PerformansKafka: High-Performance Decorator-Based Kafka Library

A minimalist, modern, and high-performance Python package that simplifies
Kafka producer and consumer implementation using function decorators.
"""

# Import and expose the public API
from .decorators import consumer, producer
from .core import KafkaService
from .registry import registry

__all__ = ['consumer', 'producer', 'KafkaService', 'registry']

__version__ = '0.1.0'