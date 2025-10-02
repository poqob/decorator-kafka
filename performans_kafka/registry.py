"""
Registry module for PerformansKafka.

This module defines the KafkaRegistry class which serves as a central storage
for all consumer and producer configurations.
"""
from typing import Dict, List, Any, Callable, Optional, Set
import asyncio


class KafkaRegistry:
    """
    Central registry for Kafka consumer and producer configurations.
    
    Stores all decorated functions along with their Kafka configurations
    and manages a single, shared AIOKafkaProducer client.
    """
    
    def __init__(self):
        """Initialize an empty registry."""
        self._consumers: List[Dict[str, Any]] = []
        self._producers: Set[str] = set()
        self._producer_configs: Dict[str, Any] = {}
        self._bootstrap_servers: Optional[str] = None
        self._producer_client = None
        self._started = False
    
    @property
    def consumers(self) -> List[Dict[str, Any]]:
        """Get the list of registered consumers."""
        return self._consumers
    
    @property
    def producers(self) -> Set[str]:
        """Get the set of producer topics."""
        return self._producers
    
    @property
    def producer_configs(self) -> Dict[str, Any]:
        """Get producer configurations."""
        return self._producer_configs
    
    @property
    def bootstrap_servers(self) -> Optional[str]:
        """Get the bootstrap servers configuration."""
        return self._bootstrap_servers
    
    @bootstrap_servers.setter
    def bootstrap_servers(self, servers: str):
        """Set the bootstrap servers configuration."""
        self._bootstrap_servers = servers
    
    @property
    def producer_client(self):
        """Get the AIOKafkaProducer client."""
        return self._producer_client
    
    @producer_client.setter
    def producer_client(self, client):
        """Set the AIOKafkaProducer client."""
        self._producer_client = client
    
    @property
    def started(self) -> bool:
        """Check if the Kafka service is started."""
        return self._started
    
    @started.setter
    def started(self, value: bool):
        """Set the started status."""
        self._started = value
    
    def register_consumer(self, 
                         func: Callable, 
                         topic: str, 
                         group_id: str, 
                         **kafka_config) -> None:
        """
        Register a consumer function with its Kafka configuration.
        
        Args:
            func: The async function to be called when a message is received
            topic: The Kafka topic to consume from
            group_id: The consumer group ID
            **kafka_config: Additional Kafka consumer configuration
        """
        consumer_config = {
            'func': func,
            'topic': topic,
            'group_id': group_id,
            **kafka_config
        }
        self._consumers.append(consumer_config)
    
    def register_producer(self, 
                         topic: str, 
                         **kafka_config) -> None:
        """
        Register a producer topic with its Kafka configuration.
        
        Args:
            topic: The Kafka topic to produce to
            **kafka_config: Additional Kafka producer configuration
        """
        self._producers.add(topic)
        # Store any topic-specific producer configs if needed
        if kafka_config:
            self._producer_configs[topic] = kafka_config


# Global registry instance
registry = KafkaRegistry()