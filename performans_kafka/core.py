"""
Core module for PerformansKafka.

This module defines the KafkaService class which manages the Kafka connections
and processes messages.
"""
import asyncio
import logging
from typing import Optional, Dict, Any, List
import json

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError

from .registry import registry


logger = logging.getLogger(__name__)


class KafkaService:
    """
    Service that manages Kafka connections and message processing.
    
    This class is responsible for initializing the shared AIOKafkaProducer client
    and launching consumer tasks for each registered consumer.
    """
    
    def __init__(self, bootstrap_servers: str):
        """
        Initialize the KafkaService.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka bootstrap servers
        """
        self.bootstrap_servers = bootstrap_servers
        registry.bootstrap_servers = bootstrap_servers
        self._consumer_tasks: List[asyncio.Task] = []
        
    async def start(self) -> None:
        """
        Start the Kafka service.
        
        This method initializes the AIOKafkaProducer and starts a consumer task
        for each registered consumer.
        """
        if registry.started:
            logger.warning("KafkaService already started")
            return
            
        # Initialize the AIOKafkaProducer
        producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await producer.start()
        registry.producer_client = producer
        
        # Start a consumer task for each registered consumer
        for consumer_config in registry.consumers:
            task = asyncio.create_task(
                self._run_consumer(consumer_config)
            )
            self._consumer_tasks.append(task)
            
        registry.started = True
        logger.info(f"KafkaService started with bootstrap servers: {self.bootstrap_servers}")
        logger.info(f"Started {len(self._consumer_tasks)} consumer tasks")
        
    async def _run_consumer(self, config: Dict[str, Any]) -> None:
        """
        Run a consumer loop for a registered consumer.
        
        Args:
            config: The consumer configuration from the registry
        """
        func = config.pop('func')
        topic = config.pop('topic')
        group_id = config.pop('group_id')
        
        # Create a consumer with the appropriate configuration
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            **config
        )
        
        try:
            await consumer.start()
            logger.info(f"Consumer started for topic {topic} with group_id {group_id}")
            
            # Process messages
            async for message in consumer:
                try:
                    # Pass the message value directly to the handler function
                    await func(message.value)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KafkaError as e:
            logger.error(f"Kafka error in consumer: {e}")
        finally:
            await consumer.stop()
            
    async def stop(self) -> None:
        """
        Stop the Kafka service.
        
        This method stops all consumer tasks and the producer.
        """
        if not registry.started:
            logger.warning("KafkaService not started")
            return
            
        # Cancel all consumer tasks
        for task in self._consumer_tasks:
            task.cancel()
            
        # Wait for all tasks to complete
        if self._consumer_tasks:
            try:
                await asyncio.gather(*self._consumer_tasks, return_exceptions=True)
            except asyncio.CancelledError:
                pass
                
        # Stop the producer
        if registry.producer_client:
            await registry.producer_client.stop()
            registry.producer_client = None
            
        registry.started = False
        self._consumer_tasks = []
        logger.info("KafkaService stopped")