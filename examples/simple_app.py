"""
Example application demonstrating the use of PerformansKafka.

This example shows how to define decorated functions for producing and consuming
Kafka messages, and how to start the KafkaService.
"""
import asyncio
import logging
import os
from typing import Dict, Any

from decorator_kafka import consumer, producer, KafkaService


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Define topics
USER_CREATED_TOPIC = "user-created"
USER_NOTIFIED_TOPIC = "user-notified"


# Consumer for user-created events
@consumer(topic=USER_CREATED_TOPIC, group_id="user-notification-service")
async def handle_user_created(message: Dict[str, Any]) -> None:
    """
    Handle user created events.
    
    Args:
        message: The message from Kafka
    """
    logger.info(f"Received user created event: {message}")
    
    # Process the user and send a notification
    user_id = message.get('id')
    email = message.get('email')
    
    if user_id and email:
        # Create notification message
        notification = {
            'user_id': user_id,
            'email': email,
            'template': 'welcome',
            'sent_at': asyncio.get_event_loop().time()
        }
        
        # Send notification (this will be produced to Kafka)
        await send_notification(notification)
        
        logger.info(f"Sent welcome notification to user {user_id}")
    else:
        logger.warning(f"Invalid user created event: {message}")


# Producer for user-notified events
@producer(topic=USER_NOTIFIED_TOPIC)
async def send_notification(notification: Dict[str, Any]) -> Dict[str, Any]:
    """
    Send a notification for a user.
    
    Args:
        notification: The notification data
        
    Returns:
        The notification data, which will be produced to Kafka
    """
    # You could perform additional processing here
    # The return value will be automatically sent to the USER_NOTIFIED_TOPIC
    return notification


# Consumer for user-notified events
@consumer(topic=USER_NOTIFIED_TOPIC, group_id="notification-tracking-service")
async def handle_user_notification(notification: Dict[str, Any]) -> None:
    """
    Handle user notification events.
    
    Args:
        notification: The notification data from Kafka
    """
    user_id = notification.get('user_id')
    email = notification.get('email')
    template = notification.get('template')
    sent_at = notification.get('sent_at')
    
    logger.info(f"Notification tracker received: User {user_id} ({email}) "
                f"was sent a {template} notification at {sent_at}")
    
    # In a real application, you might:
    # - Track notification deliveries in a database
    # - Send analytics events
    # - Trigger follow-up actions based on notification type
    # - Update notification status metrics


# Mock function to simulate creating users (in a real app, this might be an API endpoint)
@producer(topic=USER_CREATED_TOPIC)
async def create_user(username: str, email: str) -> Dict[str, Any]:
    """
    Create a new user.
    
    Args:
        username: The username
        email: The email address
        
    Returns:
        The user data, which will be produced to Kafka
    """
    user = {
        'id': f"user_{asyncio.get_event_loop().time()}",
        'username': username,
        'email': email,
        'created_at': asyncio.get_event_loop().time()
    }
    logger.info(f"Created user: {user}")
    return user


async def main() -> None:
    """Run the example application."""
    # Get bootstrap servers from environment or use default
    host = os.environ.get('KAFKA_HOST', 'localhost')
    port = os.environ.get('KAFKA_PORT', '29092')
    bootstrap_servers = f"{host}:{port}"

    # Create and start the KafkaService
    service = KafkaService(bootstrap_servers)
    await service.start()
    
    try:
        # Create some example users
        await create_user("alice", "alice@example.com")
        await asyncio.sleep(1)
        await create_user("bob", "bob@example.com")
        await asyncio.sleep(1)
        await create_user("charlie", "charlie@example.com")
        
        # Keep the service running for a while to process messages
        logger.info("Service running, press Ctrl+C to stop...")
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Stopping service...")
    finally:
        # Stop the KafkaService
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())