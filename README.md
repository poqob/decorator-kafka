# Decorator Kafka

A minimalist, modern, and high-performance Python package that simplifies Kafka producer and consumer implementation using function decorators.

## Features

- **Decorator-based API**: Easily mark functions as Kafka consumers or producers using simple decorators
- **Async-first**: Built on `aiokafka` to leverage asyncio for superior network I/O performance
- **Centralized configuration**: Single registry for all Kafka configurations
- **Shared connection**: Reuses a single Kafka producer for all decorated producer functions
- **Minimal boilerplate**: Focus on your business logic, not Kafka plumbing

## Installation

```bash
pip install .
# or
pip install decorator-kafka
```

## Quick Start

```python
import asyncio
from decorator_kafka import consumer, producer, KafkaService

# Define a consumer
@consumer(topic="my-topic", group_id="my-group")
async def handle_message(message):
    print(f"Received: {message}")
    await process_message(message)

# Define a producer
@producer(topic="another-topic")
async def send_data(data):
    # The return value will be sent to Kafka
    return {
        "data": data,
        "timestamp": asyncio.get_event_loop().time()
    }

# Start the service
async def main():
    # Initialize and start the Kafka service
    service = KafkaService(bootstrap_servers="localhost:9092")
    await service.start()
    
    try:
        # Your application code here
        await send_data("Hello, Kafka!")
        
        # Keep the service running
        while True:
            await asyncio.sleep(1)
    finally:
        await service.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

## Full Example

See the [examples/simple_app.py](examples/simple_app.py) for a complete working example.

## License

MIT
