# RabbitMQ Manager Library

A standalone Python library for RabbitMQ message broker operations. This library provides clean, object-oriented interfaces for RabbitMQ communication including message publishing, consumption, queue management, and health monitoring without external framework dependencies.

## Overview

The RabbitMQ Manager library provides functionality to:
- Connect to RabbitMQ message brokers
- Publish messages with priority and persistence
- Consume messages with acknowledgment
- Manage queues and exchanges
- Monitor connection health
- Handle messages with custom handlers

## Features

- **Standalone Implementation**: No external framework dependencies
- **Connection Management**: Automatic connection handling with retry logic
- **Message Publishing**: Publish messages with priority and persistence options
- **Message Consumption**: Consume messages with automatic acknowledgment
- **Queue & Exchange Management**: Declare and manage queues and exchanges
- **Health Monitoring**: Built-in health checks and connection monitoring
- **Message Handlers**: Extensible message handling system
- **Statistics Tracking**: Detailed message and connection statistics
- **Thread Safety**: Thread-safe operations for concurrent access
- **Context Manager Support**: Safe resource management with `with` statements

## Installation

### Prerequisites

```bash
# For RabbitMQ communication
pip install pika
```

### Usage

Simply copy the `connect_to_rabbit.py` file into your project and import it:

```python
from connect_to_rabbit import RabbitMQManager, QueueConfig, ExchangeConfig, MessagePriority, MessageHandler
```

## Quick Start

```python
from connect_to_rabbit import RabbitMQManager, QueueConfig, MessagePriority
import time

# Create RabbitMQ manager
manager = RabbitMQManager(
    name="my_manager",
    host="localhost",
    port=5672,
    username="guest",
    password="guest"
)

# Add queue
queue_config = QueueConfig(name="data_queue", durable=True)
manager.add_queue(queue_config)

# Publish message
message = {"sensor_id": "temp_001", "value": 25.5, "timestamp": time.time()}
manager.publish_message(
    message=message,
    routing_key="data_queue",
    priority=MessagePriority.NORMAL,
    persistent=True
)

# Start consuming
with manager:
    manager.start_consuming("data_queue")
```

## Message Priorities

The library supports four message priority levels:

- **LOW**: Priority 1
- **NORMAL**: Priority 2 (default)
- **HIGH**: Priority 3
- **CRITICAL**: Priority 4

## Configuration

### Connection Settings

```python
manager = RabbitMQManager(
    name="my_manager",
    host="localhost",           # RabbitMQ host
    port=5672,                  # RabbitMQ port
    username="guest",           # Username
    password="guest",           # Password
    virtual_host="/",           # Virtual host
    connection_timeout=5.0,     # Connection timeout
    heartbeat_interval=600,     # Heartbeat interval
    retry_count=3,              # Retry attempts
    retry_delay=1.0             # Delay between retries
)
```

### Logging

The library uses a simple logging system that can be customized:

```python
from connect_to_rabbit import SimpleLogger

# Create custom logger
logger = SimpleLogger(log_level=1)  # 0=info, 1=warning, 2=error

# Use with RabbitMQ manager
manager = RabbitMQManager(name="test", logger=logger)
```

### Health Monitoring

Health monitoring is automatically enabled:

```python
# Check health status
status = manager.get_status()
print(f"Health: {status['health_status']}")
print(f"Last check: {status['last_health_check']}")
```

### Statistics

The library provides detailed statistics:

```python
# Get statistics
stats = manager.get_status()['stats']
print(f"Messages published: {stats['messages_published']}")
print(f"Messages consumed: {stats['messages_consumed']}")
print(f"Messages failed: {stats['messages_failed']}")
print(f"Connection errors: {stats['connection_errors']}")
```

## Examples

### Basic Message Publishing

```python
from connect_to_rabbit import RabbitMQManager, MessagePriority
import time

# Create manager
manager = RabbitMQManager(
    name="publisher",
    host="localhost",
    port=5672,
    username="guest",
    password="guest"
)

# Publish messages
messages = [
    {"id": 1, "data": "Hello World", "timestamp": time.time()},
    {"id": 2, "data": "Test Message", "timestamp": time.time()},
    {"id": 3, "data": "Important Data", "timestamp": time.time()}
]

with manager:
    for i, message in enumerate(messages):
        priority = MessagePriority.HIGH if i == 2 else MessagePriority.NORMAL
        success = manager.publish_message(
            message=message,
            routing_key="test_queue",
            priority=priority,
            persistent=True
        )
        
        if success:
            print(f"Message {i+1} published successfully")
        else:
            print(f"Failed to publish message {i+1}")
```

### Message Consumption with Custom Handler

```python
from connect_to_rabbit import RabbitMQManager, QueueConfig, MessageHandler
import json

# Custom message handler
class DataProcessor(MessageHandler):
    def __init__(self):
        super().__init__("DataProcessor")
    
    def handle_message(self, message):
        try:
            # Process the message
            print(f"Processing message: {message}")
            
            # Extract data
            sensor_id = message.get('sensor_id')
            value = message.get('value')
            timestamp = message.get('timestamp')
            
            # Process the data (e.g., save to database, analyze, etc.)
            print(f"Sensor {sensor_id}: {value} at {timestamp}")
            
            return True  # Message processed successfully
            
        except Exception as e:
            print(f"Error processing message: {e}")
            return False  # Message processing failed

# Create manager
manager = RabbitMQManager(
    name="consumer",
    host="localhost",
    port=5672,
    username="guest",
    password="guest"
)

# Configure queue
queue_config = QueueConfig(name="sensor_data", durable=True)
manager.add_queue(queue_config)

# Add message handler
handler = DataProcessor()
manager.add_message_handler("sensor_data", handler)

# Start consuming
with manager:
    manager.start_consuming("sensor_data")
    # This will run indefinitely until interrupted
```

### IoT Sensor Data Example

```python
from connect_to_rabbit import RabbitMQManager, QueueConfig, MessagePriority
import time
import random

# Create manager for IoT data
manager = RabbitMQManager(
    name="iot_manager",
    host="localhost",
    port=5672,
    username="guest",
    password="guest"
)

# Configure queues for different sensor types
queues = [
    QueueConfig(name="temperature_data", durable=True),
    QueueConfig(name="humidity_data", durable=True),
    QueueConfig(name="pressure_data", durable=True)
]

for queue in queues:
    manager.add_queue(queue)

# Simulate sensor data publishing
sensors = [
    {"id": "temp_001", "type": "temperature", "queue": "temperature_data"},
    {"id": "temp_002", "type": "temperature", "queue": "temperature_data"},
    {"id": "hum_001", "type": "humidity", "queue": "humidity_data"},
    {"id": "pres_001", "type": "pressure", "queue": "pressure_data"}
]

with manager:
    while True:
        for sensor in sensors:
            # Generate simulated sensor data
            if sensor["type"] == "temperature":
                value = random.uniform(20.0, 30.0)
                unit = "°C"
                priority = MessagePriority.NORMAL
            elif sensor["type"] == "humidity":
                value = random.uniform(40.0, 80.0)
                unit = "%"
                priority = MessagePriority.NORMAL
            else:  # pressure
                value = random.uniform(1000.0, 1020.0)
                unit = "hPa"
                priority = MessagePriority.HIGH
            
            # Create message
            message = {
                "sensor_id": sensor["id"],
                "type": sensor["type"],
                "value": round(value, 2),
                "unit": unit,
                "timestamp": time.time()
            }
            
            # Publish message
            success = manager.publish_message(
                message=message,
                routing_key=sensor["queue"],
                priority=priority,
                persistent=True
            )
            
            if success:
                print(f"Published {sensor['type']} data: {value}{unit}")
            else:
                print(f"Failed to publish {sensor['type']} data")
        
        time.sleep(5)  # Publish every 5 seconds
```

### Exchange and Routing Example

```python
from connect_to_rabbit import RabbitMQManager, ExchangeConfig, QueueConfig, MessagePriority

# Create manager
manager = RabbitMQManager(
    name="routing_manager",
    host="localhost",
    port=5672,
    username="guest",
    password="guest"
)

# Create exchange
exchange_config = ExchangeConfig(
    name="sensor_exchange",
    exchange_type="topic",
    durable=True
)
manager.add_exchange(exchange_config)

# Create queues
queues = [
    QueueConfig(name="critical_alerts", durable=True),
    QueueConfig(name="normal_data", durable=True),
    QueueConfig(name="all_sensors", durable=True)
]

for queue in queues:
    manager.add_queue(queue)

# Bind queues to exchange with routing keys
# This would typically be done through RabbitMQ management interface
# or using the channel.basic_bind() method

# Publish messages with different routing keys
messages = [
    {
        "routing_key": "sensor.temperature.critical",
        "message": {"sensor": "temp_001", "value": 45.0, "status": "critical"},
        "priority": MessagePriority.CRITICAL
    },
    {
        "routing_key": "sensor.humidity.normal",
        "message": {"sensor": "hum_001", "value": 65.0, "status": "normal"},
        "priority": MessagePriority.NORMAL
    },
    {
        "routing_key": "sensor.pressure.warning",
        "message": {"sensor": "pres_001", "value": 1050.0, "status": "warning"},
        "priority": MessagePriority.HIGH
    }
]

with manager:
    for msg_config in messages:
        success = manager.publish_message(
            message=msg_config["message"],
            routing_key=msg_config["routing_key"],
            exchange="sensor_exchange",
            priority=msg_config["priority"],
            persistent=True
        )
        
        if success:
            print(f"Published to {msg_config['routing_key']}")
        else:
            print(f"Failed to publish to {msg_config['routing_key']}")
```

### Logging Message Handler

```python
from connect_to_rabbit import RabbitMQManager, QueueConfig, LoggingMessageHandler

# Create manager
manager = RabbitMQManager(
    name="logging_manager",
    host="localhost",
    port=5672,
    username="guest",
    password="guest"
)

# Configure queue
queue_config = QueueConfig(name="log_queue", durable=True)
manager.add_queue(queue_config)

# Add logging handler
handler = LoggingMessageHandler(manager.logger)
manager.add_message_handler("log_queue", handler)

# Start consuming
with manager:
    manager.start_consuming("log_queue")
```

### Health Monitoring Example

```python
from connect_to_rabbit import RabbitMQManager
import time

# Create manager
manager = RabbitMQManager(
    name="health_monitor",
    host="localhost",
    port=5672,
    username="guest",
    password="guest"
)

# Continuous health monitoring
while True:
    try:
        # Check health
        is_healthy = manager.check_health()
        
        if is_healthy:
            print("RabbitMQ connection is healthy")
        else:
            print("RabbitMQ connection issues detected")
        
        # Get detailed status
        status = manager.get_status()
        print(f"Connection status: {status['is_connected']}")
        print(f"Health status: {status['health_status']}")
        print(f"Last health check: {status['last_health_check']}")
        
        # Get statistics
        stats = status['stats']
        print(f"Messages published: {stats['messages_published']}")
        print(f"Messages consumed: {stats['messages_consumed']}")
        print(f"Connection errors: {stats['connection_errors']}")
        
        print("-" * 50)
        time.sleep(30)  # Check every 30 seconds
        
    except KeyboardInterrupt:
        print("Health monitoring stopped")
        break
    except Exception as e:
        print(f"Health check error: {e}")
        time.sleep(30)
```

## Message Handling

Create custom message handlers by extending the `MessageHandler` base class:

```python
from connect_to_rabbit import MessageHandler

class CustomMessageHandler(MessageHandler):
    def __init__(self, logger):
        super().__init__("CustomHandler")
        self.logger = logger
    
    def handle_message(self, message):
        try:
            # Process the message
            print(f"Processing: {message}")
            
            # Your custom processing logic here
            # e.g., save to database, analyze data, trigger actions
            
            return True  # Return True if processing was successful
            
        except Exception as e:
            self.logger.log(f"Error processing message: {e}", log_type=2)
            return False  # Return False if processing failed

# Add handler to manager
handler = CustomMessageHandler(manager.logger)
manager.add_message_handler("my_queue", handler)
```

## Error Handling

The library includes comprehensive error handling:

- **Connection Errors**: Automatic retry with exponential backoff
- **Message Errors**: Failed message handling with acknowledgment
- **Resource Management**: Safe cleanup with context managers
- **Logging**: Detailed error logging with different levels

## Performance Considerations

### Connection Management
- Configure appropriate heartbeat intervals (300-600 seconds)
- Use connection pooling for high-throughput applications
- Monitor connection health regularly

### Message Publishing
- Use persistent messages for critical data
- Set appropriate message priorities
- Consider message size and frequency

### Message Consumption
- Configure prefetch count based on processing speed
- Use appropriate acknowledgment strategies
- Monitor queue depths and consumer performance

### Queue Management
- Use durable queues for important data
- Configure appropriate queue arguments
- Monitor queue performance and memory usage

## Troubleshooting

### Common Issues

1. **Connection Failures**
   - Check RabbitMQ server status
   - Verify network connectivity
   - Ensure proper authentication credentials
   - Check virtual host permissions

2. **Message Publishing Errors**
   - Verify queue/exchange exists
   - Check routing key configuration
   - Ensure proper message format
   - Monitor queue capacity

3. **Message Consumption Issues**
   - Check consumer acknowledgment
   - Verify message handler implementation
   - Monitor consumer performance
   - Check for message processing errors

4. **Performance Issues**
   - Monitor connection pool usage
   - Check message throughput
   - Verify queue depths
   - Review acknowledgment strategies

### Debug Mode

Enable debug logging by setting log level to 0:

```python
logger = SimpleLogger(log_level=0)
```

### RabbitMQ Server Commands

```bash
# Check RabbitMQ status
sudo systemctl status rabbitmq-server

# Start RabbitMQ server
sudo systemctl start rabbitmq-server

# Stop RabbitMQ server
sudo systemctl stop rabbitmq-server

# Enable RabbitMQ on boot
sudo systemctl enable rabbitmq-server

# Check RabbitMQ management plugin
sudo rabbitmq-plugins list

# Enable management plugin
sudo rabbitmq-plugins enable rabbitmq_management
```

### Connection Testing

```python
from connect_to_rabbit import RabbitMQManager

# Test connection
manager = RabbitMQManager(
    name="test",
    host="localhost",
    port=5672,
    username="guest",
    password="guest"
)

# Test connection
if manager.connect():
    print("Connection successful")
    manager.disconnect()
else:
    print("Connection failed")
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review the examples
3. Create an issue with detailed information

## Version History

- **v1.0.0**: Initial release with RabbitMQ support
- Standalone implementation without external framework dependencies
- Comprehensive message publishing and consumption
- Queue and exchange management
- Health monitoring and statistics
- Extensible message handling system
- Thread-safe operations

## References

- RabbitMQ Documentation: https://www.rabbitmq.com/documentation.html
- AMQP Protocol: https://www.amqp.org/
- Message Queuing Patterns
- Event-Driven Architecture 
