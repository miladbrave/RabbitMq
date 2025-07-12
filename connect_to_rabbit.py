import time
import threading
import json
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum

import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError


class MessagePriority(Enum):
    """Enumeration for message priorities."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class QueueConfig:
    """Configuration for RabbitMQ queue."""
    name: str
    durable: bool = True
    auto_delete: bool = False
    arguments: Optional[Dict[str, Any]] = None


@dataclass
class ExchangeConfig:
    """Configuration for RabbitMQ exchange."""
    name: str
    exchange_type: str = "direct"
    durable: bool = True
    auto_delete: bool = False
    arguments: Optional[Dict[str, Any]] = None


class SimpleLogger:
    """Simple logger for RabbitMQ manager."""
    
    def __init__(self, log_level: int = 0):
        """
        Initialize logger.
        
        Args:
            log_level: Log level (0=info, 1=warning, 2=error)
        """
        self.log_level = log_level
    
    def log(self, data: Any, log_type: int = 0, visibility: str = "TD", tag: str = "RabbitMQManager") -> None:
        """
        Log a message.
        
        Args:
            data: Data to log
            log_type: Type of log (0=info, 1=warning, 2=error)
            visibility: Visibility level
            tag: Tag for the log
        """
        if log_type >= self.log_level:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            level_str = {0: "INFO", 1: "WARNING", 2: "ERROR"}.get(log_type, "INFO")
            print(f"[{timestamp}] [{level_str}] [{tag}] {data}")


class MessageHandler:
    """Base class for message handlers."""
    
    def __init__(self, name: str):
        """
        Initialize message handler.
        
        Args:
            name: Handler name for identification
        """
        self.name = name
    
    def handle_message(self, message: Dict[str, Any]) -> bool:
        """
        Handle a received message.
        
        Args:
            message: Message to handle
            
        Returns:
            True if message was processed successfully, False otherwise
        """
        raise NotImplementedError("Subclasses must implement handle_message()")


class RabbitMQManager:
    """
    OOP wrapper for RabbitMQ communication.
    
    This class provides a clean, object-oriented interface for RabbitMQ
    operations with connection management, message publishing/consumption,
    and health monitoring.
    """
    
    def __init__(
        self,
        name: str,
        host: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        virtual_host: str = "/",
        connection_timeout: float = 5.0,
        heartbeat_interval: int = 600,
        retry_count: int = 3,
        retry_delay: float = 1.0,
        logger: Optional[SimpleLogger] = None
    ):
        """
        Initialize RabbitMQ Manager.
        
        Args:
            name: Manager name for identification
            host: RabbitMQ host address
            port: RabbitMQ port
            username: RabbitMQ username
            password: RabbitMQ password
            virtual_host: RabbitMQ virtual host
            connection_timeout: Connection timeout in seconds
            heartbeat_interval: Heartbeat interval in seconds
            retry_count: Number of retry attempts on failure
            retry_delay: Delay between retries in seconds
            logger: Logger instance
        """
        self.name = name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.connection_timeout = connection_timeout
        self.heartbeat_interval = heartbeat_interval
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        
        self.logger = logger or SimpleLogger()
        
        # Connection objects
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.is_connected = False
        
        # Configuration
        self.queues: Dict[str, QueueConfig] = {}
        self.exchanges: Dict[str, ExchangeConfig] = {}
        self.message_handlers: Dict[str, MessageHandler] = {}
        
        # Statistics
        self.stats = {
            "messages_published": 0,
            "messages_consumed": 0,
            "messages_failed": 0,
            "connection_errors": 0,
            "last_error": None
        }
        
        # Health monitoring
        self.last_health_check: Optional[float] = None
        self.health_status = "unknown"
        self.health_monitor_thread: Optional[threading.Thread] = None
        self.health_monitor_running = False
        
        # Start health monitoring
        self._start_health_monitor()
    
    def add_queue(self, queue_config: QueueConfig) -> bool:
        """
        Add a queue configuration.
        
        Args:
            queue_config: Queue configuration
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.is_connected and not self.connect():
                return False
            
            self.channel.queue_declare(
                queue=queue_config.name,
                durable=queue_config.durable,
                auto_delete=queue_config.auto_delete,
                arguments=queue_config.arguments or {}
            )
            
            self.queues[queue_config.name] = queue_config
            
            self.logger.log(
                data=f"Added queue: {queue_config.name}",
                log_type=0,
                visibility="TD",
                tag="RabbitMQManager"
            )
            return True
            
        except Exception as e:
            self.logger.log(
                data=f"Failed to add queue {queue_config.name}: {str(e)}",
                log_type=2,
                visibility="TD",
                tag="RabbitMQManager"
            )
            return False
    
    def add_exchange(self, exchange_config: ExchangeConfig) -> bool:
        """
        Add an exchange configuration.
        
        Args:
            exchange_config: Exchange configuration
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.is_connected and not self.connect():
                return False
            
            self.channel.exchange_declare(
                exchange=exchange_config.name,
                exchange_type=exchange_config.exchange_type,
                durable=exchange_config.durable,
                auto_delete=exchange_config.auto_delete,
                arguments=exchange_config.arguments or {}
            )
            
            self.exchanges[exchange_config.name] = exchange_config
            
            self.logger.log(
                data=f"Added exchange: {exchange_config.name}",
                log_type=0,
                visibility="TD",
                tag="RabbitMQManager"
            )
            return True
            
        except Exception as e:
            self.logger.log(
                data=f"Failed to add exchange {exchange_config.name}: {str(e)}",
                log_type=2,
                visibility="TD",
                tag="RabbitMQManager"
            )
            return False
    
    def add_message_handler(self, queue_name: str, handler: MessageHandler) -> None:
        """
        Add a message handler for a queue.
        
        Args:
            queue_name: Name of the queue to handle
            handler: Message handler instance
        """
        self.message_handlers[queue_name] = handler
        
        self.logger.log(
            data=f"Added message handler '{handler.name}' for queue '{queue_name}'",
            log_type=0,
            visibility="TD",
            tag="RabbitMQManager"
        )
    
    def connect(self) -> bool:
        """
        Establish connection to RabbitMQ.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if self.is_connected:
                return True
            
            # Create connection parameters
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.virtual_host,
                credentials=credentials,
                connection_attempts=self.retry_count,
                retry_delay=self.retry_delay,
                socket_timeout=self.connection_timeout,
                heartbeat=self.heartbeat_interval
            )
            
            # Create connection
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Set QoS
            self.channel.basic_qos(prefetch_count=1)
            
            self.is_connected = True
            self.stats["connection_errors"] = 0
            self.stats["last_error"] = None
            
            self.logger.log(
                data=f"Connected to RabbitMQ at {self.host}:{self.port}",
                log_type=0,
                visibility="TD",
                tag="RabbitMQManager"
            )
            return True
            
        except Exception as e:
            self.is_connected = False
            self.stats["connection_errors"] += 1
            self.stats["last_error"] = str(e)
            
            self.logger.log(
                data=f"Failed to connect to RabbitMQ: {str(e)}",
                log_type=2,
                visibility="TD",
                tag="RabbitMQManager"
            )
            return False
    
    def disconnect(self) -> None:
        """Disconnect from RabbitMQ."""
        try:
            if self.channel:
                self.channel.close()
            if self.connection:
                self.connection.close()
            
            self.is_connected = False
            
            self.logger.log(
                data=f"Disconnected from RabbitMQ",
                log_type=0,
                visibility="TD",
                tag="RabbitMQManager"
            )
            
        except Exception as e:
            self.logger.log(
                data=f"Error during disconnect: {str(e)}",
                log_type=2,
                visibility="TD",
                tag="RabbitMQManager"
            )
    
    def publish_message(
        self,
        message: Dict[str, Any],
        routing_key: str,
        exchange: str = "",
        priority: MessagePriority = MessagePriority.NORMAL,
        persistent: bool = True
    ) -> bool:
        """
        Publish a message to RabbitMQ.
        
        Args:
            message: Message to publish
            routing_key: Routing key for the message
            exchange: Exchange name (empty string for default)
            priority: Message priority
            persistent: Whether message should be persistent
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected and not self.connect():
            return False
        
        try:
            # Prepare message properties
            properties = pika.BasicProperties(
                delivery_mode=2 if persistent else 1,
                priority=priority.value,
                content_type='application/json',
                timestamp=int(time.time())
            )
            
            # Serialize message
            message_body = json.dumps(message, ensure_ascii=False)
            
            # Publish message
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message_body,
                properties=properties
            )
            
            self.stats["messages_published"] += 1
            
            self.logger.log(
                data=f"Published message to {exchange}:{routing_key}",
                log_type=0,
                visibility="TD",
                tag="RabbitMQManager"
            )
            return True
            
        except Exception as e:
            self.stats["messages_failed"] += 1
            self.stats["last_error"] = str(e)
            
            self.logger.log(
                data=f"Failed to publish message: {str(e)}",
                log_type=2,
                visibility="TD",
                tag="RabbitMQManager"
            )
            return False
    
    def start_consuming(self, queue_name: str, auto_ack: bool = False) -> bool:
        """
        Start consuming messages from a queue.
        
        Args:
            queue_name: Name of the queue to consume from
            auto_ack: Whether to automatically acknowledge messages
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected and not self.connect():
            return False
        
        try:
            # Set up consumer
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=self._message_callback,
                auto_ack=auto_ack
            )
            
            self.logger.log(
                data=f"Started consuming from queue: {queue_name}",
                log_type=0,
                visibility="TD",
                tag="RabbitMQManager"
            )
            return True
            
        except Exception as e:
            self.logger.log(
                data=f"Failed to start consuming from {queue_name}: {str(e)}",
                log_type=2,
                visibility="TD",
                tag="RabbitMQManager"
            )
            return False
    
    def _message_callback(
        self,
        ch: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes
    ) -> None:
        """
        Callback for received messages.
        
        Args:
            ch: Channel
            method: Delivery method
            properties: Message properties
            body: Message body
        """
        try:
            # Parse message
            message = json.loads(body.decode('utf-8'))
            
            # Get handler for this queue
            queue_name = method.routing_key
            handler = self.message_handlers.get(queue_name)
            
            if handler:
                # Process message
                success = handler.handle_message(message)
                
                if success:
                    self.stats["messages_consumed"] += 1
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                    self.logger.log(
                        data=f"Processed message from {queue_name}",
                        log_type=0,
                        visibility="TD",
                        tag="RabbitMQManager"
                    )
                else:
                    self.stats["messages_failed"] += 1
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    
                    self.logger.log(
                        data=f"Failed to process message from {queue_name}",
                        log_type=2,
                        visibility="TD",
                        tag="RabbitMQManager"
                    )
            else:
                # No handler, reject message
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                
                self.logger.log(
                    data=f"No handler for queue {queue_name}",
                    log_type=2,
                    visibility="TD",
                    tag="RabbitMQManager"
                )
                
        except Exception as e:
            self.stats["messages_failed"] += 1
            self.stats["last_error"] = str(e)
            
            # Reject message
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            self.logger.log(
                data=f"Error processing message: {str(e)}",
                log_type=2,
                visibility="TD",
                tag="RabbitMQManager"
            )
    
    def check_health(self) -> bool:
        """
        Check the health of the RabbitMQ connection.
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            if not self.is_connected:
                return False
            
            # Check if connection is still open
            if not self.connection or self.connection.is_closed:
                self.is_connected = False
                return False
            
            # Check if channel is still open
            if not self.channel or self.channel.is_closed:
                self.is_connected = False
                return False
            
            self.health_status = "healthy"
            self.last_health_check = time.time()
            return True
            
        except Exception as e:
            self.health_status = "unhealthy"
            self.last_health_check = time.time()
            self.logger.log(
                data=f"Health check failed: {str(e)}",
                log_type=2,
                visibility="TD",
                tag="RabbitMQManager"
            )
            return False
    
    def _start_health_monitor(self) -> None:
        """Start the health monitoring thread."""
        if not self.health_monitor_running:
            self.health_monitor_running = True
            self.health_monitor_thread = threading.Thread(
                target=self._health_monitor_loop,
                daemon=True
            )
            self.health_monitor_thread.start()
    
    def _health_monitor_loop(self) -> None:
        """Health monitoring loop."""
        while self.health_monitor_running:
            try:
                self.check_health()
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                self.logger.log(
                    data=f"Health monitor error: {str(e)}",
                    log_type=2,
                    visibility="TD",
                    tag="RabbitMQManager"
                )
                time.sleep(30)
    
    def execute(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute RabbitMQ operations.
        
        Returns:
            Tuple of (success: bool, results: Dict[str, Any])
        """
        try:
            # Connect to RabbitMQ
            if not self.connect():
                return False, {"error": "Failed to connect to RabbitMQ"}
            
            # Start consuming from all configured queues
            for queue_name in self.queues.keys():
                if queue_name in self.message_handlers:
                    self.start_consuming(queue_name)
            
            # Keep connection alive for a while
            time.sleep(10)
            
            return True, {
                "queues_configured": len(self.queues),
                "exchanges_configured": len(self.exchanges),
                "handlers_configured": len(self.message_handlers),
                "stats": self.stats.copy()
            }
            
        except Exception as e:
            return False, {"error": str(e)}
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get RabbitMQ manager status.
        
        Returns:
            Dictionary containing status information
        """
        return {
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "virtual_host": self.virtual_host,
            "connection_timeout": self.connection_timeout,
            "heartbeat_interval": self.heartbeat_interval,
            "retry_count": self.retry_count,
            "retry_delay": self.retry_delay,
            "is_connected": self.is_connected,
            "health_status": self.health_status,
            "last_health_check": self.last_health_check,
            "queues_count": len(self.queues),
            "exchanges_count": len(self.exchanges),
            "handlers_count": len(self.message_handlers),
            "stats": self.stats.copy()
        }
    
    def close(self) -> None:
        """Close the RabbitMQ manager and clean up resources."""
        self.health_monitor_running = False
        if self.health_monitor_thread:
            self.health_monitor_thread.join(timeout=5.0)
        
        self.disconnect()
        
        self.logger.log(
            data=f"Closed RabbitMQ manager: {self.name}",
            log_type=0,
            visibility="TD",
            tag="RabbitMQManager"
        )
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Example message handler
class LoggingMessageHandler(MessageHandler):
    """Example message handler that logs messages."""
    
    def __init__(self, logger: SimpleLogger):
        """
        Initialize logging message handler.
        
        Args:
            logger: Logger instance
        """
        super().__init__("LoggingHandler")
        self.logger = logger
    
    def handle_message(self, message: Dict[str, Any]) -> bool:
        """
        Handle a received message by logging it.
        
        Args:
            message: Message to handle
            
        Returns:
            True if message was processed successfully, False otherwise
        """
        try:
            self.logger.log(
                data=message,
                log_type=0,
                visibility="TD",
                tag="LoggingMessageHandler"
            )
            return True
        except Exception as e:
            self.logger.log(
                data=f"Failed to log message: {str(e)}",
                log_type=2,
                visibility="TD",
                tag="LoggingMessageHandler"
            )
            return False


# Factory functions and backward compatibility
def create_rabbitmq_manager(
    name: str,
    host: str = "localhost",
    port: int = 5672,
    **kwargs
) -> RabbitMQManager:
    """
    Factory function to create a RabbitMQ manager.
    
    Args:
        name: Manager name
        host: RabbitMQ host
        port: RabbitMQ port
        **kwargs: Additional arguments
        
    Returns:
        Configured RabbitMQManager instance
    """
    return RabbitMQManager(name, host, port, **kwargs)


def publish_to_rabbitmq(
    host: str,
    port: int,
    username: str,
    password: str,
    message: Dict[str, Any],
    routing_key: str,
    exchange: str = "",
    **kwargs
) -> bool:
    """
    Publish message to RabbitMQ (backward compatibility function).
    
    Args:
        host: RabbitMQ host
        port: RabbitMQ port
        username: RabbitMQ username
        password: RabbitMQ password
        message: Message to publish
        routing_key: Routing key
        exchange: Exchange name
        **kwargs: Additional arguments
        
    Returns:
        True if successful, False otherwise
    """
    manager = RabbitMQManager(
        name="temp_publisher",
        host=host,
        port=port,
        username=username,
        password=password,
        **kwargs
    )
    
    with manager:
        return manager.publish_message(message, routing_key, exchange)