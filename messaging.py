import pika
import json
import logging
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
import time

from schemas import OrderMessage, OrderStatus
from database import SessionLocal, MarketOrderDB, LimitOrderDB

logger = logging.getLogger(__name__)


class RabbitMQConnection:
    def __init__(self, host: str = 'localhost', port: int = 5672,
                 username: Optional[str] = None, password: Optional[str] = None,
                 max_retries: int = 3, retry_delay: int = 5):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection = None
        self.channel = None

    def connect(self):
        """Connect to RabbitMQ with retry logic and detailed error reporting"""
        for attempt in range(self.max_retries):
            try:
                logger.info(
                    f"Attempting to connect to RabbitMQ at {self.host}:{self.port} (attempt {attempt + 1}/{self.max_retries})")

                if self.username and self.password:
                    credentials = pika.PlainCredentials(self.username, self.password)
                    parameters = pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                        credentials=credentials,
                        heartbeat=600,
                        blocked_connection_timeout=300
                    )
                else:
                    parameters = pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                        heartbeat=600,
                        blocked_connection_timeout=300
                    )

                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                logger.info(f"Successfully connected to RabbitMQ at {self.host}:{self.port}")
                return True

            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"AMQP Connection Error (attempt {attempt + 1}): {str(e)}")
                logger.error(f"Check if RabbitMQ server is running on {self.host}:{self.port}")
            except pika.exceptions.ProbableAuthenticationError as e:
                logger.error(f"Authentication Error: {str(e)}")
                logger.error("Check your RabbitMQ username and password")
                break  # Don't retry on auth errors
            except ConnectionRefusedError as e:
                logger.error(f"Connection Refused (attempt {attempt + 1}): {str(e)}")
                logger.error(f"RabbitMQ server might not be running on {self.host}:{self.port}")
            except Exception as e:
                logger.error(
                    f"Unexpected error connecting to RabbitMQ (attempt {attempt + 1}): {type(e).__name__}: {str(e)}")

            if attempt < self.max_retries - 1:
                logger.info(f"Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)

        logger.error(f"Failed to connect to RabbitMQ after {self.max_retries} attempts")
        return False

    def disconnect(self):
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("Disconnected from RabbitMQ")
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")

    def is_connected(self):
        """Check if connection is active"""
        return (self.connection and
                not self.connection.is_closed and
                self.channel and
                not self.channel.is_closed)

    def declare_queue(self, queue_name: str, durable: bool = True):
        if self.channel:
            try:
                self.channel.queue_declare(queue=queue_name, durable=durable)
                logger.info(f"Queue '{queue_name}' declared successfully")
            except Exception as e:
                logger.error(f"Failed to declare queue '{queue_name}': {e}")

    def declare_exchange(self, exchange_name: str, exchange_type: str = 'direct'):
        if self.channel:
            try:
                self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)
                logger.info(f"Exchange '{exchange_name}' declared successfully")
            except Exception as e:
                logger.error(f"Failed to declare exchange '{exchange_name}': {e}")


# Global RabbitMQ connection instance
rabbitmq = RabbitMQConnection()


class OrderPublisher:
    def __init__(self):
        self.exchange_name = 'orders_exchange'

    def setup_exchange(self):
        """Setup exchange and queues with proper error handling"""
        if not rabbitmq.connect():
            logger.error("Cannot setup exchange - RabbitMQ connection failed")
            return False

        try:
            rabbitmq.declare_exchange(self.exchange_name, 'direct')
            rabbitmq.declare_queue('market_orders', durable=True)
            rabbitmq.declare_queue('limit_orders', durable=True)

            # Bind queues to exchange
            rabbitmq.channel.queue_bind(
                exchange=self.exchange_name,
                queue='market_orders',
                routing_key='market'
            )
            rabbitmq.channel.queue_bind(
                exchange=self.exchange_name,
                queue='limit_orders',
                routing_key='limit'
            )
            logger.info("Exchange and queues setup completed successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to setup exchange and queues: {e}")
            return False

    def publish_order(self, order_message: OrderMessage):
        try:
            if not rabbitmq.is_connected():
                logger.info("Reconnecting to RabbitMQ...")
                if not rabbitmq.connect():
                    logger.error("Failed to reconnect to RabbitMQ")
                    return False

            routing_key = 'market' if order_message.order_type == 'market' else 'limit'

            rabbitmq.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=order_message.json(),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    timestamp=int(datetime.now().timestamp())
                )
            )
            logger.info(f"Published {order_message.order_type} order: {order_message.order_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish order: {type(e).__name__}: {str(e)}")
            return False


class OrderConsumer:
    def __init__(self):
        pass

    def setup_consumer(self):
        if not rabbitmq.connect():
            logger.error("Cannot setup consumer - RabbitMQ connection failed")
            return False

        try:
            rabbitmq.declare_queue('market_orders', durable=True)
            rabbitmq.declare_queue('limit_orders', durable=True)
            logger.info("Consumer queues setup completed")
            return True
        except Exception as e:
            logger.error(f"Failed to setup consumer queues: {e}")
            return False

    def process_market_order(self, ch, method, properties, body):
        try:
            order_data = json.loads(body)
            order_message = OrderMessage(**order_data)

            # Process the market order
            db = SessionLocal()
            try:
                db_order = db.query(MarketOrderDB).filter(
                    MarketOrderDB.id == order_message.order_id
                ).first()

                if db_order:
                    # Simulate order processing - implement your matching logic here
                    db_order.status = OrderStatus.EXECUTED
                    db.commit()
                    logger.info(f"Processed market order: {order_message.order_id}")

                ch.basic_ack(delivery_tag=method.delivery_tag)
            finally:
                db.close()

        except Exception as e:
            logger.error(f"Error processing market order: {type(e).__name__}: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def process_limit_order(self, ch, method, properties, body):
        try:
            order_data = json.loads(body)
            order_message = OrderMessage(**order_data)

            # Process the limit order
            db = SessionLocal()
            try:
                db_order = db.query(LimitOrderDB).filter(
                    LimitOrderDB.id == order_message.order_id
                ).first()

                if db_order:
                    # Implement your limit order matching logic here
                    logger.info(f"Processing limit order: {order_message.order_id}")

                ch.basic_ack(delivery_tag=method.delivery_tag)
            finally:
                db.close()

        except Exception as e:
            logger.error(f"Error processing limit order: {type(e).__name__}: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self):
        if not rabbitmq.is_connected():
            if not rabbitmq.connect():
                logger.error("Cannot start consuming - RabbitMQ connection failed")
                return

        try:
            rabbitmq.channel.basic_qos(prefetch_count=10)  # Process 10 messages at a time

            rabbitmq.channel.basic_consume(
                queue='market_orders',
                on_message_callback=self.process_market_order
            )

            rabbitmq.channel.basic_consume(
                queue='limit_orders',
                on_message_callback=self.process_limit_order
            )

            logger.info("Starting to consume messages...")
            rabbitmq.channel.start_consuming()
        except Exception as e:
            logger.error(f"Error during message consumption: {type(e).__name__}: {str(e)}")


# Global instances
order_publisher = OrderPublisher()