#!/usr/bin/env python3
"""
Run this script separately to consume and process orders from RabbitMQ.
Usage: python consumer.py
"""

import logging
from messaging import OrderConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

if __name__ == "__main__":
    consumer = OrderConsumer()
    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        print("Stopping consumer...")
    except Exception as e:
        print(f"Consumer error: {e}")