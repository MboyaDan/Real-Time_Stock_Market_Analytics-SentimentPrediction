from kafka import KafkaConsumer
import json
import os
import time
import traceback

# Load environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

# Kafka Topics
TOPIC_STOCKS = "stock_prices"
TOPIC_NEWS = "stock_news"
TOPIC_REDDIT = "stock_reddit"

# Create Kafka Consumer with Auto-Retry
def create_consumer(topic):
    """Creates a Kafka consumer with retry logic for robustness."""
    delay = 5  # Initial retry delay
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8"))
            )
            print(f"Connected to Kafka topic: {topic}")
            return consumer
        except Exception as e:
            print(f"Failed to connect to Kafka ({topic}): {e}. Retrying in {delay}s...")
            traceback.print_exc()  # Print full error stack trace
            time.sleep(delay)
            delay = min(delay * 2, 60)  # Exponential backoff (max 60s)

# Consume messages from Kafka
def consume_messages():
    """Consumes messages from multiple Kafka topics."""
    print("Kafka Consumer Starting...")

    consumers = {
        TOPIC_STOCKS: create_consumer(TOPIC_STOCKS),
        TOPIC_NEWS: create_consumer(TOPIC_NEWS),
        TOPIC_REDDIT: create_consumer(TOPIC_REDDIT),
    }

    while True:
        try:
            for topic, consumer in consumers.items():
                messages = consumer.poll(timeout_ms=1000)

                if messages:
                    for records in messages.values():
                        for record in records:
                            print(f"Topic: {record.topic}, Message: {record.value}")

            time.sleep(1)  # Prevent CPU overuse

        except Exception as e:
            print(f"Consumer error: {e}. Restarting consumer in 5s...")
            traceback.print_exc()  # Show full error details
            time.sleep(5)
            consume_messages()  # Restart consumer after failure

if __name__ == "__main__":
    consume_messages()
