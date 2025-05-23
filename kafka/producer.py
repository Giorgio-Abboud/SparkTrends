from kafka import KafkaProducer
import os
import json
import logging
from time import sleep  # Used to simulate delay between messages (subject to change)
from dotenv import load_dotenv

load_dotenv()

# Setup logging to print errors if message sending fails
logging.basicConfig(level=logging.INFO)  # Logs messages with a level of INFO or higher (ignores DEBUG)
log = logging.getLogger(__name__)  # Retrieves logger instance specific to the current module

# Create a Kafka producer instance
producer = KafkaProducer(
    # Address of the Kafka broker
    bootstrap_servers=os.environ["KAFKA_BROKER"],
    
    # Serialize Python dicts to JSON bytes before sending to Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to send data from a file to a Kafka topic
def send_data(file_path, topic):
    # Load JSON records from file
    with open(file_path, 'r') as f:
        data = json.load(f)

    # Send each record to Kafka topic, one by one
    for record in data:
        print(f"Sending to {topic}: {record}")

        # Send message with success and error callbacks ("\ to skip to the next line and make it look cleaner")
        producer.send(topic, value=record)\
                .add_callback(successful_send)\
                .add_errback(send_error)
        sleep(1)  # Simulate a delay (real-time streaming effect) (subject to change to actual streaming)

# Called when a message is successfully sent to Kafka
def successful_send(record_metadata):
    print(f"Sent to {record_metadata.topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")

# Called when there is an error sending a message to Kafka
def send_error(excp):
    log.error('Failed to send message to Kafka: ', exc_info=excp)

# Main execution
if __name__ == "__main__":
    # Send news articles to current_news topic
    send_data('test_data/news.json', 'current_news')

    # Send stock records to current_stock topic
    send_data('test_data/stocks.json', 'current_stock')

    # Wait until all buffered messages are sent
    producer.flush()

    # Close the producer cleanly
    producer.close()
