import os
import json
import logging
import psycopg2
from kafka import KafkaConsumer # Possible switch to confluent kafka in the future
from dotenv import load_dotenv

load_dotenv()

# Setup logging to print errors if message sending fails
logging.basicConfig(level=logging.INFO)  # Logs messages with a level of INFO or higher (ignores DEBUG)
log = logging.getLogger(__name__)  # Retrieves logger instance specific to the current module

def connect_db():
    try:
        conn = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            port=int(os.environ["POSTGRES_PORT"])
        )
        return conn
    except KeyError as e:
        log.error(f"Missing required environment variable: {e}") # More specific error handling
        raise # Inturrupts program flow when an error is encountered
    except Exception as e:
        log.error(f"Could not connect to the Postgres database: {e}")
        raise

consumer = KafkaConsumer(
    'current_news', 'current_stock',
    bootstrap_servers=os.environ["KAFKA_BROKER"],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset="earliest",
    group_id='market-consumer-group',
    enable_auto_commit=True
)

def consume_data():
    conn = connect_db() # To connect to the databse
    cur = conn.cursor() # To run SQL commands

    print("Kafka consumer now listening to topics") # Print to see the start of the consumer

    for msg in consumer:
        topic = msg.topic
        data = msg.value
        log.info(f"Received from {topic}: {data}")

        try:
            if topic == 'current_news':
                cur.execute("""
                            INSERT INTO news (id, source_id, headline, published_at, source_from, site_url)
                            VALUES (DEFAULT, %s, %s, %s, %s, %s)
                            """, (
                                data['source_id'],
                                data['headline'],
                                data['published_at'],
                                data['source_from'],
                                data['site_url']
                            ))

            elif topic == 'current_stock':
                cur.execute("""
                            INSERT INTO stocks (id, ticker, price, published_at)
                            VALUES (DEFAULT, %s, %s, %s)
                            """, (
                                data['ticker'],
                                data['price'],
                                data['published_at']
                            ))

            conn.commit()

        except Exception as e:
            log.error(f"Could not consume the data: {e}")
            raise

if __name__ == "__main__":
    consume_data()
