import os
import json
import logging
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer
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
    'current_news', 'current_stock', 'current_crypto',
    bootstrap_servers=os.environ["KAFKA_BROKER"],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset="earliest",
    group_id='market-consumer-group',
    enable_auto_commit=True
)

def consume_data():
    conn = connect_db() # To connect to the databse
    cur = conn.cursor() # To run SQL commands
    log.info("Kafka consumer now listening to topics")

    for msg in consumer:
        topic = msg.topic
        data = msg.value
        log.info(f"Received on {topic}: keys={list(data.keys())}")

        try:
            if topic == 'current_news':
                time_published = datetime.strptime(data['time_published'], "%Y%m%dT%H%M%S")
                cur.execute("""
                            INSERT INTO raw_news (ticker, company, sector, time_published, news_info)
                            VALUES (%s, %s, %s, %s, %s)
                            """, (
                                data['ticker'],
                                data['company'],
                                data['sector'],
                                time_published,
                                json.dumps(data['news_info'])
                            ))

            elif topic == 'current_stock':
                cur.execute("""
                            INSERT INTO raw_stock (ticker, company, sector, market_date, stock_info)
                            VALUES (%s, %s, %s, %s, %s)
                            """, (
                                data['ticker'],
                                data['company'],
                                data['sector'],
                                data['market_date'],
                                json.dumps(data['stock_info'])
                            ))
                
            elif topic == 'current_crypto':
                cur.execute("""
                            INSERT INTO raw_crypto (symbol, crypto, category, market_date, crypto_info)
                            VALUES (%s, %s, %s, %s, %s)
                            """, (
                                data['symbol'],
                                data['crypto'],
                                data['category'],
                                data['market_date'],
                                json.dumps(data['crypto_info'])
                            ))

            conn.commit()

        except Exception as e:
            log.error(f"Insert failed (topic={topic}): {e}")
            conn.rollback()

if __name__ == "__main__":
    consume_data()
