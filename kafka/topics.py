from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import os
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVER = os.environ["KAFKA_BROKER"]

def create_topics():
    admin_client = KafkaAdminClient(BOOTSTRAP_SERVER)  # Create an admin client to handle topic creation

    # The list of topics needed for news, stocks and crypto
    topics = [
        NewTopic("current_news", num_partitions=3, replication_factor=1),
        NewTopic("stock_prices", num_partitions=3, replication_factor=1),
        NewTopic("crypto_prices", num_partitions=3, replication_factor=1),
    ]

    # Attempt the creation of topics, handle errors, and close the admin client when done
    try:
        admin_client.create_topics(new_topics=topics, validate_only=False)
        print("Successfully created the Kafka topics")
    except TopicAlreadyExistsError:
        print("Topic already exists")
    finally:
        admin_client.close()

if __name__ == "__main__":
    create_topics()