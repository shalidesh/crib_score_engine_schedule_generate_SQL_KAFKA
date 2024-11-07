import json,sys
import os
import time
import logging
from kafka import KafkaConsumer
import requests
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    bootstrap_servers = [os.environ.get('BOOSTRAP_SERVERS')]
    topicName = 'test'

    # Wait for Kafka Connect to be ready
    while True:
        try:
            consumer = KafkaConsumer(
                topicName,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                group_id='crib_api_consumers'
            )
            logger.info("Connected to Kafka")
            break
        except Exception as e:
            logger.warning(f"Waiting for Kafka Connect: {e}")
            time.sleep(1)

    for msg in consumer:
        try:
            message = msg.value
            message_dict = json.loads(message)
            if 'HELP_ID' in message_dict:
                json_data = json.dumps(message_dict)
                response = requests.post(
                    f"http://{os.environ.get('ENGINE_ADDRESS')}/crib_calculate",
                    data=json_data,
                    headers={'Content-Type': 'application/json'}
                )

                if response.status_code == 200:
                    logger.info("Request was successful.")
                    logger.info(response.json())
                else:
                    logger.error(f"Request failed with status code {response.status_code}.")
            else:
                logger.info("Bot Status is not NEW")
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
