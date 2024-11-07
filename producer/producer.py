import json,sys,os,logging,time
from kafka import KafkaProducer
import schedule
from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
import urllib
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
SQL_SERVER = os.environ.get("SQL_SERVER")
SQL_DATABASE = os.environ.get("SQL_DATABASE")
SQL_USER = os.environ.get("SQL_USER")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD")
SQL_DRIVER = os.environ.get("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

connection_string = (
    f"DRIVER={SQL_DRIVER};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USER};"
    f"PWD={SQL_PASSWORD}"
)

connection_url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"
engine = create_engine(connection_url)
db_session = scoped_session(sessionmaker(bind=engine))

try:
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    logger.info("Connected to the database successfully")
except Exception as e:
    logger.error(f"Database connection failed: {str(e)}")

def get_all_users():
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM cdbdw_prod.cdb_cdpu_crib_status WHERE BOT_STATUS='NEW';"))
            all_users = result.fetchall()
            if all_users:
                all_users_list = [{column: value for column, value in zip(result.keys(), row)} for row in all_users]
                return all_users_list
            else:
                return []
    except Exception as e:
        logger.error(f"Database query failed: {str(e)}")
        return []

def publish_message():
    bootstrap_servers = [os.environ.get('BOOSTRAP_SERVERS')]
    topicName = 'test'

    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            break
        except Exception as e:
            logger.warning(f"Waiting for Kafka Connect: {e}")
            time.sleep(1)

    all_users = get_all_users()

    if all_users:
        for user in all_users:
            help_id = user.get('HELP_ID')
            if help_id:
                producer.send(topicName, {'HELP_ID': help_id})
                logger.info(f"Message Sent: {help_id}")
    else:
        logger.info("No user data found")

    logger.info("All messages sent")

def main():
    # schedule.every().day.at("11:13").do(publish_message)
    schedule.every(5).minutes.do(publish_message)
    

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
