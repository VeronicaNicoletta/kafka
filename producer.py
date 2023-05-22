from kafka import KafkaProducer
import logging
import psycopg2
import time
import connection

# Configure the format of logs
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Kafka producer configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'documents'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
print('Kafka Producer has been initiated...')

# Connect to the log table in the database
conn=connection.connect()

cursor = conn.cursor()

# Query the log table for changes
sql = "SELECT * FROM book_logs"
cursor.execute(sql)
rows = cursor.fetchall()

# Publish log table changes to Kafka topic
for row in rows:
    if len(row) != 3:
        print("Invalid row:", row)
        continue

    id_book, title, log_timestamp = row
    message = f"Book ID: {id_book}, Title: {title},log time:{log_timestamp}"
    producer.send(topic_name, value=message.encode())

conn.commit()

# Wait for 60 seconds before monitoring the table again
time.sleep(60)

# Close resources
cursor.close()
conn.close()
producer.flush()
producer.close()
