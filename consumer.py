from confluent_kafka import Consumer
import psycopg2
import connection


# Connect to the log table in the database
conn=connection.connect()


# Kafka consumer configuration
def consume_topic(topic):
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'python-consumer',
    }

    consumer = Consumer(conf)
    print('Kafka Consumer has been initiated...')
    consumer.subscribe([topic])
    print('Subscribed to topic:', topic)

    try:
        while True:
            msg = consumer.poll(1.0)  # Timeout of 1 second
            if msg is not None:
                continue
            if msg.error():
                print('Error: {}'.format(msg.error()))
                continue
            message = 'Produced message with value of {}\n'.format(msg.value().decode('utf-8'))
            print('Received message:', message)

            cursor = conn.cursor()
            sql = "INSERT INTO book(id_book,author,date_book,title) VALUES (%s,%s,%s,%s)"
            cursor.execute(sql, (message, 'additional_value'))
            conn.commit()
            cursor.close()

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
        conn.close()


if __name__ == '__main__':
    consume_topic('documents')
