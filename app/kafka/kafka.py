from kafka import KafkaConsumer, KafkaProducer
import os
from dotenv import load_dotenv
import json
from integrations.stripe.Stripe import create_customer_stripe, delete_customer_stripe, update_customer_stripe

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(dotenv_path)

bootstrap_servers = 'localhost:9092'

def kafka_event_consumer():
    consumer = KafkaConsumer(
        os.getenv("KAFKA_TO_STRIPE_TOPIC", "my-topic"),
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',
    )

    try:
        for message in consumer:
            json_data = message.value.decode('utf-8')

            try:
                data = json.loads(json_data)
                stripe_id = data.get("stripe_id")
                name = data.get("name")
                email = data.get("email")
                action = data.get("action")

                if action == "create":
                    create_customer_stripe(email, name)
                elif action == "delete":
                    delete_customer_stripe(email)
                elif action == "update":
                    update_customer_stripe(stripe_id, name, email)

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

    except KeyboardInterrupt:
        print("Kafka consumer terminated by user.")
    finally:
        consumer.close()

def kafka_event_producer(message):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    topic = os.getenv("KAFKA_TO_STRIPE_TOPIC", "my-topic")
    producer.send(topic, value=message.encode('utf-8'))

    producer.close()