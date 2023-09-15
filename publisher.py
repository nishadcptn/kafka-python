from confluent_kafka import Producer

def publish(payload):
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    def delivery_report(err, msg):
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    producer.produce('kafka.dev.v1', key=None, value=payload, callback=delivery_report)
    producer.flush()

if __name__ == '__main__':
    import json
    
    message_payload = {
        "id": 1,
        "payload": {
            "name": "Test payload 1"
        }
    }

    publish(json.dumps(message_payload))
