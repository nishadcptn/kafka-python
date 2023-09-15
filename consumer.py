from confluent_kafka import Consumer, KafkaError

def consume(topic, group):
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': group,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition for topic {msg.topic()} [{msg.partition()}]")
            else:
                print(f"Error while consuming from topic {msg.topic()}: {msg.error()}")
        else:
            print(f"CONSUME --->[{group}]", msg.value().decode('utf-8'))

if __name__ == '__main__':
    consume("kafka.dev.v1", "Nishad")
