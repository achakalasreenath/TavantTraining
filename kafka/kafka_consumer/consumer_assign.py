from kafka import *
from kafka import TopicPartition


class KafkaConsumerSubscribeApp:
    def __init__(self):
        self.consumer = KafkaConsumer(bootstrap_servers = 'localhost:9092')
        self.consumer.assign([TopicPartition("other_new_topic",0)])
        try:
            while True:
                records = self.consumer.poll(100)
                for record in records:
                    print(record.partition)
        finally:
            self.consumer.close()

