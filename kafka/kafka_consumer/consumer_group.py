from kafka import *


class ConsumerGroupApp01:
    def __init__(self):
        self.consumer = KafkaConsumer(bootstrap_servers="localhost:9092", group_id = "test_group")
        self.consumer.subscribe("new_topic")
        while True:
            records = self.consumer.poll(10)
            for record in records:
                print(record.partition)

if __name__ == "__main__":
    ConsumerGroupApp01()