from kafka import *

class KafkaConsumerSubscribeApp:
    def __init__(self):
        self.consumer = KafkaConsumer(bootstrap_servers = 'localhost:9092')
        self.consumer.subscribe("new_topic")
        try:
            while(True):
                records = self.consumer.poll(100)
                for record in records:
                    print(record.partition)
        finally:
            self.consumer.close()

