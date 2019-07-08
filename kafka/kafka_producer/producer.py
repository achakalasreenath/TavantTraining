from kafka import *
class KafkaProducerApp:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
        for i in range(50):
            self.producer.send('new_topic',bytes(str(i),encoding='utf-8'))

