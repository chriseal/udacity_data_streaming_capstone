from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, verbose, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        self.verbose = verbose

    # DONE we're generating a dummy data
    def generate_data(self):
        with open(self.input_file) as f:
            data = json.load(f)
            if self.verbose:
                print(f"{len(data)} total events")
            for message in data:
                message_b = self.dict_to_binary(message)
                # DONE send the correct data
                self.send(self.topic, message_b)
                if self.verbose:
                    print(message_b)
                time.sleep(1)

    # DONE fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')
    
if __name__ == '__main__':
    self = ProducerServer(
        input_file='./police-department-calls-for-service.json',
        topic="police.call.service",
        verbose=True,
        **{'bootstrap_servers': "localhost:9092", 'client_id': "police.call.0"}
    )