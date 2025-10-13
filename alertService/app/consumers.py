from channels.generic.websocket import WebsocketConsumer
from asgiref.sync import async_to_sync
import json

class TestConsumer(WebsocketConsumer):
    def connect(self):
        self.room_name = "test_consumer"
        self.group_name = "test_consumer_group"

        async_to_sync(self.channel_layer.group_add)(
            self.group_name, self.room_name
        )
        self.accept()
        self.send(text_data=json.dumps({'status' : 'connected'}))

    def receive(self, text_data):
        print(text_data)
        data = {
            "message" : f'recieved {text_data}'
        }
        self.send(text_data=json.dumps(data))

    def disconnect(self, *args, **kwargs):
        print("disconnect")