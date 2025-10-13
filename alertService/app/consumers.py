from channels.generic.websocket import WebsocketConsumer, AsyncJsonWebsocketConsumer
from asgiref.sync import async_to_sync
import json
import redis.asyncio as redis
from django.conf import settings
from channels.layers import get_channel_layer

REDIS_URL = settings.REDIS_URL


class AlertConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        await self.accept()
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)

    async def disconnect(self, code):
        await self.redis.zrem("device_locations", self.channel_name)
        await self.redis.close()

    async def receive_json(self, content):
        latitude = content.get("latitude")
        longitude = content.get("longitude")

        if latitude is not None and longitude is not None:
            await self.redis.geoadd("device_locations", (longitude, latitude, self.channel_name)) 
            await self.send_json({"status": "location stored"})
            return

        alert_data = content.get("alert")
        alert_lat = content.get("alert_latitude")
        alert_lon = content.get("alert_longitude")
        alert_radius = content.get("alert_radius_km", 1)  # default 1 km

        if alert_data and alert_lat is not None and alert_lon is not None:
            await self.broadcast_alert(alert_data, alert_lat, alert_lon, alert_radius)
            # await self.send_json({"status": "alert broadcasted"})
            return

        await self.send_json({"error": "invalid message format"})

    async def broadcast_alert(self, alert_data, center_lat, center_lon, radius_km):
        channel_layer = get_channel_layer()

        nearby_devices = await self.redis.georadius(
            "device_locations",
            center_lon,
            center_lat,
            radius_km,
            unit="km"
        )

        for device_channel in nearby_devices:
            await channel_layer.send(device_channel, {
                "type": "alert.message",
                "alert": alert_data,
            })

    async def alert_message(self, event):
        alert = event["alert"]
        await self.send_json({"alert": alert})

# Test Web Sockets

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

