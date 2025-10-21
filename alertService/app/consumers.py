from channels.generic.websocket import WebsocketConsumer, AsyncJsonWebsocketConsumer
from asgiref.sync import async_to_sync
import json
import hashlib
import time
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
            # Store location using channel_name as member (existing behavior).
            await self.redis.geoadd("device_locations", (longitude, latitude, self.channel_name))
            await self.send_json({"status": "location stored"})
            return

        alert_data = content.get("alert")
        alert_lat = content.get("alert_latitude")
        alert_lon = content.get("alert_longitude")
        alert_radius = content.get("alert_radius_km", 1)  # default 1 km

        # Extract dedupe-related fields: 'class' and optional 'alert_id'
        class_id = content.get("class")
        alert_id = content.get("alert_id")

        if alert_data and alert_lat is not None and alert_lon is not None:
            await self.broadcast_alert(alert_data, alert_lat, alert_lon, alert_radius, class_id=class_id, alert_id=alert_id)
            # await self.send_json({"status": "alert broadcasted"})
            return

        await self.send_json({"error": "invalid message format"})

    async def broadcast_alert(self, alert_data, center_lat, center_lon, radius_km):
        channel_layer = get_channel_layer()

        # Existing simple broadcast (kept as fallback) - this method is replaced by enhanced_broadcast
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

    # Helper: compute deterministic signature for alert grouping
    def compute_alert_signature(self, alert_obj, class_id, center_lat, center_lon, radius_km, rounding=2):
        """
        Compute a deterministic signature (sha256 hex) from alert components.
        rounding roughly controls spatial grouping: 2 decimals ~1.1 km, 3 ~110m.
        """
        alert_json = json.dumps(alert_obj or {}, sort_keys=True, separators=(",", ":"))
        lat_r = round(float(center_lat), rounding)
        lon_r = round(float(center_lon), rounding)
        base = f"class={class_id}|alert={alert_json}|lat={lat_r}|lon={lon_r}|r={float(radius_km)}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()

    async def broadcast_alert(self, alert_data, center_lat, center_lon, radius_km, class_id=None, alert_id=None):
        """
        Enhanced broadcast with deduplication using Redis SET NX. If an alert_id is provided it is used;
        otherwise a deterministic signature including 'class' and rounded coords is computed.
        """
        DEDUPE_TTL_SECONDS = 300 # 300 seconds
        ROUNDING_DECIMALS = 2

        # Choose dedupe key
        if alert_id:
            dedupe_key = f"alert_id:{alert_id}"
        else:
            sig = self.compute_alert_signature(alert_data, class_id, center_lat, center_lon, radius_km, rounding=ROUNDING_DECIMALS)
            dedupe_key = f"alert_sig:{sig}"

        # Try to claim the dedupe key atomically
        claimed = await self.redis.set(dedupe_key, "1", nx=True, ex=DEDUPE_TTL_SECONDS)
        if not claimed:
            # Already processed recently
            await self.send_json({"status": "duplicate_alert_skipped", "key": dedupe_key})
            return

        # Proceed to broadcast
        channel_layer = get_channel_layer()

        nearby_devices = await self.redis.georadius(
            "device_locations",
            float(center_lon),
            float(center_lat),
            float(radius_km),
            unit="km"
        )

        unique_channels = set(nearby_devices)
        sent_set_key = f"alert_sent:{dedupe_key}"

        for device_channel in unique_channels:
            await channel_layer.send(device_channel, {
                "type": "alert.message",
                "alert": alert_data,
                "class": class_id,
            })
            # record recipients (optional) so we can track who received this alert
            await self.redis.sadd(sent_set_key, device_channel)

        # expire the sent set to clean up
        await self.redis.expire(sent_set_key, DEDUPE_TTL_SECONDS)

        await self.send_json({"status": "alert_broadcasted", "recipients": len(unique_channels), "key": dedupe_key})

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

