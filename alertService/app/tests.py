import asyncio
import json
from django.test import SimpleTestCase

from . import consumers as consumers_module


class FakeRedis:
	"""Minimal async in-memory fake Redis supporting only methods used in consumer tests."""

	def __init__(self):
		# store geo sets as dict: key -> list of (lon, lat, member)
		self._geo = {}
		self._sets = {}
		self._keys = {}

	async def geoadd(self, key, tuple_value):
		# tuple_value expected as (lon, lat, member) or multiple tuples
		if key not in self._geo:
			self._geo[key] = []
		# Allow adding single tuple
		if isinstance(tuple_value[0], (int, float)):
			lon, lat, member = tuple_value
			self._geo[key].append((float(lon), float(lat), member))
		else:
			# sequence of tuples
			for lon, lat, member in tuple_value:
				self._geo[key].append((float(lon), float(lat), member))

	async def georadius(self, key, lon, lat, radius_km, unit="km"):
		# simplistic: return all members in the geo key (tests are small)
		# In tests we will prepopulate only nearby members
		if key not in self._geo:
			return []
		return [member for (_lon, _lat, member) in self._geo[key]]

	async def set(self, key, value, nx=False, ex=None):
		if nx:
			if key in self._keys:
				return False
			self._keys[key] = value
			return True
		self._keys[key] = value
		return True

	async def sadd(self, key, member):
		s = self._sets.setdefault(key, set())
		s.add(member)
		return 1

	async def expire(self, key, seconds):
		# no-op for fake
		return True


class DummyChannelLayer:
	def __init__(self):
		self.sent = []

	async def send(self, channel, message):
		self.sent.append((channel, message))


class AlertConsumerTests(SimpleTestCase):
	def setUp(self):
		# patch get_channel_layer used inside consumers to return our dummy layer
		consumers_module.get_channel_layer = lambda: self.layer
		self.layer = DummyChannelLayer()

		# fake redis
		self.fake_redis = FakeRedis()

		# create consumer and attach fake redis
		self.consumer = consumers_module.AlertConsumer()
		self.consumer.redis = self.fake_redis
		# attach a fake send_json so consumer doesn't call ASGI base_send
		async def _fake_send_json(payload):
			# record acks on the channel layer for inspection
			if not hasattr(self.layer, 'acks'):
				self.layer.acks = []
			self.layer.acks.append(payload)

		self.consumer.send_json = _fake_send_json

	def test_alert_dedupe_with_signature(self):
		alert = {"msg": "pothole", "severity": 1}
		lat = 12.34567
		lon = 76.54321
		radius = 1

		# prepopulate geo members
		asyncio.run(self.fake_redis.geoadd('device_locations', (lon, lat, 'channel-A')))
		asyncio.run(self.fake_redis.geoadd('device_locations', (lon + 0.001, lat + 0.001, 'channel-B')))

		# first broadcast
		asyncio.run(self.consumer.broadcast_alert(alert, lat, lon, radius, class_id=1, alert_id=None))
		self.assertEqual(len(self.layer.sent), 2)

		# clear
		self.layer.sent.clear()

		# second broadcast should be skipped
		asyncio.run(self.consumer.broadcast_alert(alert, lat, lon, radius, class_id=1, alert_id=None))
		self.assertEqual(len(self.layer.sent), 0)

	def test_alert_dedupe_with_alert_id(self):
		alert = {"msg": "heavy traffic", "severity": 2}
		lat = 10.0
		lon = 20.0
		radius = 1

		asyncio.run(self.fake_redis.geoadd('device_locations', (lon, lat, 'channel-A')))

		asyncio.run(self.consumer.broadcast_alert(alert, lat, lon, radius, class_id=2, alert_id='alert-123'))
		self.assertEqual(len(self.layer.sent), 1)

		self.layer.sent.clear()

		asyncio.run(self.consumer.broadcast_alert(alert, lat, lon, radius, class_id=2, alert_id='alert-123'))
		self.assertEqual(len(self.layer.sent), 0)


