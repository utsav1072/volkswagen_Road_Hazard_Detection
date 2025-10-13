from django.urls import re_path
from .consumers import TestConsumer, AlertConsumer

websocket_urlpatterns = [
    re_path("test/", TestConsumer.as_asgi()),
    re_path("ws/alerts/", AlertConsumer.as_asgi()),
]
