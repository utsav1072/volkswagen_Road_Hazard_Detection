from django.urls import re_path
from .consumers import TestConsumer

websocket_urlpatterns = [
    re_path("test/", TestConsumer.as_asgi())
]
