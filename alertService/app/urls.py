from django.urls import path
from .views import helloWorld

urlpatterns = [
    path('', helloWorld, name='hello_world'),
]
