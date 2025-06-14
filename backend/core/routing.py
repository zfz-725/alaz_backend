from django.urls import re_path
from core.consumers import LogConsumer


websocket_urlpatterns = [
    # re_path(r'ws\/logs\/(?P<monitoring_id>[^\/]+)\/(?P<token>[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+)\/(?P<pod_uid>[a-zA-Z0-9-]+)\/(?P<container_name>[a-zA-Z0-9-]+)\/(?P<container_num>[0-9]+)\/((?P<start_time>\d{13})\/)?$', LogConsumer.as_asgi())
    re_path(r'ws\/logs\/(?P<monitoring_id>[^\/]+)\/(?P<token>[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+)\/(?P<pod_uid>[a-zA-Z0-9-]+)\/(?P<container_name>[a-zA-Z0-9-]+)\/(?P<container_num>[a-zA-Z0-9-]+)\/((?P<start_time>\d{13})\/)?$', LogConsumer.as_asgi())

]