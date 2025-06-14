from django.conf import settings
import redis
from core import exceptions

redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0, charset="utf-8", decode_responses=True)


def metric_used(metric, setting):
    if not setting:
        raise exceptions.NotFoundError('Setting not found')

    used_metrics = setting.used_metrics
    if metric in used_metrics:
        return True
    else:
        return False

def get_storage_usages():
    response = {}
    with redis_client.pipeline() as pipe:
        pipe.hgetall(settings.REQUESTS_SIZE_KEY)
        pipe.delete(settings.REQUESTS_SIZE_KEY)
        requests_usage = pipe.execute()[0]

    for cluster_id, size in requests_usage.items():
        cluster_id = str(cluster_id)
        if cluster_id not in response:
            response[cluster_id] = {}
        response[cluster_id]['requests_bytes'] = size
        
    with redis_client.pipeline() as pipe:
        pipe.hgetall(settings.CONNECTIONS_SIZE_KEY)
        pipe.delete(settings.CONNECTIONS_SIZE_KEY)
        connections_usage = pipe.execute()[0]

    for cluster_id, size in connections_usage.items():
        cluster_id = str(cluster_id)
        if cluster_id not in response:
            response[cluster_id] = {}
        response[cluster_id]['connections_bytes'] = size

    with redis_client.pipeline() as pipe:
        pipe.hgetall(settings.TRACES_SIZE_KEY)
        pipe.delete(settings.TRACES_SIZE_KEY)
        traces_usage = pipe.execute()[0]
        
    for cluster_id, size in traces_usage.items():
        cluster_id = str(cluster_id)
        if cluster_id not in response:
            response[cluster_id] = {}
        response[cluster_id]['traces_bytes'] = size
        
    with redis_client.pipeline() as pipe:
        pipe.hgetall(settings.LOGS_SIZE_KEY)
        pipe.delete(settings.LOGS_SIZE_KEY)
        logs_usage = pipe.execute()[0]

    for cluster_id, size in logs_usage.items():
        cluster_id = str(cluster_id)
        if cluster_id not in response:
            response[cluster_id] = {}
        response[cluster_id]['logs_bytes'] = size

    return response