from django.contrib.auth import get_user_model
from django.core.cache import caches
from django.conf import settings
from rest_framework.throttling import SimpleRateThrottle


# Throttling for the backend

class ConcurrencyThrottleApiKey(SimpleRateThrottle):
    cache = caches[settings.CONCURRENCY_THROTTLE_NAME]  # redis, see settings.py
    rate = settings.CONCURRENCY_THROTTLE_RATE    # do not delete, required for initial self.num_requests and self.duration calculation in SimpleRateThrottle

    def get_cache_key(self, request, view):
        api_key = request.META.get('HTTP_X_API_KEY')
        if not api_key:
            api_key = request.META.get('X-API-KEY')

        if not api_key:
            return "9999"   # anonym

        try:
            get_user_model().objects.get(api_key=api_key)
        except Exception as e:
            return "9999"

        return api_key


# Throttling for Alaz
    
class ConcurrencyThrottleGetApiKey(ConcurrencyThrottleApiKey):
    rate = settings.CONCURRENCY_GET_THROTTLE_RATE

class ConcurrencyAlazScrapePostThrottle(SimpleRateThrottle):
    cache = caches[settings.CONCURRENCY_THROTTLE_NAME]  # redis, see settings.py

    def get_cache_key(self, request, view):
        default_monitoring_id = '9998'
        default_node_id = 'node_id'

        monitoring_id = request.query_params.get('monitoring_id', default_monitoring_id)  # 9998 is anonym
        node_id = request.query_params.get('node_id', default_node_id)

        return f'{monitoring_id}_{node_id}_scrape_post'
    
    rate = settings.CONCURRENCY_ALAZ_DEFAULT_THROTTLE_RATE

class ConcurrencyAlazDefaultThrottleApiKey(SimpleRateThrottle):
    cache = caches[settings.CONCURRENCY_THROTTLE_NAME]  # redis, see settings.py

    def get_cache_key(self, request, view):
        monitoring_id = '9998'
        node_id = 'node_id'

        if 'metadata' in request.data:
            if 'monitoring_id' in request.data['metadata']:
                monitoring_id = request.data['metadata']['monitoring_id']
            if 'node_id' in request.data['metadata']:
                node_id = request.data['metadata']['node_id']

        return f'{monitoring_id}_{node_id}_default'
    
    rate = settings.CONCURRENCY_ALAZ_DEFAULT_THROTTLE_RATE

# class ConcurrencyAlazTCPDumpThrottleApiKey(SimpleRateThrottle):
#     cache = caches[settings.CONCURRENCY_THROTTLE_NAME]  # redis, see settings.py

#     def get_cache_key(self, request, view):
#         monitoring_id = '9998'
#         node_id = 'node_id'

#         if 'metadata' in request.data:
#             if 'monitoring_id' in request.data['metadata']:
#                 monitoring_id = request.data['metadata']['monitoring_id']
#             if 'node_id' in request.data['metadata']:
#                 node_id = request.data['metadata']['node_id']

#         return f'{monitoring_id}_{node_id}_default'
    
#     if settings.TCPDUMP_ENABLED:
#         rate = settings.TCPDUMP_THROTTLE_RATE
#     else:
#         rate = settings.CONCURRENCY_ALAZ_DEFAULT_THROTTLE_RATE

class ConcurrencyAlazHealthCheckThrottleApiKey(SimpleRateThrottle):
    cache = caches[settings.CONCURRENCY_THROTTLE_NAME]  # redis, see settings.py

    def get_cache_key(self, request, view):
        monitoring_id = '9998'
        node_id = 'node_id'

        if 'metadata' in request.data:
            if 'monitoring_id' in request.data['metadata']:
                monitoring_id = request.data['metadata']['monitoring_id']
            if 'node_id' in request.data['metadata']:
                node_id = request.data['metadata']['node_id']

        return f'{monitoring_id}_{node_id}_health_check'
    
    rate = settings.CONCURRENCY_ALAZ_HEALTH_CHECK_THROTTLE_RATE

class ConcurrencyTelemetryThrottle(SimpleRateThrottle):
    cache = caches[settings.CONCURRENCY_THROTTLE_NAME]  # redis, see settings.py

    def get_cache_key(self, request, view):
        onprem_key = 'generic_onprem'
        if 'onprem_key' in request.data:
            onprem_key = request.data['onprem_key']
        return f'telemetry_{onprem_key}'
    
    rate = settings.CONCURRENCY_TELEMETRY_THROTTLE_RATE


class ConcurrencyAlazTrafficThrottle(SimpleRateThrottle):
    cache = caches[settings.CONCURRENCY_THROTTLE_NAME]  # redis, see settings.py

    def get_cache_key(self, request, view):
        monitoring_id = '9998'
        node_id = 'node_id'

        if 'metadata' in request.data:
            if 'monitoring_id' in request.data['metadata']:
                monitoring_id = request.data['metadata']['monitoring_id']
            if 'node_id' in request.data['metadata']:
                node_id = request.data['metadata']['node_id']

        return f'{monitoring_id}_{node_id}_traffic'
    
    rate = settings.CONCURRENCY_ALAZ_TRAFFIC_THROTTLE_RATE