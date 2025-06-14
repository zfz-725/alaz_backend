import datetime
import logging
from rest_framework.generics import GenericAPIView

from pricing.utils import get_storage_usages
from core.models import Cluster
from pricing.models import MetricCount
from core import exceptions
from core import throttling
from backend.permissions import BasicAuth
from rest_framework.exceptions import Throttled
from django.contrib.auth import get_user_model
from rest_framework.response import Response
from rest_framework import status
from django.db.models import Sum

logger = logging.getLogger(__name__)

class MetricCountView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})
    
    def get(self, request, *args, **kwargs):
        # Group metric counts by user
        metric_counts_qs = MetricCount.objects.using('default').values('user_id').annotate(total_count=Sum('count'))

        logger.info(f'Aggregated metric counts in the db')
        metric_counts = {str(item['user_id']): item['total_count'] for item in metric_counts_qs}

        logger.info(f'Processed the metric counts to a dictionary')
        metric_counts_qs._raw_delete(metric_counts_qs.db)
        logger.info(f'Deleted metric counts')
                
        return Response(metric_counts, status=status.HTTP_200_OK)
     

class StorageUsageView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]
    
    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})
    
    def get(self, request, *args, **kwargs):
        storage_usages = get_storage_usages()
        return Response(storage_usages, status=status.HTTP_200_OK)
        