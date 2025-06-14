import json
import logging
import redis
from rest_framework import status
from accounts.utils import find_team_owner_if_exists
from backend.permissions import BasicAuth
from core.models import Cluster
from core import exceptions
# from dist_tracing.serializers import TraceListSerializer, TraceRetrieveSerializer
from core.utils import alaz_set_instance_as_active, check_monitoring_read_permission, check_monitoring_write_permission, check_user_instance_count, get_cluster, silk_wrapper, validate_alaz_post_method
from accounts.models import User
from core import throttling
from rest_framework.exceptions import Throttled
# from dist_tracing.utils import get_trace_filters
# from dist_tracing.models import Span, Trace, Traffic
from rest_framework.generics import GenericAPIView
from django.conf import settings
from rest_framework.response import Response
from datetime import datetime, UTC
from django.utils import timezone


redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0, charset="utf-8", decode_responses=True)
logger = logging.getLogger(__name__)
# Create your views here.

class SpanView(GenericAPIView):
    throttle_classes = (throttling.ConcurrencyThrottleGetApiKey, )

    def get_user(self):
        user_id = self.request.query_params.get('user_id', None)
        if not user_id:
            raise exceptions.InvalidRequestError({"msg": "user_id is required"})
        try:
            user = User.objects.get(id=user_id)
        except Exception:
            raise exceptions.InvalidRequestError({"msg": "Invalid user_id"})
        return user

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def delete(self, request, *args, **kwargs):
        return Response({"msg": "Distributed tracing is disabled"}, status=status.HTTP_200_OK)
        user = self.get_user()
        if 'span_id' not in kwargs:
            raise exceptions.InvalidRequestError({"msg": "span_id is required"})
        span_id = kwargs['span_id']

        try:
            if user.team:
                span = Span.objects.using('default').get(span_id=span_id, cluster__user__team=user.team)
            else:
                span = Span.objects.using('default').get(id=span_id, cluster__user=user, cluster__user__team=None)
        except Exception:
            raise exceptions.InvalidRequestError({"msg": "Invalid span_id"})

        cluster = get_cluster(span.cluster)
        check_monitoring_write_permission(user, cluster)
        
        span.delete()

        return Response({"msg": "Span is deleted"}, status=status.HTTP_200_OK)


class TraceListView(GenericAPIView):
    throttle_classes = (throttling.ConcurrencyThrottleGetApiKey, )
    permission_classes = [BasicAuth]
    # pagination_class = PaginationWithPageNumber

    def get_user(self):
        user_id = self.request.query_params.get('user_id', None)
        if not user_id:
            raise exceptions.InvalidRequestError({"msg": "user_id is required"})
        try:
            user = User.objects.get(id=user_id)
        except Exception:
            raise exceptions.InvalidRequestError({"msg": "Invalid user_id"})
        return user

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    # @silk_wrapper(name='Trace-List')
    def post(self, request):
        return Response({"msg": "Distributed tracing is disabled"}, status=status.HTTP_200_OK)
        user = self.get_user()
        body = request.data
        limit = body.get('limit', settings.DIST_TRACING_TRACE_LIST_DEFAULT)
        try:
            limit = int(limit)
            limit = min(limit, settings.DIST_TRACING_TRACE_LIST_LIMIT)
        except Exception:
            raise exceptions.InvalidRequestError({"msg": "limit is not valid"})
        
        filters = get_trace_filters(user, self.request)

        traces = Trace.objects.filter(filters).order_by('-start_time')[:limit]

        chart = {}
        for trace in traces:
            start_time_ts = int(trace.start_time.timestamp() * 1000)
            chart[trace.id] = {
                "timestamp": start_time_ts,
                "name": trace.name,
                "id": str(trace.id),
                "duration_ms": trace.duration_ms,
                "span_count": trace.span_count,
            }

        serializer = TraceListSerializer(traces, many=True)
        response = {
            'list': serializer.data,
            'chart': list(chart.values())
        }
        return Response(response, status=status.HTTP_200_OK)


class TraceRetrieveView(GenericAPIView):
    throttle_classes = (throttling.ConcurrencyThrottleGetApiKey, )
    permission_classes = [BasicAuth]

    def get_user(self):
        user_id = self.request.query_params.get('user_id', None)
        if not user_id:
            raise exceptions.InvalidRequestError({"msg": "user_id is required"})
        try:
            user = User.objects.get(id=user_id)
        except Exception:
            raise exceptions.InvalidRequestError({"msg": "Invalid user_id"})
        return user

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def get(self, request, *args, **kwargs):
        return Response({"msg": "Distributed tracing is disabled"}, status=status.HTTP_200_OK)
        user = self.get_user()
        if 'trace_id' not in kwargs:
            raise exceptions.InvalidRequestError({"msg": "trace_id is required"})
        trace_id = kwargs['trace_id']

        try:
            trace = Trace.objects.get(id=trace_id)
        except Exception:
            raise exceptions.InvalidRequestError({"msg": "Invalid trace_id"})

        cluster = get_cluster(trace.cluster)
        check_monitoring_read_permission(user, cluster)

        serializer = TraceRetrieveSerializer(trace, context={'monitoring_id': cluster.monitoring_id})
        response = serializer.data

        return Response(response, status=status.HTTP_200_OK)

    def delete(self, request, *args, **kwargs):
        return Response({"msg": "Distributed tracing is disabled"}, status=status.HTTP_200_OK)
        user = self.get_user()
        if 'trace_id' not in kwargs:
            raise exceptions.InvalidRequestError({"msg": "trace_id is required"})
        trace_id = kwargs['trace_id']

        try:
            trace = Trace.objects.using('default').get(id=trace_id)
        except Exception:
            raise exceptions.InvalidRequestError({"msg": "Invalid trace_id"})
        
        cluster = get_cluster(trace.cluster)
        check_monitoring_write_permission(user, cluster)

        trace.delete()

        return Response({"msg": "Trace is deleted"}, status=status.HTTP_200_OK)


class TraceMetadataView(GenericAPIView):
    throttle_classes = (throttling.ConcurrencyThrottleGetApiKey, )
    permission_classes = [BasicAuth]

    def get_user(self):
        user_id = self.request.query_params.get('user_id', None)
        if not user_id:
            raise exceptions.InvalidRequestError({"msg": "user_id is required"})
        try:
            user = User.objects.get(id=user_id)
        except Exception:
            raise exceptions.InvalidRequestError({"msg": "Invalid user_id"})
        return user
    
    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})
    
    # @silk_wrapper(name='Trace-Metadata')
    def get(self, request, *args, **kwargs):
        return Response({"msg": "Distributed tracing is disabled"}, status=status.HTTP_200_OK)
        monitoring_id = self.request.query_params.get('monitoring_id', None)
        if monitoring_id:
            try:
                cluster = Cluster.objects.get(monitoring_id=monitoring_id)
            except Exception:
                raise exceptions.InvalidRequestError({"msg": "Invalid monitoring id"})
        else:
            raise exceptions.InvalidRequestError({"msg": "Monitoring id is required"})
        
        start_time = self.request.query_params.get('start_time', None)
        end_time = self.request.query_params.get('end_time', None)
        
        if not start_time:
            raise exceptions.InvalidRequestError({"msg": "Start time is required"})
        if not end_time:
            raise exceptions.InvalidRequestError({"msg": "End time is required"})
        
        try:
            start_time = datetime.fromtimestamp(int(start_time) / 1000)
            end_time = datetime.fromtimestamp(int(end_time) / 1000)
        except Exception:
            raise exceptions.InvalidRequestError({"msg": "Start time or end time is not valid"})
        
        traces = Trace.objects.filter(cluster=cluster.uid, start_time__gte=start_time, end_time__lte=end_time)
        operations = traces.values_list('name', flat=True).distinct()
        # operations = Trace.objects.filter(cluster=cluster).values_list('name', flat=True).distinct()

        # resources_query = Span.objects.filter(cluster=cluster, trace__isnull=False).values(
        #     from_uid=F('attributes__from_uid'),
        #     from_type=F('attributes__from_type'),
        #     from_name=F('attributes__from_name'),
        #     to_uid=F('attributes__to_uid'),
        #     to_type=F('attributes__to_type'),
        #     to_name=F('attributes__to_name'),
        # )

        # resources_dict = {}

        # for resource in resources_query:
        #     if 'from_name' not in resource or resource['from_name'] is None or resource['from_name'] == '':
        #         logger.fatal(f"from_name is not valid: {resource}")
        #     else:
        #         if resource['from_type'] not in resources_dict:
        #             resources_dict[resource['from_type']] = {}
        #         if resource['from_uid'] not in resources_dict[resource['from_type']]:
        #             resources_dict[resource['from_type']][resource['from_uid']] = {
        #                 'name': resource['from_name'],
        #                 'type': resource['from_type'],
        #                 'uid': resource['from_uid']
        #             }

        #     if 'to_name' not in resource or resource['to_name'] is None or resource['to_name'] == '':
        #         logger.fatal(f"to_name is not valid: {resource}")
        #     else:
        #         if resource['to_type'] not in resources_dict:
        #             resources_dict[resource['to_type']] = {}
        #         if resource['to_uid'] not in resources_dict[resource['to_type']]:
        #             resources_dict[resource['to_type']][resource['to_uid']] = {
        #                 'name': resource['to_name'],
        #                 'type': resource['to_type'],
        #                 'uid': resource['to_uid']
        #             }

        # resources = {}
        # for resource_type in resources_dict:
        #     if resource_type not in resources:
        #         resources[resource_type] = []
        #     for resource_uid in resources_dict[resource_type]:
        #         resources[resource_type].append(resources_dict[resource_type][resource_uid])

        log_time = datetime.now(UTC)
        response = {
            'operations': list(operations),
            # 'resources': resources
        }
        logger.info(f"Trace metadata response: {response} at {log_time}")

        # TODO: Add data for autocomplete

        return Response(response, status=status.HTTP_200_OK)

class TrafficView(GenericAPIView):
    throttling_classes = (throttling.ConcurrencyAlazTrafficThrottle)

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})
    
    @silk_wrapper(name='Traffic')
    def post(self, request, *args, **kwargs):
        return Response({"msg": "Distributed tracing is disabled"}, status=status.HTTP_200_OK)
        # logger.info(f'{len(request.data['traffic'])} traffic received')
        # return Response({"msg": "Traffic are processed"}, status=status.HTTP_200_OK)
        # verbose_log(f'Traffic payload: {request.data}')
        cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='traffic')
        data = request.data
        
        batch_size = settings.BULK_BATCH_SIZE

        if settings.ANTEON_ENV != 'onprem':
            if not find_team_owner_if_exists(cluster.user).active:
                return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)
        
        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Alaz traffic are already processed"}, status=status.HTTP_200_OK)

        instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)

        traffic = data["traffic"]
        bulk_process = []
        # response = {'msg': 'Alaz traffic are processed', 'errors': []}
        response = {'msg': 'Alaz traffic are processed'}
        item_num = 1
        # last_data_ts = None
        # return Response({"msg": "Traffic are processed"}, status=status.HTTP_200_OK)
        for item in traffic:
            # Fields do not have keys, their order is:
            # 1- timestamp
            # 2- tcp_seq_num
            # 3- thread_id
            # 4- ingress

            try:
                if len(item) != 4:
                    raise exceptions.InvalidRequestError({"msg": "Traffic fields are not valid"})

                timestamp = item[0]
                tcp_seq_num = item[1]
                if tcp_seq_num == 0:
                    continue
                thread_id = item[2]
                ingress = item[3]
                node_id = instance

                kwargs = {
                    'cluster_id': str(cluster.uid),
                    'timestamp': timestamp,
                    'tcp_seq_num': tcp_seq_num,
                    'thread_id': thread_id,
                    'ingress': ingress,
                    'node_id': node_id
                }

                bulk_process.append(json.dumps(kwargs))

                if len(bulk_process) >= batch_size:
                    try:
                        redis_client.rpush(settings.DIST_TRACING_TRAFFIC_QUEUE, *bulk_process)
                    except Exception as exc:
                        logger.fatal(f"Redis rpush failed: {exc}")
                        raise exceptions.InvalidRequestError({"msg": "Redis rpush failed"})

                    bulk_process = []

            except Exception as exc:
                # errors = str(exc.args)
                # response['errors'].append({'traffic_num': item_num, 'traffic': item, 'error': errors})
                logger.error(f"Traffic item is not valid: {item}")
                continue

            item_num += 1


        start_time = datetime.now()
        if len(bulk_process) > 0:
            try:
                redis_client.rpush(settings.DIST_TRACING_TRAFFIC_QUEUE, *bulk_process)
            except Exception as exc:
                logger.fatal(f"Redis rpush failed: {exc}")
                raise exceptions.InvalidRequestError({"msg": "Redis rpush failed"})

        redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 1)
        redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

        # if 'errors' in response and len(response['errors']) > 0:
        #     print(response)

        return Response(response, status=status.HTTP_200_OK)
                

