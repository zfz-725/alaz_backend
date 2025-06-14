import time
from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
from datetime import datetime, UTC
import json
import logging
import math
from django.db.models import Q, Count, Window, F, Case, When, Value, CharField, IntegerField, BooleanField
from django.db.models.functions import TruncSecond, RowNumber, Coalesce
import uuid
from urllib.parse import urlparse
from django.http import HttpResponse

import redis
from analytics.models import Telemetry
from accounts.models import User
from django.conf import settings
from accounts.utils import find_team_owner_if_exists
from pricing.utils import metric_used
from pricing.models import MetricCount
from core.clickhouse_utils import get_all_logs
from backend.permissions import BasicAuth
from core import exceptions, throttling
from core.models import (DaemonSet, Cluster, Container, Deployment, Endpoint, K8sEvent, KafkaEvent, Pod, ReplicaSet, Request, RequestType, Service, Setting, Connection, StatefulSet)
# from core.pagination import SchedulerResultPagination
from core.requester import PrometheusRequester
from core.serializers import (ClusterDetailSerializer, ClusterSerializer, DaemonSetSerializer, DaemonSetSummarySerializer, DeploymentSerializer, DeploymentSummarySerializer, LatestAlazVersionSerializer, 
                              OutboundSerializer, OutboundSummarySerializer, PodSerializer, PodSummarySerializer, RequestSerializer, ServiceSerializer, ServiceSummarySerializer, StatefulSetSerializer, StatefulSetSummarySerializer)
from core.utils import (Outbound, RawAnnotation, alaz_add_connected_resources_if_any, alaz_add_used_port_to_resource, alaz_assign_most_frequent_protocol_to_resources, alaz_find_all_namespaces, alaz_find_destination_node, alaz_find_pointed_destinations_of_service, alaz_find_services_pointing_to_pod, alaz_find_source_node, alaz_find_target_port_of_service, alaz_keep_count_of_protocol, alaz_process_node_cpu_seconds_total_data, alaz_process_prometheus_data, alaz_request_find_destination_node, alaz_mark_zombie_resources, alaz_request_find_source_node, alaz_set_instance_as_active, cache_alaz_resources, check_monitoring_read_permission, check_monitoring_write_permission, check_user_instance_count, 
                        get_clusters_and_cluster_count, get_default_setting, get_grouping_expr, get_grouping_interval, get_instances_of_cluster, get_last_data_ts_of_user, get_log_sources, get_monitoring_id,
                        get_pods_of_daemonset, get_pods_of_replicaset, get_pods_of_statefulset, get_replicasets_of_deployment, get_resources_of_cluster, limit_active_instances_of_user,
                        log_endpoint, prepare_api_catalog_selected_filters, prepare_k8s_events_selected_filters, remove_values_from_postgres_query, revert_group_if_required, save_cluster, silk_wrapper, simplify_http_endpoint,
                        simplify_postgres_query, remove_values_from_redis_query, string_is_ascii, validate_metrics_get_method, validate_monitoring_id, lightweight_validate_alaz_post,
                        validate_service_map_get_method, validate_alaz_post_method, validate_start_and_end_times, verbose_log, mute_gpu_metrics_after, parse_resp)
from django.contrib.auth import get_user_model
from django.db import IntegrityError, connection
from django.db.models import Count, Sum, Avg
from django.utils import timezone
from django.utils.timezone import now, timedelta
from rest_framework import mixins, permissions, status, viewsets
from rest_framework.exceptions import Throttled
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response

# from backend.permissions import (HasAPIKeyorIsAuthenticatedandEmailConfirmed,
#                                  HasInternalAccessAPIKey,
#                                  HasJWTPermissionOrOnPrem, IsOnPrem, OnPremNotEnabledDistributedMode, OnPremNotUpgradedToEnterprise, RootUserExists)

logger = logging.getLogger(__name__)
redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0, charset="utf-8", decode_responses=True)

type_kafka_event = RequestType.KAFKA_EVENT
type_request = RequestType.REQUEST

# TODO: Add user ids for validation here
class ClusterView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def get(self, request):
        verbose_log(f'GET Clusters request: {request.query_params}')
        user_id = request.query_params.get('user_id', None)
        if not user_id:
            raise exceptions.ValidationError({"msg": "User id is required"})
        try:
            verbose_log(f'Received user_id: {user_id}')
            user = User.objects.get(id=user_id)
        except Exception:
            raise exceptions.NotFoundError({'msg': 'User id is invalid'})

        clusters = get_clusters_and_cluster_count(user)

        job = 'alaz'
        result = {}
        verbose_log(f'Will send request to prometheus')
        prometheus_requester = PrometheusRequester()
        for cluster in clusters:
            result[cluster.uid] = {}
            instances = get_instances_of_cluster(cluster)
            payload = {
                'utilized_cpu_count': 0,
                'total_cpu_count': 0,
                'utilized_memory_bytes': 0,
                'total_memory_bytes': 0,
                'utilized_filesystem_bytes': 0,
                'total_filesystem_bytes': 0,
                'total_node_count': 0,
                'pod_count': Pod.objects.filter(cluster=cluster.uid, deleted=False).count(),
            }
            if len(instances) == 0:
                payload = {
                    'utilized_cpu_count': None,
                    'total_cpu_count': None,
                    'utilized_memory_bytes': None,
                    'total_memory_bytes': None,
                    'utilized_filesystem_bytes': None,
                    'total_filesystem_bytes': None,
                    'total_node_count': None,
                    'pod_count': None,
                }
            for instance in instances:
                data = prometheus_requester.get_all_cluster_summary(instance, cluster.monitoring_id, job)
                processed_data = alaz_process_prometheus_data(data, valid_until=user.passive_from)
                for field, data_dict in processed_data.items():
                    if type(data_dict) != dict or len(data_dict) == 0:
                        # print(f'No metric data found for field {field} in cluster {cluster.uid}')
                        continue
                    # Assuming we have a single data
                    # Get the single value of a dictionary
                    metric_data = list(data_dict.values())[0]
                    if metric_data == 'NaN':
                        continue
                    metric_data = float(metric_data)
                    if field in payload:
                        payload[field] += metric_data
                    else:
                        payload[field] = metric_data
                payload['total_node_count'] += 1

            cluster.summary_data = payload

        verbose_log(f'Will serialize clusters')        
        serializer = ClusterDetailSerializer(clusters, many=True)
        response = {
            'clusters': serializer.data
        }
        verbose_log(f'Will send response')
        return Response(response, status=status.HTTP_200_OK)

    def put(self, request):
        monitoring_id = request.query_params.get('monitoring_id', None)
        if not monitoring_id:
            raise exceptions.ValidationError({"msg": "Monitoring id is required"})

        cluster = get_monitoring_id(monitoring_id)
        if not cluster:
            raise exceptions.ValidationError({"msg": f"Cluster for the monitoring id is not found: {monitoring_id}"})

        if 'name' not in request.data:
            raise exceptions.ValidationError({"msg": "Name is required"})

        if 'owner' in request.data:
            raise exceptions.ValidationError({"msg": "The owner of the cluster can not be changed"})

        if 'team' in request.data:
            raise exceptions.ValidationError({"msg": "The team of the cluster can not be changed"})

        if 'fresh' in request.data:
            raise exceptions.ValidationError({"msg": "The status of the cluster can not be changed"})

        cluster_serializer = ClusterSerializer(cluster, data=request.data, partial=True)
        cluster_serializer.is_valid(raise_exception=True)
        cluster_serializer.save()
        return Response(cluster_serializer.data, status=status.HTTP_200_OK)

    def post(self, request):
        request_data = request.data
        user_id = request_data.get('user_id', None)
        if not user_id:
            raise exceptions.NotFoundError({'msg': "User id is required"})
        try:
            user = User.objects.get(id=user_id)
        except Exception:
            raise exceptions.NotFoundError({'msg': 'User id is invalid'})

        request_data['user'] = user.id
        request_data['team'] = user.team.id if user.team else None
        request_data['is_alive'] = False
        serializer = ClusterSerializer(data=request_data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def delete(self, request):
        monitoring_id = request.query_params.get('monitoring_id', None)
        if not monitoring_id:
            raise exceptions.ValidationError({"msg": "Monitoring id is required"})

        if settings.LOCK_CLUSTER_UPDATE:
            with transaction.atomic():
                try:
                    cluster = Cluster.objects.select_for_update().get(monitoring_id=monitoring_id)
                except ObjectDoesNotExist:
                    raise exceptions.NotFoundError({"msg": f"Cluster for the monitoring id is not found: {monitoring_id}"})

                logger.info(f'Deleting cluster {cluster.uid}')

                cluster.delete()

                logger.info(f'Cluster {cluster.uid} is deleted')
        else:
            cluster = get_monitoring_id(monitoring_id)
            if not cluster:
                raise exceptions.ValidationError({"msg": f"Cluster for the monitoring id is not found: {monitoring_id}"})

            cluster.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class ClusterRetrieveView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def get(self, request, *args, **kwargs):
        monitoring_id = kwargs.get('monitoring_id', None)
        if not monitoring_id:
            raise exceptions.ValidationError({"msg": "Monitoring id is required"})

        cluster = get_monitoring_id(monitoring_id)
        if not cluster:
            raise exceptions.ValidationError({"msg": "Monitoring id is invalid"})

        serializer = ClusterSerializer(cluster)
        return Response(serializer.data, status=status.HTTP_200_OK)


class ClusterDetailsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def get(self, request, *args, **kwargs):
        monitoring_id = kwargs.get('monitoring_id', None)
        if not monitoring_id:
            raise exceptions.ValidationError({"msg": "Monitoring id is required"})

        cluster = get_monitoring_id(monitoring_id)
        if not cluster:
            raise exceptions.ValidationError({"msg": "Monitoring id is invalid"})

        serializer = ClusterDetailSerializer(cluster)
        return Response(serializer.data, status=status.HTTP_200_OK)


class ServiceMapView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    # @silk_wrapper(name='Service-Map-Summary')
    def get(self, request):
        all_start_time = datetime.now()
        
        preparation_start = datetime.now()
        cluster, start_time_ts, end_time_ts = validate_service_map_get_method(request=request)

        namespaces = request.query_params.get('namespaces', None)
        namespaces = namespaces.split(',') if namespaces else None
        get_all_resources = request.query_params.get('get_all_resources', 'false')
        if get_all_resources == 'true':
            get_all_resources = True
        else:
            get_all_resources = False

        get_connections = request.query_params.get('connections', 'false')
        if get_connections == 'true':
            get_connections = True
        else:
            get_connections = False

        # Convert start_time str to timestamp in ms
        start_time_ts = int(start_time_ts / 1000)
        end_time_ts = int(end_time_ts / 1000)
        end_time = datetime.fromtimestamp(end_time_ts)
        start_time = datetime.fromtimestamp(start_time_ts)

        cluster_serializer = ClusterSerializer(cluster)
        response = {"cluster": cluster_serializer.data,
                    'pods': {},
                    'services': {},
                    'deployments': {},
                    'daemon_sets': {},
                    'stateful_sets': {},
                    'outbounds': {},
                    'traffic': [],
                    'avg_latency': 0,
                    'total_rps': 0,
                    'all_namespaces': []
                    }
        
        cached_replica_sets, cached_deployments, cached_pods, cached_endpoints, cached_services, cached_daemon_sets, cached_outbounds, endpoint_to_service, cached_stateful_sets = cache_alaz_resources(
            cluster, namespaces)

        all_namespaces = alaz_find_all_namespaces(cluster)
        response['all_namespaces'] = all_namespaces

        resources = {'pods': {}, 'services': {}, 'deployments': {}, 'daemon_sets': {}, 'outbounds': {}, 'stateful_sets': {}}

        preparation_time_sec = (datetime.now() - preparation_start).total_seconds()

        # if settings.SERVICE_MAP_VERBOSE_LOG:
            # print('**VERBOSE_LOG: Requests qs:')
            # print(requests.explain())

        requests_grouping_start = datetime.now()

        grouped_requests = Request.objects.filter(cluster=cluster.uid, start_time__gte=start_time, start_time__lte=end_time).values(
            'from_uid_pod',
            'from_uid_service',
            # 'from_uid_deployment',
            # 'from_uid_statefulset',
            # 'from_uid_daemonset',
            'to_uid_pod',
            'to_uid_service',
            # 'to_uid_deployment',
            # 'to_uid_statefulset',
            # 'to_uid_daemonset',
            'to_url_outbound',
            'protocol',
            'status_code',  # This could be removed and retrieved separately to improve the speed
            # A way to do it is to retrieve distinct status codes 
            # Indexing on status code could help a lot in this case
            'path',
            'to_port').annotate(
                count=Count('id'),
                sum_latency=Sum('latency')
            )
        
        requests_grouping_time_sec = (datetime.now() - requests_grouping_start).total_seconds()
        
        kafka_events_grouping_start = datetime.now()
        
        grouped_kafka_events = KafkaEvent.objects.filter(cluster=cluster.uid, start_time__gte=start_time, start_time__lte=end_time).values(
            'from_uid_pod',
            'from_uid_service',
            # 'from_uid_deployment',
            # 'from_uid_statefulset',
            # 'from_uid_daemonset',
            'to_uid_pod',
            'to_uid_service',
            # 'to_uid_deployment',
            # 'to_uid_statefulset',
            # 'to_uid_daemonset',
            'to_url_outbound',
            'to_port',
            'topic',
            'type'
        ).annotate(
            count=Count('id'),
            sum_latency=Sum('latency')
        )
    
        kafka_events_grouping_time_sec = (datetime.now() - kafka_events_grouping_start).total_seconds()

        endpoint_info = {}

        if settings.SERVICE_MAP_DURATION_LOG:
            verbose_log(f'Got {len(grouped_requests)} many request groups and {len(grouped_kafka_events)} many kafka event groups.')

        traffic = {}
        connections = {}
        edges = dict()
        sum_src_filter_time = 0
        sum_dest_filter_time = 0
        sum_remaining_time = 0
        requests_sum_time = 0
        pointed_sum_time = 0
        requests_start_time = datetime.now()
        total_loop_overhead = 0
        iteration_start_time = datetime.now()
        found_resources = {'pods': {}, 'services': {}, 'deployments': {}, 'daemon_sets': {}, 'outbounds': {}, 'stateful_sets': {}}
        # protocol_counts = {'pods': {}, 'services': {}, 'deployments': {}, 'daemon_sets': {}, 'outbounds': {}, 'stateful_sets': {}}
        total_count = 0
        total_latency = 0
        
        time_diff = (end_time - start_time).total_seconds() if end_time > start_time else 1

        requests_process_start = datetime.now()
        for group in grouped_requests:

            total_loop_overhead += (datetime.now() - iteration_start_time).total_seconds()

            request_start_time = datetime.now()

            # from_port = group['from_port']
            to_port = group['to_port']

            source_node_filtering_time = datetime.now()

            from_type, from_object = alaz_request_find_source_node(
                group, cached_pods, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_services, cached_stateful_sets)
            if from_type is None or from_object is None:
                if settings.VERBOSE_LOG:
                    logger.warn(f'From object is None for request. Skipping')
                continue

            sum_src_filter_time += (datetime.now() - source_node_filtering_time).total_seconds()

            found_resources[from_type + 's'][str(from_object.uid)] = from_object

            destination_node_filtering_time = datetime.now()

            to_type, to_object = alaz_request_find_destination_node(
                group, cached_pods, cached_services, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_stateful_sets, add_port=False)
            if to_type is None or to_object is None:
                if settings.VERBOSE_LOG:
                    logger.warn(f'To object is None for request. Skipping')
                continue

            sum_dest_filter_time += (datetime.now() - destination_node_filtering_time).total_seconds()

            remaining_start_time = datetime.now()

            rps = group['count'] / time_diff
            latency = group['sum_latency'] / group['count']

            total_count += group['count']
            total_latency += group['sum_latency']

            partial_edge_info = {
                "from_type": from_type,
                "from_id": str(from_object.uid),
                "to_port": to_port,
                "total_count": group['count'],
                "total_latency": group['sum_latency'],
                'protocols': {group['protocol']},
                'status_codes': {group['status_code']},
            }

            partial_edge_info['to_type'] = to_type
            partial_edge_info['to_id'] = str(to_object.uid)
            found_resources[to_type + 's'][str(to_object.uid)] = to_object
            identify_key = (from_type, str(from_object.uid),
                            to_type, str(to_object.uid), to_port)

            if identify_key not in endpoint_info:
                endpoint_info[identify_key] = {}
            if group['protocol'] not in endpoint_info[identify_key]:
                endpoint_info[identify_key][group['protocol']] = {}
            if group['path'] not in endpoint_info[identify_key][group['protocol']]:
                endpoint_info[identify_key][group['protocol']][group['path']] = {
                    'count': 0,
                    'sum_latency': 0,
                }
            endpoint_info[identify_key][group['protocol']][group['path']]['count'] += group['count']
            endpoint_info[identify_key][group['protocol']][group['path']]['sum_latency'] += group['sum_latency']

            if to_type == 'service':
                pointed_start_time = datetime.now()
                service = to_object
                found_resources, pointed = alaz_find_pointed_destinations_of_service(
                    service, group['to_port'], cached_pods, cached_endpoints, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets, found_resources)
                if pointed is None:
                    if settings.VERBOSE_LOG:
                        logger.warn(f'No pointed resources found for service {service.uid}. Skipping')
                    continue
                partial_edge_info['pointed'] = pointed
                pointed_sum_time += (datetime.now() - pointed_start_time).total_seconds()
            else:
                pointed = None

            # protocol_counts = alaz_keep_count_of_protocol(from_type, from_object,
            #     to_type, to_object, group['protocol'], protocol_counts, pointed, group['count'])

            if identify_key in edges:
                new_total_count = edges[identify_key]['total_count'] + group['count']
                latency_sum = edges[identify_key]['total_latency'] + group['sum_latency']
                edges[identify_key]['total_count'] = new_total_count
                edges[identify_key]['total_latency'] = latency_sum
                edges[identify_key]['status_codes'].add(group['status_code'])
                edges[identify_key]['protocols'].add(group['protocol'])
                if pointed:
                    edges[identify_key]['pointed'] = {}
                    for pointed_uid, pointed_obj in pointed.items():
                        # If the same to_port is redirected to different ports, this needs to be changed
                        edges[identify_key]['pointed'][pointed_uid] = pointed_obj
            else:
                edges[identify_key] = partial_edge_info

            sum_remaining_time += (datetime.now() - remaining_start_time).total_seconds()
            requests_sum_time += (datetime.now() - request_start_time).total_seconds()
            iteration_start_time = datetime.now()
        
        requests_duration_sec = (datetime.now() - requests_process_start).total_seconds()

        kafka_events_process_start = datetime.now()
        for group in grouped_kafka_events:

            total_loop_overhead += (datetime.now() - iteration_start_time).total_seconds()

            request_start_time = datetime.now()

            # from_port = group['from_port']
            to_port = group['to_port']

            source_node_filtering_time = datetime.now()

            from_type, from_object = alaz_request_find_source_node(
                group, cached_pods, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_services, cached_stateful_sets)
            if from_type is None or from_object is None:
                if settings.VERBOSE_LOG:
                    logger.warn(f'From object is None for kafka event. Skipping')
                continue

            sum_src_filter_time += (datetime.now() - source_node_filtering_time).total_seconds()

            found_resources[from_type + 's'][str(from_object.uid)] = from_object

            destination_node_filtering_time = datetime.now()

            to_type, to_object = alaz_request_find_destination_node(
                group, cached_pods, cached_services, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_stateful_sets, add_port=False)
            if to_type is None or to_object is None:
                if settings.VERBOSE_LOG:
                    logger.warn(f'To object is None for kafka event. Skipping')
                continue

            sum_dest_filter_time += (datetime.now() - destination_node_filtering_time).total_seconds()

            remaining_start_time = datetime.now()

            rps = group['count'] / time_diff
            latency = group['sum_latency'] / group['count']

            total_count += group['count']
            total_latency += group['sum_latency']

            group['protocol'] = type_kafka_event

            partial_edge_info = {
                "from_type": from_type,
                "from_id": str(from_object.uid),
                "to_port": to_port,
                "total_count": group['count'],
                "total_latency": group['sum_latency'],
                'protocols': {group['protocol']},
            }

            partial_edge_info['to_type'] = to_type
            partial_edge_info['to_id'] = str(to_object.uid)
            found_resources[to_type + 's'][str(to_object.uid)] = to_object
            identify_key = (from_type, str(from_object.uid),
                            to_type, str(to_object.uid), to_port)

            if identify_key not in endpoint_info:
                endpoint_info[identify_key] = {}
            if group['protocol'] not in endpoint_info[identify_key]:
                endpoint_info[identify_key][group['protocol']] = {}
            if group['type'] not in endpoint_info[identify_key][group['protocol']]:
                endpoint_info[identify_key][group['protocol']][group['type']] = {}
            if group['topic'] not in endpoint_info[identify_key][group['protocol']][group['type']]:
                endpoint_info[identify_key][group['protocol']][group['type']][group['topic']] = {
                    'count': 0,
                    'sum_latency': 0,
                }
            endpoint_info[identify_key][group['protocol']][group['type']][group['topic']]['count'] += group['count']
            endpoint_info[identify_key][group['protocol']][group['type']][group['topic']]['sum_latency'] += group['sum_latency']

            if to_type == 'service':
                pointed_start_time = datetime.now()
                service = to_object
                found_resources, pointed = alaz_find_pointed_destinations_of_service(
                    service, group['to_port'], cached_pods, cached_endpoints, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets, found_resources)
                if pointed is None:
                    if settings.VERBOSE_LOG:
                        logger.warn(f'No pointed resources found for service {service.uid}. Skipping')
                    continue
                partial_edge_info['pointed'] = pointed
                pointed_sum_time += (datetime.now() - pointed_start_time).total_seconds()
            else:
                pointed = None

            # protocol_counts = alaz_keep_count_of_protocol(from_type, from_object,
                # to_type, to_object, group['protocol'], protocol_counts, pointed, group['count'])

            if identify_key in edges:
                new_total_count = edges[identify_key]['total_count'] + group['count']
                latency_sum = edges[identify_key]['total_latency'] + group['sum_latency']
                edges[identify_key]['total_count'] = new_total_count
                edges[identify_key]['total_latency'] = latency_sum
                edges[identify_key]['protocols'].add(group['protocol'])
                if pointed:
                    edges[identify_key]['pointed'] = {}
                    for pointed_uid, pointed_obj in pointed.items():
                        # If the same to_port is redirected to different ports, this needs to be changed
                        edges[identify_key]['pointed'][pointed_uid] = pointed_obj
            else:
                edges[identify_key] = partial_edge_info

            sum_remaining_time += (datetime.now() - remaining_start_time).total_seconds()
            requests_sum_time += (datetime.now() - request_start_time).total_seconds()
            iteration_start_time = datetime.now()

        kafka_events_duration_sec = (datetime.now() - kafka_events_process_start).total_seconds()
            
        calculations_start = datetime.now()
        resource_count = 0  # if min_time is None and max_time is None, this will stay as 0
        hit_resource_threshold = False
        total_rps = total_count / time_diff
        avg_latency = total_latency / total_count if total_count > 0 else 0
        response['avg_latency'] = avg_latency
        response['total_rps'] = total_rps

        min_rps = None
        max_rps = None
        min_latency = None
        max_latency = None
        for identify_key, partial_edge_info in edges.items():
            # Check if from resource is in resources
            from_type = partial_edge_info['from_type']
            from_id = partial_edge_info['from_id']
            to_type = partial_edge_info['to_type']
            to_id = partial_edge_info['to_id']
            to_port = partial_edge_info['to_port']

            if hit_resource_threshold:
                # Take it only if its resources are present
                from_resource = resources[from_type + 's'].get(from_id, None)
                if from_resource is None:
                    continue
                to_resource = resources[to_type + 's'].get(to_id, None)
                if to_resource is None:
                    continue
                if to_type =='service':
                    for pointed_uid, pointed in partial_edge_info['pointed'].items():
                        if resources[pointed['type'] + 's'].get(pointed_uid) is None:
                            continue
            else:
                # Take it and add its resources if not present
                from_resource = found_resources[from_type + 's'][from_id]
                to_resource = found_resources[to_type + 's'][to_id]
                resources[from_type + 's'][from_id] = from_resource
                resources[to_type + 's'][to_id] = to_resource
                if to_type =='service':
                    for pointed_uid, pointed in partial_edge_info['pointed'].items():
                        resources[pointed['type'] + 's'][pointed_uid] = found_resources[pointed['type'] + 's'][pointed_uid]

            port_info = {'port': to_port, 'protocol': 'TCP'}
            to_resource = alaz_add_used_port_to_resource(
                to_type, to_resource, cached_pods, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets, port_info) 
            if to_resource == 'service':
                target_port, target_port_protocol = alaz_find_target_port_of_service(to_port, to_resource)
                pointed_port_info = {'port': target_port, 'protocol': target_port_protocol} 
                for pointed_uid, pointed in partial_edge_info['pointed'].items():
                    pointed_resource = resources[pointed['type'] + 's'].get(pointed_uid, None)
                    if pointed_resource:
                        verbose_log(f'Will add port {pointed_port_info} to {pointed["type"]}-{pointed_resource.uid}')
                        pointed_resource = alaz_add_used_port_to_resource(pointed['type'], pointed_resource, cached_pods, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, pointed_port_info)

            resource_count = len(resources['pods']) + len(resources['services']) + len(resources['deployments']) + len(resources['daemon_sets']) + len(resources['outbounds'])
            if resource_count >= settings.SERVICE_MAP_MAX_RESOURCE_COUNT:
                hit_resource_threshold = True

            edge_total_count = partial_edge_info['total_count']
            edge_total_latency = partial_edge_info['total_latency']
            edge_rps = edge_total_count / time_diff
            edge_avg_latency = edge_total_latency / edge_total_count if edge_total_count > 0 else 0

            endpoints = endpoint_info.get(identify_key)
            if endpoints:
                endpoints_min_rps = None
                endpoints_max_rps = None
                endpoints_min_latency = None
                endpoints_max_latency = None
                for protocol, paths in endpoints.items():
                    for _, data in paths.items():
                        if protocol != type_kafka_event:
                            rps = data['count'] / time_diff
                            latency = data['sum_latency'] / data['count']
                            endpoints_min_latency = latency if endpoints_min_latency is None else min(endpoints_min_latency, latency)
                            endpoints_max_latency = latency if endpoints_max_latency is None else max(endpoints_max_latency, latency)
                            endpoints_min_rps = rps if endpoints_min_rps is None else min(endpoints_min_rps, rps)
                            endpoints_max_rps = rps if endpoints_max_rps is None else max(endpoints_max_rps, rps)
                        else:
                                for _, data_item in data.items():
                                    rps = data_item['count'] / time_diff
                                    latency = data_item['sum_latency'] / data_item['count']
                                    endpoints_min_latency = latency if endpoints_min_latency is None else min(endpoints_min_latency, latency)
                                    endpoints_max_latency = latency if endpoints_max_latency is None else max(endpoints_max_latency, latency)
                                    endpoints_min_rps = rps if endpoints_min_rps is None else min(endpoints_min_rps, rps)
                                    endpoints_max_rps = rps if endpoints_max_rps is None else max(endpoints_max_rps, rps)
                min_rps = endpoints_min_rps if min_rps is None else min(min_rps, endpoints_min_rps)
                max_rps = endpoints_max_rps if max_rps is None else max(max_rps, endpoints_max_rps)
                min_latency = endpoints_min_latency if min_latency is None else min(min_latency, endpoints_min_latency)
                max_latency = endpoints_max_latency if max_latency is None else max(max_latency, endpoints_max_latency)
                partial_edge_info['endpoints_min_rps'] = endpoints_min_rps
                partial_edge_info['endpoints_max_rps'] = endpoints_max_rps
                partial_edge_info['endpoints_min_latency'] = endpoints_min_latency
                partial_edge_info['endpoints_max_latency'] = endpoints_max_latency 
            partial_edge_info['rps'] = edge_rps
            partial_edge_info['avg_latency'] = edge_avg_latency
            del partial_edge_info['total_count']
            del partial_edge_info['total_latency']
            traffic[identify_key] = partial_edge_info

        response['min_rps'] = min_rps
        response['max_rps'] = max_rps
        response['min_latency'] = min_latency
        response['max_latency'] = max_latency

        calculations_time_sec = (datetime.now() - calculations_start).total_seconds()

        connections_start = datetime.now()
        if get_connections:
            # Get connections
            connection_groups = list(Connection.objects.filter(cluster=cluster.uid, timestamp__gte=start_time, timestamp__lte=end_time).values(
                'from_uid_pod',
                'from_uid_service',
                # 'from_uid_deployment',
                # 'from_uid_daemonset',
                # 'from_uid_statefulset',
                'from_port',
                'to_uid_pod',
                'to_uid_service',
                # 'to_uid_deployment',
                # 'to_uid_daemonset',
                # 'to_url_outbound',
                'to_uid_statefulset',
                'to_port')
            )

            processed_connection_groups = {}
            # Preprocess connection groups beforehand to determine and fix the directionless ones
            for group in connection_groups:
                to_port = group['to_port']
                from_port = group['from_port']
                # From type - From UID - To type - To UID - To port
                if group['from_uid_deployment']:
                    from_type = 'deployment'
                    from_uid = group['from_uid_deployment']
                elif group['from_uid_daemonset']:
                    from_type = 'daemon_set'
                    from_uid = group['from_uid_daemonset']
                elif group['from_uid_statefulset']:
                    from_type = 'stateful_set'
                    from_uid = group['from_uid_statefulset']
                elif group['from_uid_pod']:
                    from_type = 'pod'
                    from_uid = group['from_uid_pod']
                elif group['from_uid_service']:
                    from_type = 'service'
                    from_uid = group['from_uid_service']

                if group['to_uid_deployment']:
                    to_type = 'deployment'
                    to_uid = group['to_uid_deployment']
                elif group['to_uid_daemonset']:
                    to_type = 'daemon_set'
                    to_uid = group['to_uid_daemonset']
                elif group['to_uid_statefulset']:
                    to_type = 'stateful_set'
                    to_uid = group['to_uid_statefulset']
                elif group['to_uid_pod']:
                    to_type = 'pod'
                    to_uid = group['to_uid_pod']
                elif group['to_uid_service']:
                    to_type = 'service'
                    to_uid = group['to_uid_service']
                elif group['to_url_outbound']:
                    to_type = 'outbound'
                    to_uid = group['to_url_outbound']

                from_type, from_object = alaz_request_find_source_node(
                    group, cached_pods, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_services, cached_stateful_sets)
                if from_type is None or from_object is None:
                    if settings.VERBOSE_LOG:
                        logger.warn(f'From object is None for connection {group}. Skipping')
                    continue

                to_type, to_object = alaz_request_find_destination_node(
                    group, cached_pods, cached_services, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_stateful_sets, add_port=False)
                if to_type is None or to_object is None:
                    if settings.VERBOSE_LOG:
                        logger.warn(f'To object is None for connection {group}. Skipping')
                    continue

                from_uid = str(from_object.uid)
                to_uid = str(to_object.uid)

                identify_key = (from_type, str(from_uid), to_type, str(to_uid), to_port)
                reverse_identify_key = (to_type, str(to_uid), from_type, str(from_uid), from_port)

                group['has_direction'] = True
                group['reverted'] = False

                if identify_key in edges:
                    if settings.VERBOSE_LOG:
                        verbose_log(f'Connection already exists: {identify_key}. Will delete them from the connection groups')
                    if identify_key in processed_connection_groups:
                        del processed_connection_groups[identify_key]
                    if reverse_identify_key in processed_connection_groups:
                        del processed_connection_groups[reverse_identify_key]
                    continue
                else:
                    verbose_log(f'Connection does not exist in the traffic edges: {identify_key}')

                if reverse_identify_key in edges:
                    if settings.VERBOSE_LOG:
                        verbose_log(f'Connection already exists: {reverse_identify_key}. Will delete them from the connection groups')
                    if identify_key in processed_connection_groups:
                        del processed_connection_groups[identify_key]
                    if reverse_identify_key in processed_connection_groups:
                        del processed_connection_groups[reverse_identify_key]
                    continue
                else:
                    verbose_log(f'Reverse connection does not exist in the traffic edges: {reverse_identify_key}')

                if identify_key in processed_connection_groups:
                    if settings.VERBOSE_LOG:
                        logger.warn(f'Connection already exists: {identify_key}')
                    continue
                elif reverse_identify_key in processed_connection_groups:
                    # Is directionless, adjust the direction
                    if settings.VERBOSE_LOG:
                        verbose_log(f'Connection is directionless: {reverse_identify_key}')
                    existing_group = processed_connection_groups[reverse_identify_key]
                    existing_group['has_direction'] = False
                    existing_group = revert_group_if_required(existing_group, heuristics=True)
                    if not existing_group:
                        continue
                    processed_connection_groups[reverse_identify_key] = existing_group
                else:
                    group = revert_group_if_required(group, heuristics=False)
                    if not group:
                        continue
                    processed_connection_groups[identify_key] = group

            connection_edges = {}

            for group in processed_connection_groups.values():
                to_port = group['to_port']
                from_port = group['from_port']

                from_type, from_object = alaz_request_find_source_node(
                    group, cached_pods, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_services, cached_stateful_sets)
                if from_type is None or from_object is None:
                    if settings.VERBOSE_LOG:
                        logger.warn(f'From object is None for connection {group}. Skipping')
                    continue

                from_uid = str(from_object.uid)

                found_resources[from_type + 's'][from_uid] = from_object

                to_type, to_object = alaz_request_find_destination_node(
                    group, cached_pods, cached_services, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_stateful_sets, add_port=False)
                if to_type is None or to_object is None:
                    if settings.VERBOSE_LOG:
                        logger.warn(f'To object is None for connection {group}. Skipping')
                    continue

                to_uid = str(to_object.uid)

                found_resources[to_type + 's'][to_uid] = to_object

                connection_edge = {
                    "from_type": from_type,
                    "from_id": from_uid,
                    # "from_port": from_port,
                    "to_port": to_port,
                    'to_type': to_type,
                    'to_id': to_uid,
                    # 'has_direction': group['has_direction'],
                    # 'reverted': group['reverted'],
                }

                if to_type == 'service':
                    service = to_object
                    found_resources, pointed = alaz_find_pointed_destinations_of_service(
                        service, group['to_port'], cached_pods, cached_endpoints, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets, found_resources)
                    if pointed is None:
                        if settings.VERBOSE_LOG:
                            logger.warn(f'**VERBOSE_LOG: No pointed resources found for service {service.uid}. Skipping')
                        continue
                    connection_edge['pointed'] = pointed
                else:
                    pointed = None

                identify_key = (from_type, str(from_object.uid),
                                to_type, str(to_object.uid), to_port)
                
                if identify_key in connection_edges:
                    continue

                connection_edges[identify_key] = connection_edge

            for identify_key, connection_edge in connection_edges.items():
                verbose_log(f'Will try to process connection: {connection_edge}')
                # Check if from resource is in resources
                from_type = connection_edge['from_type']
                from_id = connection_edge['from_id']
                to_type = connection_edge['to_type']
                to_id = connection_edge['to_id']
                to_port = connection_edge['to_port']

                if hit_resource_threshold:
                    # Take it only if its resources are present
                    from_resource = resources[from_type + 's'].get(from_id, None)
                    if from_resource is None:
                        continue
                    to_resource = resources[to_type + 's'].get(to_id, None)
                    if to_resource is None:
                        continue
                    if to_type =='service':
                        for pointed_uid, pointed in connection_edge['pointed'].items():
                            if resources[pointed['type'] + 's'].get(pointed_uid) is None:
                                continue
                else:
                    # Take it and add its resources if not present
                    from_resource = found_resources[from_type + 's'][from_id]
                    to_resource = found_resources[to_type + 's'][to_id]
                    resources[from_type + 's'][from_id] = from_resource
                    resources[to_type + 's'][to_id] = to_resource
                    if to_type =='service':
                        for pointed_uid, pointed in connection_edge['pointed'].items():
                            resources[pointed['type'] + 's'][pointed_uid] = found_resources[pointed['type'] + 's'][pointed_uid]

                port_info = {'port': to_port, 'protocol': 'TCP'}
                to_resource = alaz_add_used_port_to_resource(
                    to_type, to_resource, cached_pods, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets, port_info) 
                if to_resource == 'service':
                    verbose_log(f'Will add port {port_info} to {from_type}-{from_resource.uid}')
                    target_port, target_port_protocol = alaz_find_target_port_of_service(to_port, to_resource)
                    pointed_port_info = {'port': target_port, 'protocol': target_port_protocol} 
                    for pointed_uid, pointed in connection_edge['pointed'].items():
                        pointed_resource = resources[pointed['type'] + 's'].get(pointed_uid, None)
                        if pointed_resource:
                            verbose_log(f'Will add port {pointed_port_info} to {pointed["type"]}-{pointed_resource.uid}')
                            pointed_resource = alaz_add_used_port_to_resource(pointed['type'], pointed_resource, cached_pods, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, pointed_port_info)
                            # resources[pointed['type'] + 's'][pointed_resource.uid] = pointed_resource

                resource_count = len(resources['pods']) + len(resources['services']) + len(resources['deployments']) + len(resources['daemon_sets']) + len(resources['outbounds'])
                if resource_count >= settings.SERVICE_MAP_MAX_RESOURCE_COUNT:
                    hit_resource_threshold = True

                verbose_log(f'Added connection: {connection_edge}')
                connections[identify_key] = connection_edge

        connections_duration_sec = (datetime.now() - connections_start).total_seconds()

        if get_all_resources:

            if not hit_resource_threshold:
                for _, pod in cached_pods.items():
                    if pod.replicaset_owner or pod.daemonset_owner or pod.statefulset_owner:
                        # Don't draw pods that have deployments or daemonsets
                        continue
                    if str(pod.uid) in resources['pods']:
                        continue
                    resources['pods'][str(pod.uid)] = pod
                    resource_count += 1
                    if resource_count >= settings.SERVICE_MAP_MAX_RESOURCE_COUNT:
                        hit_resource_threshold = True
                        break

            if not hit_resource_threshold:
                for _, service in cached_services.items():
                    if str(service.uid) in resources['services']:
                        continue
                    resources['services'][str(service.uid)] = service
                    resource_count += 1
                    if resource_count >= settings.SERVICE_MAP_MAX_RESOURCE_COUNT:
                        hit_resource_threshold = True
                        break

            if not hit_resource_threshold:
                for _, deployment in cached_deployments.items():
                    if str(deployment.uid) in resources['deployments']:
                        continue
                    resources['deployments'][str(deployment.uid)] = deployment
                    resource_count += 1
                    if resource_count >= settings.SERVICE_MAP_MAX_RESOURCE_COUNT:
                        hit_resource_threshold = True
                        break

            if not hit_resource_threshold:
                for _, daemon_set in cached_daemon_sets.items():
                    if str(daemon_set.uid) in resources['daemon_sets']:
                        continue
                    resources['daemon_sets'][str(daemon_set.uid)] = daemon_set
                    resource_count += 1
                    if resource_count >= settings.SERVICE_MAP_MAX_RESOURCE_COUNT:
                        hit_resource_threshold = True
                        break

            if not hit_resource_threshold:
                for _, stateful_set in cached_stateful_sets.items():
                    if str(stateful_set.uid) in resources['stateful_sets']:
                        continue
                    resources['stateful_sets'][str(stateful_set.uid)] = stateful_set
                    resource_count += 1
                    if resource_count >= settings.SERVICE_MAP_MAX_RESOURCE_COUNT:
                        hit_resource_threshold = True
                        break

            if not hit_resource_threshold:
                for _, outbound in cached_outbounds.items():
                    if str(outbound.uid) in resources['outbounds']:
                        continue
                    resources['outbounds'][outbound.uid] = outbound
                    resource_count += 1
                    if resource_count >= settings.SERVICE_MAP_MAX_RESOURCE_COUNT:
                        hit_resource_threshold = True
                        break

        helpers_start = datetime.now()

        resources = alaz_add_connected_resources_if_any(resources, edges)

        resources = alaz_add_connected_resources_if_any(resources, connections)

        resources = alaz_mark_zombie_resources(resources)

        # resources = alaz_assign_most_frequent_protocol_to_resources(resources, protocol_counts)

        helpers_duration_sec = (datetime.now() - helpers_start).total_seconds()

        response['hit_resource_threshold'] = hit_resource_threshold

        serializer_context = {'cached_pods': cached_pods, 'cached_services': cached_services, 'cached_replica_sets': cached_replica_sets,
                              'cached_deployments': cached_deployments, 'cached_daemon_sets': cached_daemon_sets, 'cached_outbounds': cached_outbounds,
                              'cached_endpoints': cached_endpoints,
                              'cached_stateful_sets': cached_stateful_sets,}

        serialization_start_time = datetime.now()
        for pod in resources['pods'].values():
            response['pods'][str(pod.uid)] = PodSummarySerializer(pod, context=serializer_context).data

        for service in resources['services'].values():
            response['services'][str(service.uid)] = ServiceSummarySerializer(service, context=serializer_context).data

        for deployment in resources['deployments'].values():
            response['deployments'][str(deployment.uid)] = DeploymentSummarySerializer(
                deployment, context=serializer_context).data

        for daemon_set in resources['daemon_sets'].values():
            response['daemon_sets'][str(daemon_set.uid)] = DaemonSetSummarySerializer(
                daemon_set, context=serializer_context).data

        for outbound in resources['outbounds'].values():
            response['outbounds'][outbound.uid] = OutboundSummarySerializer(outbound).data
            
        for stateful_set in resources['stateful_sets'].values():
            response['stateful_sets'][str(stateful_set.uid)] = StatefulSetSummarySerializer(
                stateful_set, context=serializer_context).data

        response['traffic'] = list(traffic.values())
        response['connections'] = list(connections.values())
        response['service_map_max_resource_count'] = settings.SERVICE_MAP_MAX_RESOURCE_COUNT
        response['start_time'] = start_time_ts * 1000 # ms
        response['end_time'] = end_time_ts * 1000 # ms

        debug = {
            'requests_duration_sec': requests_duration_sec,
            'kafka_events_duration_sec': kafka_events_duration_sec,
            'connections_duration_sec': connections_duration_sec,
            'helpers_duration_sec': helpers_duration_sec,
            'serialization_duration_sec': (datetime.now() - serialization_start_time).total_seconds(),
            'total_duration_sec': (datetime.now() - all_start_time).total_seconds(),
            'sum_src_filter_time': sum_src_filter_time,
            'requests_grouping_time_sec': requests_grouping_time_sec,
            'kafka_events_grouping_time_sec': kafka_events_grouping_time_sec,
            'calculations_time_sec': calculations_time_sec,
            'sum_dest_filter_time': sum_dest_filter_time,
            'preparation_time_sec': preparation_time_sec,
            'pointed_sum_time': pointed_sum_time,
            'sum_remaining_time': sum_remaining_time,
            'requests_sum_time': requests_sum_time,
            'total_loop_overhead': total_loop_overhead,
            'total_count': total_count,
            'resource_count': resource_count,
            'hit_resource_threshold': hit_resource_threshold,
            'query_count': len(connection.queries),
            
        }
        if settings.SERVICE_MAP_DURATION_LOG:
            logger.info(f'Service map debug: {debug}')
            # verbose_log(f'Total source node filtering time: {sum_src_filter_time}')
            # verbose_log(f'Total destination node filtering time: {sum_dest_filter_time}')
            # verbose_log(f'Total pointed node filtering time: {pointed_sum_time}')
            # verbose_log(f'Total remaining time for all requests: {sum_remaining_time}')
            # verbose_log(f'Requests sum time: {requests_sum_time}')
            # verbose_log(f'Total loop overhead: {total_loop_overhead}')
            # verbose_log(f'Total time: {(datetime.now() - all_start_time).total_seconds()}')
            # verbose_log(f'Total count: {total_count}')
            # verbose_log(f'Query count: {len(connection.queries)}')

        return Response(response, status=status.HTTP_200_OK)

class RequestsView(GenericAPIView):
    throttle_classes = (throttling.ConcurrencyAlazDefaultThrottleApiKey,)

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    # @log_endpoint
    # @silk_wrapper(name='Requests')
    def post(self, request):
        # logger.info(f'{len(request.data['requests'])} requests received')
        # return Response({"msg": "Alaz requests are processed"}, status=status.HTTP_200_OK)
        times = {}
        endpoint_start_time = datetime.now()
        cluster_uid, idempotency_key, alaz_version, instance = lightweight_validate_alaz_post(request, endpoint_type='requests')
        data = request.data
        
        batch_size = settings.REQUESTS_BATCH_SIZE
        
        # TODO: Remove this
        # if settings.ANTEON_ENV != 'onprem':
        #     if not find_team_owner_if_exists(cluster.user).active:
        #         return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)
            
        # verbose_log(f'Alaz requests payload: {data}')

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Alaz requests are already processed"}, status=status.HTTP_200_OK)

        # cached_replica_sets, cached_deployments, cached_pods, cached_endpoints, cached_services, cached_daemon_sets, cached_outbounds, endpoint_to_service = cache_alaz_resources(cluster, None)
        # TODO: Remove this
        # instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
        # if not instance_count_ok:
        #     return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)

        requests = data["requests"]
        alaz_requests = []
        response = {'msg': 'Alaz requests are processed', 'errors': []}
        request_num = 1

        pods_to_deployments_hset = settings.PODS_TO_DEPLOYMENTS_HSET
        pods_to_daemonsets_hset = settings.PODS_TO_DAEMONSETS_HSET
        pods_to_statefulsets_hset = settings.PODS_TO_STATEFULSETS_HSET
        node_id = instance

        # logger.info(f'Requests payload validation time: {(datetime.now() - endpoint_start_time).total_seconds()}')
        # times['validation'] = (datetime.now() - endpoint_start_time).total_seconds()
        # return Response({"msg": "Alaz requests are processed"}, status=status.HTTP_200_OK)
        # last_data_ts = None
        
        # TODO: Test without any preprocessing. Write to redis directly
        # TODO: Test each part in the for loop in terms of time
        # TODO: Test payload sizes

        # counts = {}

        path_simplification_seconds = 0
        pod_ownership_finding_seconds = 0

        pod_to_owner = {}
        
        loop_start_time = datetime.now()
        data_read_seconds = 0
        data_preparation_seconds = 0
        data_push_seconds = 0
        for request in requests:
            # Fields do not have keys, their order is:
            # 1- start_time (timestamp)
            # 2- latency
            # 3- from_ip
            # 4- from_type
            # 5- from_id
            # 6- from_port
            # 7- to_ip
            # 8- to_type
            # 9- to_id
            # 10- to_port
            # 11- protocol
            # 12- status_code
            # 13- fail_reason
            # 14- method
            # 15- path
            # 16- tls (only on new alaz versions)
            # 17- tcp_seq_num (only on new alaz versions)
            # 18- thread_id (only on new alaz versions)
            try:
                data_read_start_time = datetime.now()
                if len(request) != 16 and len(request) != 15 and len(request) != 18:
                    raise exceptions.InvalidRequestError({'msg': "Request fields are not valid"})

                start_time = request[0]
                latency = request[1]
                from_ip = request[2]
                from_type = request[3]
                from_id = str(request[4])
                from_port = request[5]
                to_ip = request[6]
                to_type = request[7]
                to_id = str(request[8])
                to_port = request[9]
                protocol = request[10]
                status_code = request[11]
                fail_reason = request[12]
                method = request[13]
                path = request[14].replace('\x00', '')
                tls = request[15] if len(request) > 15 else False
                # tcp_seq_num = request[16] if len(request) > 16 else None
                # thread_id = request[17] if len(request) > 17 else None
                
                data_read_seconds += (datetime.now() - data_read_start_time).total_seconds()

                # if not string_is_ascii(path):
                #     raise exceptions.InvalidRequestError({'msg': "Path is not valid"})

                # if path in ['/ABCDEFGH', '/ABCDEFGH/', '/BCDEFGH', '/BCDEFGH/', '/CDEFGH', '/CDEFGH/', '/DEFGH', '/DEFGH/', '/EFGH', '/EFGH/', '/FGH', '/FGH/', '/GH', '/GH/', '/H', '/H/']:
                    # logger.info(f'Received path: {path}')
                    
                data_preparation_start_time = datetime.now()

                simplification_start_time = datetime.now()
                non_simplified_path = path
                if protocol == 'POSTGRES':
                    if method == 'CLOSE_OR_TERMINATE':
                        continue
                    path = simplify_postgres_query(path)
                    non_simplified_path = path
                    path = remove_values_from_postgres_query(path)
                elif protocol in ['HTTP', 'HTTPS']:
                    path = simplify_http_endpoint(path)
                elif protocol == 'REDIS':
                    if path == '':
                        continue
                    path = parse_resp(path)
                    non_simplified_path = path
                    path = remove_values_from_redis_query(path)

                path_simplification_seconds += (datetime.now() - simplification_start_time).total_seconds()
                    
                # if protocol not in counts:
                #     counts[protocol] = 1
                # else:
                #     counts[protocol] += 1

                kwargs = {
                    "start_time": datetime.fromtimestamp(start_time / 1000, UTC).isoformat(),
                    "latency": latency,
                    "from_ip": from_ip,
                    "to_ip": to_ip,
                    "from_port": from_port,
                    "to_port": to_port,
                    "protocol": protocol,
                    "status_code": status_code,
                    "fail_reason": fail_reason,
                    "method": method,
                    "path": path,
                    'non_simplified_path': non_simplified_path,
                    "cluster": cluster_uid,
                    'tls': tls,
                    # 'tcp_seq_num': tcp_seq_num,
                    # 'thread_id': thread_id,
                    # 'node_id': node_id,
                    # 'span_exists': False,
                }

                from_uid_pod = None
                from_uid_service = None
                from_uid_deployment = None
                from_uid_daemonset = None
                from_uid_statefulset = None

                if from_type == "pod":
                    from_uid_pod = from_id
                elif from_type == "service":
                    from_uid_service = from_id
                else:
                    raise exceptions.InvalidRequestError({'msg': f"Invalid from type: {from_type}"})

                ownership_start_time = datetime.now()
                if from_type == 'pod':
                    if from_uid_pod in pod_to_owner:
                        type, uid = pod_to_owner[from_uid_pod]
                        if type == 'deployment':
                            from_uid_deployment = uid
                        elif type == 'daemon_set':
                            from_uid_daemonset = uid
                        elif type == 'stateful_set':
                            from_uid_statefulset = uid
                    else:
                        deployment_uid = redis_client.hget(pods_to_deployments_hset, from_id)
                        if deployment_uid:
                            from_uid_deployment = deployment_uid
                            pod_to_owner[from_id] = ('deployment', deployment_uid)
                        else:
                            daemonset_uid = redis_client.hget(pods_to_daemonsets_hset, from_id)
                            if daemonset_uid:
                                from_uid_daemonset = daemonset_uid
                                pod_to_owner[from_id] = ('daemon_set', daemonset_uid)
                            else:
                                statefulset_uid = redis_client.hget(pods_to_statefulsets_hset, from_id)
                                if statefulset_uid:
                                    from_uid_statefulset = statefulset_uid
                                    pod_to_owner[from_id] = ('stateful_set', statefulset_uid)
                                else:
                                    pod_to_owner[from_id] = ('none', None)
                
                pod_ownership_finding_seconds += (datetime.now() - ownership_start_time).total_seconds()

                # source_type, source_obj = alaz_find_source_node(from_uid_pod, from_uid_service, cached_pods, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_services)

                # if source_type == 'deployment':
                #     from_uid_deployment = str(source_obj.uid)
                # elif source_type == 'daemon_set':
                #     from_uid_daemonset = str(source_obj.uid)

                to_uid_pod = None
                to_uid_service = None
                to_url_outbound = None
                to_uid_deployment = None
                to_uid_daemonset = None
                to_uid_statefulset = None

                if to_type == "pod":
                    to_uid_pod = to_id
                elif to_type == "service":
                    to_uid_service = to_id
                elif to_type == 'outbound':
                    to_url_outbound = to_id
                else:
                    raise exceptions.InvalidRequestError({'msg': f"Invalid to type: {to_type}"})

                ownership_start_time = datetime.now()
                if to_type == 'pod':
                    if to_id in pod_to_owner:
                        type, uid = pod_to_owner[to_id]
                        if type == 'deployment':
                            to_uid_deployment = uid
                        elif type == 'daemon_set':
                            to_uid_daemonset = uid
                        elif type == 'stateful_set':
                            to_uid_statefulset = uid
                    else:
                        deployment_uid = redis_client.hget(pods_to_deployments_hset, to_id)
                        if deployment_uid:
                            to_uid_deployment = deployment_uid
                            pod_to_owner[to_id] = ('deployment', deployment_uid)
                        else:
                            daemonset_uid = redis_client.hget(pods_to_daemonsets_hset, to_id)
                            if daemonset_uid:
                                to_uid_daemonset = daemonset_uid
                                pod_to_owner[to_id] = ('daemon_set', daemonset_uid)
                            else:
                                statefulset_uid = redis_client.hget(pods_to_statefulsets_hset, to_id)
                                if statefulset_uid:
                                    to_uid_statefulset = statefulset_uid
                                    pod_to_owner[to_id] = ('stateful_set', statefulset_uid)
                                else:
                                    pod_to_owner[to_id] = ('none', None)
                    
                pod_ownership_finding_seconds += (datetime.now() - ownership_start_time).total_seconds()

                # dest_type, dest_obj = alaz_find_destination_node(to_uid_pod, to_uid_service, to_url_outbound, cached_pods, cached_services, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds)

                # if dest_type == 'deployment':
                #     to_uid_deployment = str(dest_obj.uid)
                # elif dest_type == 'daemon_set':
                #     to_uid_daemonset = str(dest_obj.uid)
                
                # if from_uid_deployment or from_uid_daemonset or from_uid_statefulset:
                #     from_uid_pod = None
                # if to_uid_deployment or to_uid_daemonset or to_uid_statefulset:
                #     to_uid_pod = None

                kwargs['from_uid_pod'] = from_uid_pod
                kwargs['from_uid_service'] = from_uid_service
                kwargs['from_uid_deployment'] = from_uid_deployment
                kwargs['from_uid_daemonset'] = from_uid_daemonset
                kwargs['from_uid_statefulset'] = from_uid_statefulset
                kwargs['to_uid_pod'] = to_uid_pod
                kwargs['to_uid_service'] = to_uid_service
                kwargs['to_uid_deployment'] = to_uid_deployment
                kwargs['to_uid_daemonset'] = to_uid_daemonset
                kwargs['to_url_outbound'] = to_url_outbound
                kwargs['to_uid_statefulset'] = to_uid_statefulset
                alaz_requests.append(json.dumps(kwargs))
                
                data_preparation_seconds += (datetime.now() - data_preparation_start_time).total_seconds()

                data_push_start_time = datetime.now()
                if len(alaz_requests) >= batch_size:
                    try:
                        redis_client.rpush(settings.REQUESTS_QUEUE, *alaz_requests)
                    except Exception as exc:
                        logger.fatal(f"Redis rpush failed: {exc}")
                        raise exceptions.InvalidRequestError({"msg": "Redis rpush failed"})

                    alaz_requests = []
                
                data_push_seconds += (datetime.now() - data_push_start_time).total_seconds()

            except Exception as exc:
                errors = str(exc.args)
                logger.error(f"Alaz request is not valid: {request}, {exc}")
                response['errors'].append({'request_num': request_num, 'request': request, 'errors': errors})

            request_num += 1

        times['loop'] = (datetime.now() - loop_start_time).total_seconds()
        times['path_simplification'] = path_simplification_seconds
        times['pod_ownership_finding'] = pod_ownership_finding_seconds
        times['data_read'] = data_read_seconds
        times['data_preparation'] = data_preparation_seconds
        times['data_push'] = data_push_seconds

        remaining_start_time = datetime.now()
        if len(alaz_requests) > 0:
            try:
                redis_client.rpush(settings.REQUESTS_QUEUE, *alaz_requests)
            except Exception as exc:
                logger.fatal(f"Redis rpush failed: {exc}")
                raise exceptions.InvalidRequestError({"msg": "Redis rpush failed"})

        redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
        redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

        bytes = len(json.dumps(data))
        redis_client.hincrby(settings.REQUESTS_SIZE_KEY, cluster_uid, bytes)

        if settings.VERBOSE_LOG and 'errors' in response and len(response['errors']) > 0:
            logger.warn(response)

        # logger.info(f'Alaz requests payload: {counts}')
        # logger.info(f'The remaining part of request payload processing: {(datetime.now() - remaining_start_time).total_seconds()}')
        # logger.info(f'Alaz requests processing time: {(datetime.now() - remaining_start_time).total_seconds()}')
        # Log all of the times
        times['remaining'] = (datetime.now() - remaining_start_time).total_seconds()
        times['total'] = (datetime.now() - endpoint_start_time).total_seconds()
        # logger.info(f'Alaz requests processing time: {(datetime.now() - endpoint_start_time).total_seconds()}\n')
        # logger.info(f'Alaz requests processing times for {len(requests)} requests: {times}')

        return Response(response, status=status.HTTP_200_OK)


class KafkaEventsView(GenericAPIView):
    throttle_classes = (throttling.ConcurrencyAlazDefaultThrottleApiKey,)

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def post(self, request):
        cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='kafka_events')
        data = request.data
        if settings.ANTEON_ENV != 'onprem':
            if not find_team_owner_if_exists(cluster.user).active:
                return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)
            
        # verbose_log(f'Kafka events payload: {data}')

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Kafka events are already processed"}, status=status.HTTP_200_OK)

        instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)

        kafka_events = data["kafka_events"]
        kafka_events_to_write = []
        response = {'msg': 'Kafka events are processed', 'errors': []}
        request_num = 1

        pods_to_deployments_hset = settings.PODS_TO_DEPLOYMENTS_HSET
        pods_to_daemonsets_hset = settings.PODS_TO_DAEMONSETS_HSET
        pods_to_statefulsets_hset = settings.PODS_TO_STATEFULSETS_HSET
        node_id = instance
        # last_data_ts = None

        for kafka_event in kafka_events:
            # Fields do not have keys, their order is:
            # // 0) StartTime
            # // 1) Latency
            # // 2) Source IP
            # // 3) Source Type
            # // 4) Source ID
            # // 5) Source Port
            # // 6) Destination IP
            # // 7) Destination Type
            # // 8) Destination ID
            # // 9) Destination Port
            # // 10) Topic
            # // 11) Partition
            # // 12) Key
            # // 13) Value
            # // 14) Type
            # // 15) Tls (bool)
            # // 16) Seq
            # // 17) Tid
            try:
                if len(kafka_event) != 16:
                    raise exceptions.InvalidRequestError({'msg': "Kafka event fields are not valid"})

                start_time = kafka_event[0]
                latency = kafka_event[1]
                from_ip = kafka_event[2]
                from_type = kafka_event[3]
                from_id = str(kafka_event[4])
                from_port = kafka_event[5]
                to_ip = kafka_event[6]
                to_type = kafka_event[7]
                to_id = str(kafka_event[8])
                to_port = kafka_event[9]
                topic = kafka_event[10]
                partition = kafka_event[11]
                key = kafka_event[12]
                value = kafka_event[13]
                event_type = kafka_event[14]
                tls = kafka_event[15]
                # tcp_seq_num = kafka_event[16]
                # thread_id = kafka_event[17]

                kwargs = {
                    "start_time": start_time,
                    "latency": latency,
                    "from_ip": from_ip,
                    "to_ip": to_ip,
                    "from_port": from_port,
                    "to_port": to_port,
                    "topic": topic,
                    "partition": partition,
                    "key": key,
                    "value": value,
                    "type": event_type,
                    "tls": tls,
                    # "tcp_seq_num": tcp_seq_num,
                    # "thread_id": thread_id,
                    # "node_id": node_id,
                    "cluster": str(cluster.uid),
                }
                
                from_uid_pod = None
                from_uid_service = None
                from_uid_deployment = None
                from_uid_daemonset = None
                from_uid_statefulset = None

                if from_type == "pod":
                    from_uid_pod = from_id
                elif from_type == "service":
                    from_uid_service = from_id
                else:
                    raise exceptions.InvalidRequestError({'msg': f"Invalid from type: {from_type}"})

                if from_type == 'pod':
                    deployment_uid = redis_client.hget(pods_to_deployments_hset, from_id)
                    if deployment_uid:
                        from_uid_deployment = deployment_uid
                    else:
                        daemonset_uid = redis_client.hget(pods_to_daemonsets_hset, from_id)
                        if daemonset_uid:
                            from_uid_daemonset = daemonset_uid
                        else:
                            statefulset_uid = redis_client.hget(pods_to_statefulsets_hset, from_id)
                            if statefulset_uid:
                                from_uid_statefulset = statefulset_uid
                    
                to_uid_pod = None
                to_uid_service = None
                to_url_outbound = None
                to_uid_deployment = None
                to_uid_daemonset = None
                to_uid_statefulset = None

                if to_type == "pod":
                    to_uid_pod = to_id
                elif to_type == "service":
                    to_uid_service = to_id
                elif to_type == 'outbound':
                    to_url_outbound = to_id
                else:
                    raise exceptions.InvalidRequestError({'msg': f"Invalid to type: {to_type}"})

                if to_type == 'pod':
                    deployment_uid = redis_client.hget(pods_to_deployments_hset, to_id)
                    if deployment_uid:
                        to_uid_deployment = deployment_uid
                    else:
                        daemonset_uid = redis_client.hget(pods_to_daemonsets_hset, to_id)
                        if daemonset_uid:
                            to_uid_daemonset = daemonset_uid
                        else:
                            statefulset_uid = redis_client.hget(pods_to_statefulsets_hset, to_id)
                            if statefulset_uid:
                                to_uid_statefulset = statefulset_uid
                                
                if from_uid_deployment or from_uid_daemonset or from_uid_statefulset:
                    from_uid_pod = None
                if to_uid_deployment or to_uid_daemonset or to_uid_statefulset:
                    to_uid_pod = None                                

                kwargs['from_uid_pod'] = from_uid_pod
                kwargs['from_uid_service'] = from_uid_service
                kwargs['from_uid_deployment'] = from_uid_deployment
                kwargs['from_uid_daemonset'] = from_uid_daemonset
                kwargs['from_uid_statefulset'] = from_uid_statefulset
                kwargs['to_uid_pod'] = to_uid_pod
                kwargs['to_uid_service'] = to_uid_service
                kwargs['to_uid_deployment'] = to_uid_deployment
                kwargs['to_uid_daemonset'] = to_uid_daemonset
                kwargs['to_url_outbound'] = to_url_outbound
                kwargs['to_uid_statefulset'] = to_uid_statefulset
                kafka_events_to_write.append(json.dumps(kwargs))

            except Exception as exc:
                errors = str(exc.args)
                logger.error(f"Alaz request is not valid: {kafka_event}, {exc}")
                response['errors'].append({'request_num': request_num, 'request': kafka_event, 'errors': errors})

            request_num += 1

        start_time = datetime.now()
        try:
            if len(kafka_events_to_write) > 0:
                redis_client.rpush(settings.KAFKA_EVENTS_QUEUE, *kafka_events_to_write)
        except Exception as exc:
            logger.fatal(f"Redis rpush failed: {exc}")
            raise exceptions.InvalidRequestError({"msg": "Redis rpush failed"})

        redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
        redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

        bytes = len(json.dumps(data))
        redis_client.hincrby(settings.REQUESTS_SIZE_KEY, str(cluster.uid), bytes)

        if settings.VERBOSE_LOG and 'errors' in response and len(response['errors']) > 0:
            logger.warn(response)

        return Response(response, status=status.HTTP_200_OK)


class ConnectionsView(GenericAPIView):
    # TODO: Write a separate throttling for this
    throttle_classes = (throttling.ConcurrencyAlazDefaultThrottleApiKey,)

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    # @log_endpoint
    # @silk_wrapper(name='Connections-Post')
    def post(self, request):
        # verbose_log(f'Request payload: {request.data}')
        cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='connections')
        data = request.data

        if settings.ANTEON_ENV != 'onprem':
            if not find_team_owner_if_exists(cluster.user).active:
                return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Connections are already processed"}, status=status.HTTP_200_OK)

        # cached_replica_sets, cached_deployments, cached_pods, cached_endpoints, cached_services, cached_daemon_sets, cached_outbounds, endpoint_to_service = cache_alaz_resources(cluster, None)
        instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)

        connections = data["connections"]
        connections_to_add = []
        response = {'msg': 'Connections are processed', 'errors': []}
        connection_num = 1
        pods_to_deployments_hset = settings.PODS_TO_DEPLOYMENTS_HSET
        pods_to_daemonsets_hset = settings.PODS_TO_DAEMONSETS_HSET
        pods_to_statefulsets_hset = settings.PODS_TO_STATEFULSETS_HSET
        # last_data_ts = None
        for connection in connections:
            # Fields do not have keys, their order is:
            # 1- start_time (timestamp)
            # 2- from_ip
            # 3- from_type
            # 4- from_id
            # 5- from_port
            # 6- to_ip
            # 7- to_type
            # 8- to_id
            # 9- to_port
            try:
                if len(connection) != 9:
                    raise exceptions.InvalidRequestError({'msg': "Connection fields are not valid"})

                timestamp = connection[0]
                from_ip = connection[1]
                from_type = connection[2]
                from_id = connection[3]
                from_port = connection[4]
                to_ip = connection[5]
                to_type = connection[6]
                to_id = connection[7]
                to_port = connection[8]

                kwargs = {
                    "timestamp": timestamp,
                    "from_ip": from_ip,
                    "to_ip": to_ip,
                    "from_port": from_port,
                    "to_port": to_port,
                    "cluster": str(cluster.uid),
                }

                from_uid_pod = None
                from_uid_service = None
                from_uid_deployment = None
                from_uid_daemonset = None
                from_uid_statefulset = None

                if from_type == "pod":
                    from_uid_pod = from_id
                elif from_type == "service":
                    from_uid_service = from_id
                else:
                    raise exceptions.InvalidRequestError({'msg': f"Invalid from type: {from_type}"})

                if from_type == 'pod':
                    deployment_uid = redis_client.hget(pods_to_deployments_hset, from_id)
                    if deployment_uid:
                        from_uid_deployment = deployment_uid
                    else:
                        daemonset_uid = redis_client.hget(pods_to_daemonsets_hset, from_id)
                        if daemonset_uid:
                            from_uid_daemonset = daemonset_uid
                        else:
                            statefulset_uid = redis_client.hget(pods_to_statefulsets_hset, from_id)
                            if statefulset_uid:
                                from_uid_statefulset = statefulset_uid

                # source_type, source_obj = alaz_find_source_node(from_uid_pod, from_uid_service, cached_pods, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_services)

                # if source_type == 'deployment':
                #     from_uid_deployment = str(source_obj.uid)
                # elif source_type == 'daemon_set':
                #     from_uid_daemonset = str(source_obj.uid)

                to_uid_pod = None
                to_uid_service = None
                to_url_outbound = None
                to_uid_deployment = None
                to_uid_daemonset = None
                to_uid_statefulset = None

                if to_type == "pod":
                    to_uid_pod = to_id
                elif to_type == "service":
                    to_uid_service = to_id
                elif to_type == 'outbound':
                    to_url_outbound = to_id
                else:
                    raise exceptions.InvalidRequestError({'msg': f"Invalid to type: {to_type}"})

                if to_type == 'pod':
                    deployment_uid = redis_client.hget(pods_to_deployments_hset, to_id)
                    if deployment_uid:
                        to_uid_deployment = deployment_uid
                    else:
                        daemonset_uid = redis_client.hget(pods_to_daemonsets_hset, to_id)
                        if daemonset_uid:
                            to_uid_daemonset = daemonset_uid
                        else:
                            statefulset_uid = redis_client.hget(pods_to_statefulsets_hset, to_id)
                            if statefulset_uid:
                                to_uid_statefulset = statefulset_uid
                            

                # dest_type, dest_obj = alaz_find_destination_node(to_uid_pod, to_uid_service, to_url_outbound, cached_pods, cached_services, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds)

                # if dest_type == 'deployment':
                #     to_uid_deployment = str(dest_obj.uid)
                # elif dest_type == 'daemon_set':
                #     to_uid_daemonset = str(dest_obj.uid)

                if from_uid_deployment or from_uid_daemonset or from_uid_statefulset:
                    from_uid_pod = None
                if to_uid_deployment or to_uid_daemonset or to_uid_statefulset:
                    to_uid_pod = None

                kwargs['from_uid_pod'] = from_uid_pod
                kwargs['from_uid_service'] = from_uid_service
                kwargs['from_uid_deployment'] = from_uid_deployment
                kwargs['from_uid_daemonset'] = from_uid_daemonset
                kwargs['from_uid_statefulset'] = from_uid_statefulset
                kwargs['to_uid_pod'] = to_uid_pod
                kwargs['to_uid_service'] = to_uid_service
                kwargs['to_uid_deployment'] = to_uid_deployment
                kwargs['to_uid_daemonset'] = to_uid_daemonset
                kwargs['to_url_outbound'] = to_url_outbound
                kwargs['to_uid_statefulset'] = to_uid_statefulset

                connections_to_add.append(json.dumps(kwargs))

            except Exception as exc:
                errors = str(exc.args)
                logger.error(f"Connection is not valid: {connection}, {exc}")
                response['errors'].append({'connection_num': connection_num, 'connection': connection, 'errors': errors})

            connection_num += 1

        timestamp = datetime.now()
        try:
            if len(connections) > 0:
                redis_client.rpush(settings.CONNECTIONS_QUEUE, *connections_to_add)
        except Exception as exc:
            logger.fatal(f"Redis rpush failed: {exc}")
            raise exceptions.InvalidRequestError({"msg": "Redis rpush failed"})

        redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
        redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

        bytes = len(json.dumps(data))
        redis_client.hincrby(settings.CONNECTIONS_SIZE_KEY, str(cluster.uid), bytes)
        
        if settings.VERBOSE_LOG and 'errors' in response and len(response['errors']) > 0:
            logger.warn(response)

        return Response(response, status=status.HTTP_200_OK)


class ResourceTrafficView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})
    
    # @silk_wrapper(name='Resource-Details')
    def get(self, request):
        cluster, start_time, end_time = validate_service_map_get_method(request)
        resource_type = request.query_params.get('resource_type', None)
        if not resource_type:
            raise exceptions.ValidationError({"msg": "Resource type is required"})
        resource_uid = request.query_params.get('resource_uid', None)
        if not resource_uid:
            raise exceptions.ValidationError({"msg": "Resource uid is required"})

        namespaces = request.query_params.get('namespaces', None)
        namespaces = namespaces.split(',') if namespaces else None
        # Convert start_time str to timestamp in ms
        end_time = datetime.fromtimestamp(int(end_time) / 1000)
        start_time = datetime.fromtimestamp(int(start_time) / 1000)

        # Resource type can be:
        # 1- Pod
        # 2- Service
        # 3- Deployment
        # 4- DaemonSet
        # 5- Outbound
        # 6- Statefulset

        cache_start_time = datetime.now()
        dest_pods = []
        dest_services = []
        dest_outbounds = []
        src_pods = []
        src_services = []
        
        cached_replica_sets, cached_deployments, cached_pods, cached_endpoints, cached_services, cached_daemon_sets, cached_outbounds, endpoint_to_service, cached_stateful_sets = cache_alaz_resources(
            cluster, namespaces)
        if settings.VERBOSE_LOG:
            print(f'SINGLE RESOURCE RETRIEVAL - Time to cache resources: {(datetime.now() - cache_start_time).total_seconds()}')
        
        preparing_resources_start_time = datetime.now()

        if resource_type == 'pod':
            resource_uid = uuid.UUID(resource_uid)
            destination_resource = cached_pods.get(resource_uid, None)
            if not destination_resource:
                raise exceptions.ValidationError({"msg": "Pod is not found"})
            dest_pods.append(destination_resource.uid)
            src_pods.append(destination_resource.uid)
            pointing_services = alaz_find_services_pointing_to_pod(destination_resource, cached_services, cached_endpoints, endpoint_to_service)
            for pointing_service in pointing_services:
                dest_services.append(pointing_service.uid)
        elif resource_type == 'service':
            resource_uid = uuid.UUID(resource_uid)
            destination_resource = cached_services.get(resource_uid, None)
            if not destination_resource:
                raise exceptions.ValidationError({"msg": "Service is not found"})
            dest_services.append(destination_resource.uid)
            src_services.append(destination_resource.uid)
        elif resource_type == 'deployment':
            resource_uid = uuid.UUID(resource_uid)
            destination_resource = cached_deployments.get(resource_uid, None)
            if not destination_resource:
                raise exceptions.ValidationError({"msg": "Deployment is not found"})
            replicasets = get_replicasets_of_deployment(destination_resource, {'cached_deployments': cached_deployments})
            for replicaset in replicasets:
                pods = get_pods_of_replicaset(replicaset, {'cached_replicasets': cached_replica_sets})
                for pod in pods:
                    dest_pods.append(pod.uid)
                    src_pods.append(pod.uid)
                    pointing_services = alaz_find_services_pointing_to_pod(pod, cached_services, cached_endpoints, endpoint_to_service)
                    for pointing_service in pointing_services:
                        dest_services.append(pointing_service.uid)
        elif resource_type == 'daemon_set':
            resource_uid = uuid.UUID(resource_uid)
            destination_resource = cached_daemon_sets.get(resource_uid, None)
            if not destination_resource:
                raise exceptions.ValidationError({"msg": "DaemonSet is not found"})
            pods = get_pods_of_daemonset(destination_resource, {'cached_daemon_sets': cached_daemon_sets})
            for pod in pods:
                dest_pods.append(pod.uid)
                src_pods.append(pod.uid)
                pointing_services = alaz_find_services_pointing_to_pod(pod, cached_services, cached_endpoints, endpoint_to_service)
                for pointing_service in pointing_services:
                    dest_services.append(pointing_service.uid)
        elif resource_type == 'stateful_set':
            resource_uid = uuid.UUID(resource_uid)
            destination_resource = cached_stateful_sets.get(resource_uid, None)
            if not destination_resource:
                raise exceptions.ValidationError({"msg": "StatefulSet is not found"})
            pods = get_pods_of_statefulset(destination_resource, {'cached_stateful_sets': cached_stateful_sets})
            for pod in pods:
                dest_pods.append(pod.uid)
                src_pods.append(pod.uid)
                pointing_services = alaz_find_services_pointing_to_pod(pod, cached_services, cached_endpoints, endpoint_to_service)
                for pointing_service in pointing_services:
                    dest_services.append(pointing_service.uid)
        elif resource_type == 'outbound':
            outbound = Outbound(resource_uid)
            destination_resource = outbound
            cached_outbounds[resource_uid] = outbound
            dest_outbounds.append(outbound.uid)
        else:
            raise exceptions.ValidationError({"msg": "Invalid resource type"})

        if settings.VERBOSE_LOG:
            print(f'SINGLE RESOURCE RETRIEVAL - Time to prepare resources: {(datetime.now() - preparing_resources_start_time).total_seconds()}')
        
        traffic = {}

        time_diff = (end_time - start_time).total_seconds() if end_time > start_time else 1

        incoming_groups = list(Request.objects.filter(
                    Q(to_uid_pod__in=dest_pods) | Q(to_uid_service__in=dest_services) | Q(to_url_outbound__in=dest_outbounds),
                    cluster=cluster.uid, start_time__gte=start_time, start_time__lte=end_time).values(
                        'from_uid_pod',
                        'from_uid_service',
                        # 'from_uid_deployment',
                        # 'from_uid_daemonset',
                        # 'from_uid_statefulset',
                        'to_uid_pod',
                        'to_uid_service',
                        # 'to_uid_deployment',
                        # 'to_uid_daemonset',
                        # 'to_uid_statefulset',
                        'to_url_outbound',
                        'to_port',
                        'protocol',
                        'status_code',
                        # 'path',
                        # 'method',
                        # 'status_code',
                    ).annotate(
                        count=Count('id'),
                        sum_latency=Sum('latency'),
                    ))

        outgoing_groups = list(Request.objects.filter(
                    Q(from_uid_pod__in=src_pods) | Q(from_uid_service__in=src_services),
                    cluster=cluster.uid, start_time__gte=start_time, start_time__lte=end_time).values(
                        'from_uid_pod',
                        'from_uid_service',
                        # 'from_uid_deployment',
                        # 'from_uid_daemonset',
                        # 'from_uid_statefulset',
                        'to_uid_pod',
                        'to_uid_service',
                        # 'to_uid_deployment',
                        # 'to_uid_daemonset',
                        # 'to_uid_statefulset',
                        'to_url_outbound',
                        'to_port',
                        'protocol',
                        'status_code'
                        # 'path',
                        # 'method',
                        # 'status_code',
                    ).annotate(
                        count=Count('id'),
                        sum_latency=Sum('latency'),
                    ))

        # Add incoming and outgoing flags
        for group in outgoing_groups:
            group['incoming'] = False
        for group in incoming_groups:
            group['incoming'] = True

        incoming_kafka_groups = list(KafkaEvent.objects.filter(
                    Q(to_uid_pod__in=dest_pods) | Q(to_uid_service__in=dest_services) | Q(to_url_outbound__in=dest_outbounds),
                    cluster=cluster.uid, start_time__gte=start_time, start_time__lte=end_time).values(
                        'from_uid_pod',
                        'from_uid_service',
                        # 'from_uid_deployment',
                        # 'from_uid_daemonset',
                        # 'from_uid_statefulset',                        
                        'to_uid_pod',
                        'to_uid_service',
                        # 'to_uid_deployment',
                        # 'to_uid_daemonset',
                        # 'to_uid_statefulset',                        
                        'to_url_outbound',
                        'to_port',
                        # 'topic',
                        'type',
                        # 'partition',
                    ).annotate(
                        count=Count('id'),
                        sum_latency=Sum('latency'),
                    ))

        outgoing_kafka_groups = list(KafkaEvent.objects.filter(
                    Q(from_uid_pod__in=src_pods) | Q(from_uid_service__in=src_services),
                    cluster=cluster.uid, start_time__gte=start_time, start_time__lte=end_time).values(
                        'from_uid_pod',
                        'from_uid_service',
                        # 'from_uid_deployment',
                        # 'from_uid_daemonset',
                        # 'from_uid_statefulset',                        
                        'to_uid_pod',
                        'to_uid_service',
                        # 'to_uid_deployment',
                        # 'to_uid_daemonset',
                        # 'to_uid_statefulset',                        
                        'to_url_outbound',
                        'to_port',
                        # 'topic',
                        'type',
                        # 'partition',
                    ).annotate(
                        count=Count('id'),
                        sum_latency=Sum('latency'),
                    ))
        
        for group in outgoing_kafka_groups:
            group['incoming'] = False
            group['protocol'] = type_kafka_event
        for group in incoming_kafka_groups:
            group['incoming'] = True
            group['protocol'] = type_kafka_event

        groups = incoming_groups + outgoing_groups + incoming_kafka_groups + outgoing_kafka_groups

        for group in groups:
            to_port = group['to_port']
            dest_service_uid = group['to_uid_service']
            protocol = group['protocol']
            if protocol == type_kafka_event:
                event_type = group['type']
            else:
                status_code = group['status_code']

            from_type, from_object = alaz_request_find_source_node(
                group, cached_pods, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_services, cached_stateful_sets)
            if from_type is None or from_object is None:
                print(f'**VERBOSE_LOG: No source node found for group {group}. Skipping')
                continue
            from_uid = from_object.uid

            to_type, to_object = alaz_request_find_destination_node(
                group, cached_pods, cached_services, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_stateful_sets)
            if to_type is None or to_object is None:
                print(f'**VERBOSE_LOG: No destination node found for group {group}. Skipping')
                continue

            to_uid = to_object.uid
            dest_object = to_object

            also_outgoing = False
            if to_type == 'service':
                _, pointed_destinations = alaz_find_pointed_destinations_of_service(
                    dest_object, group['to_port'], cached_pods, cached_endpoints, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets)
                if pointed_destinations is None:
                    if settings.VERBOSE_LOG:
                        print(f'**VERBOSE_LOG: No pointed resources found for service {dest_object.uid}. Skipping')
                    continue
                if resource_type == 'service':
                    also_outgoing = True

            looking_at_pointed = False
            if dest_service_uid and resource_type != 'service':
                looking_at_pointed = True
                # If we're looking at the pointed information
                dest_object = cached_services.get(dest_service_uid, None)
                target_port, target_port_protocol = alaz_find_target_port_of_service(to_port, dest_object)
            else:
                target_port = to_port
                # Assuming TCP protocol for now
                target_port_protocol = 'TCP'

            if target_port is not None and target_port_protocol is not None:
                port_info = {'port': target_port, 'protocol': target_port_protocol}
                alaz_add_used_port_to_resource(
                    resource_type, destination_resource, cached_pods, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets, port_info)
            else:
                if settings.VERBOSE_LOG:
                    verbose_log(f'No target port found for service {dest_object.uid}. Skipping')
                continue

            identify_key = (from_type, str(from_uid), to_type, str(to_uid), to_port)

            if identify_key in traffic:
                partial_edge_info = traffic[identify_key]
            else:
                partial_edge_info = {
                    'from_type': from_type,
                    'from_uid': str(from_uid),
                    'to_type': to_type,
                    'to_uid': str(to_uid),
                    'to_port': to_port,
                    'protocols': {}
                }

            if protocol == type_kafka_event:
                if protocol not in partial_edge_info['protocols']:
                    partial_edge_info['protocols'][protocol] = {'event_types': {}, 'sum_latency': 0, 'count': 0}
                if event_type not in partial_edge_info['protocols'][protocol]['event_types']:
                    partial_edge_info['protocols'][protocol]['event_types'][event_type] = {'sum_latency': 0, 'count': 0}

                partial_edge_info['protocols'][protocol]['event_types'][event_type]['sum_latency'] += group['sum_latency']
                partial_edge_info['protocols'][protocol]['event_types'][event_type]['count'] += group['count']

                partial_edge_info['protocols'][protocol]['sum_latency'] += group['sum_latency']
                partial_edge_info['protocols'][protocol]['count'] += group['count']
            else:
                if protocol not in partial_edge_info['protocols']:
                    partial_edge_info['protocols'][protocol] = {'status_codes': [], 'sum_latency': 0, 'count': 0}
                if status_code not in partial_edge_info['protocols'][protocol]['status_codes']:
                    partial_edge_info['protocols'][protocol]['status_codes'].append(status_code)

                partial_edge_info['protocols'][protocol]['sum_latency'] += group['sum_latency']
                partial_edge_info['protocols'][protocol]['count'] += group['count']

            if group['incoming'] and looking_at_pointed:
                partial_edge_info['pointed'] = {str(resource_uid): {'type': resource_type, 'uid': str(resource_uid), 'pointed_port': target_port}}

            if group['incoming'] and also_outgoing:
                pointed = {}
                for pointed_uid, pointed_payload in pointed_destinations.items():
                    pointed_type = pointed_payload['type']
                    pointed_port = pointed_payload['pointed_port']
                    pointed[pointed_uid] = {'type': pointed_type, 'uid': pointed_uid, 'pointed_port': pointed_port}
            
                partial_edge_info['pointed'] = pointed

            traffic[identify_key] = partial_edge_info

        for identify_key, edge in traffic.items():
            for protocol, protocol_info in edge['protocols'].items():
                if protocol == type_kafka_event:
                    for event_type, event_info in protocol_info['event_types'].items():
                        protocol_info['event_types'][event_type]['avg_latency'] = event_info['sum_latency'] / event_info['count']
                        protocol_info['event_types'][event_type]['rps'] = event_info['count'] / time_diff
                        del protocol_info['event_types'][event_type]['sum_latency']
                        del protocol_info['event_types'][event_type]['count']
                    
                protocol_info['avg_latency'] = protocol_info['sum_latency'] / protocol_info['count']
                protocol_info['rps'] = protocol_info['count'] / time_diff
                del protocol_info['sum_latency']
                del protocol_info['count']

        connections = {}

        incoming_connection_groups = list(Connection.objects.filter(
                    Q(to_uid_pod__in=dest_pods) | Q(to_uid_service__in=dest_services) | Q(to_url_outbound__in=dest_outbounds),
                    cluster=cluster.uid, timestamp__gte=start_time, timestamp__lte=end_time).values(
                        'from_uid_pod',
                        'from_uid_service',
                        # 'from_uid_deployment',
                        # 'from_uid_daemonset',
                        # 'from_uid_statefulset',
                        'from_port',
                        'to_uid_pod',
                        'to_uid_service',
                        # 'to_uid_deployment',
                        # 'to_uid_daemonset',
                        # 'to_uid_statefulset',
                        'to_url_outbound',
                        'to_port',
                    )
                )

        outgoing_connection_groups = list(Connection.objects.filter(
                    Q(from_uid_pod__in=src_pods) | Q(from_uid_service__in=src_services),
                    cluster=cluster.uid, timestamp__gte=start_time, timestamp__lte=end_time).values(
                        'from_uid_pod',
                        'from_uid_service',
                        # 'from_uid_deployment',
                        # 'from_uid_daemonset',
                        # 'from_uid_statefulset',
                        'from_port',
                        'to_uid_pod',
                        'to_uid_service',
                        # 'to_uid_deployment',
                        # 'to_uid_daemonset',
                        # 'to_uid_statefulset',
                        'to_url_outbound',
                        'to_port',
                    )
                )

        # Add incoming and outgoing flags
        for group in outgoing_connection_groups:
            group['incoming'] = False
        for group in incoming_connection_groups:
            group['incoming'] = True

        connection_groups = incoming_connection_groups + outgoing_connection_groups

        processed_connection_groups = {}
        # Preprocess connection groups beforehand to determine and fix the directionless ones
        for group in connection_groups:
            to_port = group['to_port']
            from_port = group['from_port']
            # From type - From UID - To type - To UID - To port
            if group['from_uid_deployment']:
                from_type = 'deployment'
                from_uid = group['from_uid_deployment']
            elif group['from_uid_daemonset']:
                from_type = 'daemon_set'
                from_uid = group['from_uid_daemonset']
            elif group['from_uid_statefulset']:
                from_type = 'stateful_set'
                from_uid = group['from_uid_statefulset']
            elif group['from_uid_pod']:
                from_type = 'pod'
                from_uid = group['from_uid_pod']
            elif group['from_uid_service']:
                from_type = 'service'
                from_uid = group['from_uid_service']

            if group['to_uid_deployment']:
                to_type = 'deployment'
                to_uid = group['to_uid_deployment']
            elif group['to_uid_daemonset']:
                to_type = 'daemon_set'
                to_uid = group['to_uid_daemonset']
            elif group['to_uid_statefulset']:
                to_type = 'stateful_set'
                to_uid = group['to_uid_statefulset']
            elif group['to_uid_pod']:
                to_type = 'pod'
                to_uid = group['to_uid_pod']
            elif group['to_uid_service']:
                to_type = 'service'
                to_uid = group['to_uid_service']
            elif group['to_url_outbound']:
                to_type = 'outbound'
                to_uid = group['to_url_outbound']

            from_type, from_object = alaz_request_find_source_node(
                group, cached_pods, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_services, cached_stateful_sets)
            if from_type is None or from_object is None:
                if settings.VERBOSE_LOG:
                    logger.warn(f'From object is None for connection {group}. Skipping')
                continue

            to_type, to_object = alaz_request_find_destination_node(
                group, cached_pods, cached_services, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_stateful_sets, add_port=False)
            if to_type is None or to_object is None:
                if settings.VERBOSE_LOG:
                    logger.warn(f'To object is None for connection {group}. Skipping')
                continue

            from_uid = str(from_object.uid)
            to_uid = str(to_object.uid)

            identify_key = (from_type, str(from_uid), to_type, str(to_uid), to_port)
            reverse_identify_key = (to_type, str(to_uid), from_type, str(from_uid), from_port)

            if identify_key in traffic:
                if identify_key in processed_connection_groups:
                    del processed_connection_groups[identify_key]
                if reverse_identify_key in processed_connection_groups:
                    del processed_connection_groups[reverse_identify_key]
                continue

            if reverse_identify_key in traffic:
                if identify_key in processed_connection_groups:
                    del processed_connection_groups[identify_key]
                if reverse_identify_key in processed_connection_groups:
                    del processed_connection_groups[reverse_identify_key]
                continue

            if identify_key in processed_connection_groups:
                if settings.VERBOSE_LOG:
                    logger.warn(f'Connection already exists: {identify_key}')
                continue
            elif reverse_identify_key in processed_connection_groups:
                # Is directionless, adjust the direction
                if settings.VERBOSE_LOG:
                    verbose_log(f'Connection is directionless: {reverse_identify_key}')
                existing_group = processed_connection_groups[reverse_identify_key]
                existing_group = revert_group_if_required(existing_group, heuristics=True)
                if not existing_group:
                    continue
                processed_connection_groups[reverse_identify_key] = existing_group
            else:
                group = revert_group_if_required(group, heuristics=False)
                if not group:
                    continue
                processed_connection_groups[identify_key] = group

        for group in processed_connection_groups.values():
            from_port = group['from_port']
            to_port = group['to_port']
            dest_service_uid = group['to_uid_service']

            from_type, from_object = alaz_request_find_source_node(
                group, cached_pods, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_services, cached_stateful_sets)
            if from_type is None or from_object is None:
                verbose_log(f'No source node found for group {group}. Skipping')
                continue
            from_uid = from_object.uid

            to_type, to_object = alaz_request_find_destination_node(
                group, cached_pods, cached_services, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_stateful_sets)
            if to_type is None or to_object is None:
                print(f'**VERBOSE_LOG: No destination node found for group {group}. Skipping')
                continue
            to_uid = to_object.uid
            dest_object = to_object

            identify_key = (from_type, from_uid, to_type, to_uid, to_port)

            if identify_key in connections:
                if settings.VERBOSE_LOG:
                    logger.warn(f'Connection already exists: {identify_key}')
                continue

            also_outgoing = False
            if to_type == 'service':
                _, pointed_destinations = alaz_find_pointed_destinations_of_service(
                    dest_object, group['to_port'], cached_pods, cached_endpoints, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets)
                if pointed_destinations is None:
                    if settings.VERBOSE_LOG:
                        print(f'**VERBOSE_LOG: No pointed resources found for service {dest_object.uid}. Skipping')
                    continue
                if resource_type == 'service':
                    also_outgoing = True

            looking_at_pointed = False
            if dest_service_uid and resource_type != 'service':
                looking_at_pointed = True
                # If we're looking at the pointed information
                dest_object = cached_services.get(dest_service_uid, None)
                target_port, target_port_protocol = alaz_find_target_port_of_service(to_port, dest_object)
            else:
                target_port = to_port
                # Assuming TCP protocol for now
                target_port_protocol = 'TCP'
            if target_port is not None and target_port_protocol is not None:
                port_info = {'port': target_port, 'protocol': target_port_protocol}
                alaz_add_used_port_to_resource(
                    resource_type, destination_resource, cached_pods, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets, port_info)
            else:
                if settings.VERBOSE_LOG:
                    verbose_log(f'No target port found for service {dest_object.uid}. Skipping')
                continue

            connection_edge = {
                'from_type': from_type,
                'from_uid': from_uid,
                # 'from_port': from_port,
                'to_type': to_type,
                'to_uid': to_uid,
                'to_port': to_port,
            }

            if group['incoming'] and looking_at_pointed:
                connection_edge['pointed'] = {str(resource_uid): {'type': resource_type, 'uid': str(resource_uid), 'pointed_port': target_port}}

            if group['incoming'] and also_outgoing:
                pointed = {}
                for pointed_uid, pointed_payload in pointed_destinations.items():
                    pointed_type = pointed_payload['type']
                    pointed_port = pointed_payload['pointed_port']
                    pointed[pointed_uid] = {'type': pointed_type, 'uid': pointed_uid, 'pointed_port': pointed_port}
            
                connection_edge['pointed'] = pointed

            connections[identify_key] = connection_edge

        serializer_context = {'cached_pods': cached_pods, 'cached_services': cached_services, 'cached_replica_sets': cached_replica_sets,
                                'cached_deployments': cached_deployments, 'cached_daemon_sets': cached_daemon_sets, 'cached_outbounds': cached_outbounds,
                                'cached_endpoints': cached_endpoints,
                                'cached_stateful_sets': cached_stateful_sets}

        serialization_start_time = datetime.now()
        if resource_type == 'pod':
            serialized_data = PodSerializer(destination_resource, context=serializer_context).data
        elif resource_type == 'service':
            serialized_data = ServiceSerializer(destination_resource, context=serializer_context).data
        elif resource_type == 'deployment':
            serialized_data = DeploymentSerializer(destination_resource, context=serializer_context).data
        elif resource_type == 'daemon_set':
            serialized_data = DaemonSetSerializer(destination_resource, context=serializer_context).data
        elif resource_type == 'stateful_set':
            serialized_data = StatefulSetSerializer(destination_resource, context=serializer_context).data
        elif resource_type == 'outbound':
            serialized_data = OutboundSerializer(destination_resource, context=serializer_context).data
        else:
            raise exceptions.ValidationError({"msg": "Invalid resource type"})
        if settings.VERBOSE_LOG:
            verbose_log(f'RESOURCE RETRIEVAL - Time to serialize data: {(datetime.now() - serialization_start_time).total_seconds()}')

        response = {
            'destination': serialized_data,
            'traffic': traffic.values(),
            'connections': connections.values()
        }
        return Response(response, status=status.HTTP_200_OK)

class ResourceEndpointsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})
    
    def get(self, request):
        cluster, start_time, end_time = validate_service_map_get_method(request)

        to_type = request.query_params.get('to_type', None)
        if not to_type:
            raise exceptions.ValidationError({"msg": "To type is required"})
        to_uid = request.query_params.get('to_uid', None)
        if not to_uid:
            raise exceptions.ValidationError({"msg": "To uid is required"})
        if to_type != 'outbound':
            try:
                uuid.UUID(to_uid)
            except Exception:
                raise exceptions.ValidationError({"msg": "Invalid to uid"})
        
        from_type = request.query_params.get('from_type', None)
        if not from_type:
            raise exceptions.ValidationError({"msg": "From type is required"})
        from_uid = request.query_params.get('from_uid', None)
        if not from_uid:
            raise exceptions.ValidationError({"msg": "From uid is required"})
        try:
            uuid.UUID(from_uid)
        except Exception:
            raise exceptions.ValidationError({"msg": "Invalid from uid"})
        
        protocol = request.query_params.get('protocol', None)
        if not protocol:
            raise exceptions.ValidationError({"msg": "Protocol is required"})

        if protocol == type_kafka_event:
            event_type = request.query_params.get('event_type', None)
            if not event_type:
                raise exceptions.ValidationError({"msg": "Event type is required"})
        
        page_num = int(request.query_params.get('page_num', None))
        if not page_num:
            raise exceptions.ValidationError({"msg": "Page number is required"})
        if page_num <= 0:
            raise exceptions.ValidationError({"msg": "Page number must be a positive integer"})
        
        sort_by = request.query_params.get('sort_by', None)
        if not sort_by:
            raise exceptions.ValidationError({"msg": "Sort by is required"})
        if sort_by != 'avg_latency' and sort_by != 'rps':
            raise exceptions.ValidationError({"msg": "Sort by should be either avg_latency or rps"})
        
        sort_with = request.query_params.get('sort_with', None)
        if not sort_with:
            raise exceptions.ValidationError({"msg": "Sort with is required"})
        if sort_with != 'asc' and sort_with != 'desc':
            raise exceptions.ValidationError({"msg": "Sort with should be either asc or desc"})

        to_port = int(request.query_params.get('to_port', None))
        if not to_port:
            raise exceptions.ValidationError({"msg": "To port is required"})
        if to_port <= 0:
            raise exceptions.ValidationError({"msg": "To port must be a positive integer"})

        status_codes = request.query_params.get('status_codes', None)
        # These could be 1xx, 2xx, 3xx, 4xx, 5xx
        status_codes = list(map(lambda x: int(x[0] + '00'), status_codes.split(','))) if status_codes else []
        
        if sort_with == 'asc':
            sort_with_str = sort_by
        else:
            sort_with_str = '-' + sort_by

        namespaces = request.query_params.get('namespaces', None)
        namespaces = namespaces.split(',') if namespaces else None
        # Convert start_time str to timestamp in ms
        end_time = datetime.fromtimestamp(int(end_time) / 1000)
        start_time = datetime.fromtimestamp(int(start_time) / 1000)

        # Resource type can be:
        # 1- Pod
        # 2- Service
        # 3- Deployment
        # 4- DaemonSet
        # 5- Outbound
        # 6- Statefulset

        cache_start_time = datetime.now()
        
        # TODO: See if these can be removed
        cached_replica_sets, cached_deployments, cached_pods, cached_endpoints, cached_services, cached_daemon_sets, cached_outbounds, endpoint_to_service, cached_stateful_sets = cache_alaz_resources(
            cluster, namespaces)
        if settings.VERBOSE_LOG:
            verbose_log(f'SINGLE RESOURCE RETRIEVAL - Time to cache resources: {(datetime.now() - cache_start_time).total_seconds()}')
        
        preparing_resources_start_time = datetime.now()
        from_uid_pod = None
        from_uid_service = None
        from_uid_deployment = None
        from_uid_daemonset = None
        from_uid_statefulset = None
        to_uid_pod = None
        to_uid_service = None
        to_url_outbound = None
        to_uid_deployment = None
        to_uid_daemonset = None
        to_uid_statefulset = None

        # TODO: Move these to a helper
        if to_type == 'outbound':
            to_type_uuid = to_uid
        else:
            to_type_uuid = uuid.UUID(to_uid)
        if to_type == 'pod':
            to_resource = cached_pods.get(to_type_uuid, None)
            if not to_resource:
                raise exceptions.NotFoundError({"msg": "To resource not found"})
            to_uid_pod = to_uid
        elif to_type == 'service':
            to_resource = cached_services.get(to_type_uuid, None)
            if not to_resource:
                raise exceptions.NotFoundError({"msg": "To resource not found"})
            to_uid_service = to_uid
        elif to_type == 'outbound':
            outbound = Outbound(to_uid)
            to_resource = outbound
            cached_outbounds[to_uid] = outbound
            to_url_outbound = to_uid
        elif to_type == 'deployment':
            to_resource = cached_deployments.get(to_type_uuid, None)
            if not to_resource:
                raise exceptions.NotFoundError({"msg": "To resource not found"})
            to_uid_deployment = to_uid
        elif to_type == 'daemon_set':
            to_resource = cached_daemon_sets.get(to_type_uuid, None)
            if not to_resource:
                raise exceptions.NotFoundError({"msg": "To resource not found"})
            to_uid_daemonset = to_uid
        elif to_type == 'stateful_set':
            to_resource = cached_stateful_sets.get(to_type_uuid, None)
            if not to_resource:
                raise exceptions.NotFoundError({"msg": "To resource not found"})
            to_uid_statefulset = to_uid
        else:
            raise exceptions.ValidationError({"msg": "Invalid to type"})

        # TODO: Move these to a helper
        from_type_uuid = uuid.UUID(from_uid)
        if from_type == 'pod':
            from_resource = cached_pods.get(from_type_uuid, None)
            if not from_resource:
                raise exceptions.NotFoundError({"msg": "From resource not found"})
            from_uid_pod = from_uid
        elif from_type == 'service':
            from_resource = cached_services.get(from_type_uuid, None)
            if not from_resource:
                raise exceptions.NotFoundError({"msg": "From resource not found"})
            from_uid_service = from_uid
        elif from_type == 'deployment':
            from_resource = cached_deployments.get(from_type_uuid, None)
            if not from_resource:
                raise exceptions.NotFoundError({"msg": "From resource not found"})
            from_uid_deployment = from_uid
        elif from_type == 'daemon_set':
            from_resource = cached_daemon_sets.get(from_type_uuid, None)
            if not from_resource:
                raise exceptions.NotFoundError({"msg": "From resource not found"})
            from_uid_daemonset = from_uid
        elif from_type == 'stateful_set':
            from_resource = cached_stateful_sets.get(from_type_uuid, None)
            if not from_resource:
                raise exceptions.NotFoundError({"msg": "From resource not found"})
            from_uid_statefulset = from_uid
        else:
            raise exceptions.ValidationError({"msg": "Invalid from type"})

        if settings.VERBOSE_LOG:
            verbose_log(f'SINGLE RESOURCE RETRIEVAL - Time to prepare resources: {(datetime.now() - preparing_resources_start_time).total_seconds()}')
        
        traffic = {}

        time_diff = (end_time - start_time).total_seconds() if end_time > start_time else 1

        page_size = settings.ENDPOINTS_PAGE_SIZE
        pagination_start_offset = (page_num - 1) * page_size
        pagination_end_offset = page_num * page_size

        if protocol == type_kafka_event:
            endpoints = KafkaEvent.objects.filter(
                # from_uid_deployment=from_uid_deployment,
                # from_uid_daemonset=from_uid_daemonset,
                # from_uid_statefulset=from_uid_statefulset,
                from_uid_pod=from_uid_pod,
                from_uid_service=from_uid_service,
                to_uid_pod=to_uid_pod,
                to_uid_service=to_uid_service,
                # to_uid_deployment=to_uid_deployment,
                # to_uid_daemonset=to_uid_daemonset,
                # to_uid_statefulset=to_uid_statefulset,
                cluster=cluster.uid,
                start_time__gte=start_time,
                start_time__lte=end_time,
                to_port=to_port,
                type=event_type
            )

            if not from_uid_deployment and not from_uid_daemonset and not from_uid_statefulset:
                endpoints = endpoints.filter(
                    from_uid_pod=from_uid_pod,
                    from_uid_service=from_uid_service
                )
            if not to_uid_deployment and not to_uid_daemonset and not to_uid_statefulset:
                endpoints = endpoints.filter(
                    to_uid_pod=to_uid_pod,
                    to_uid_service=to_uid_service,
                    to_url_outbound=to_url_outbound
                )

            endpoints = endpoints.values(
                'topic',
            ).annotate(
                rps=Count('id') / time_diff,
                avg_latency=Avg('latency')
            ).order_by(sort_with_str)

            page_count = math.ceil(endpoints.count() / page_size)
            endpoints = list(endpoints[pagination_start_offset:pagination_end_offset])

            topics = list(map(lambda x: x['topic'], endpoints))
            # # for endpoint in endpoints:
            # #     # traffic[endpoint['path']] = {'request_methods': {}, 'avg_latency': endpoint['avg_latency'], 'rps': endpoint['rps']}
            # #     if endpoint['topic'] not in traffic:
            # #         traffic[endpoint['topic']] = {}

            endpoint_details = KafkaEvent.objects.filter(
                # from_uid_deployment=from_uid_deployment,
                # from_uid_daemonset=from_uid_daemonset,
                # from_uid_statefulset=from_uid_statefulset,
                from_uid_pod=from_uid_pod,
                from_uid_service=from_uid_service,
                # to_uid_deployment=to_uid_deployment,
                # to_uid_daemonset=to_uid_daemonset,
                # to_uid_statefulset=to_uid_statefulset,
                to_uid_pod=to_uid_pod,
                to_uid_service=to_uid_service,
                cluster=cluster.uid,
                start_time__gte=start_time,
                start_time__lte=end_time,
                to_port=to_port,
                topic__in=topics,
                type=event_type
            )

            if not from_uid_deployment and not from_uid_daemonset and not from_uid_statefulset:
                endpoint_details = endpoint_details.filter(
                    from_uid_pod=from_uid_pod,
                    from_uid_service=from_uid_service
                )
            if not to_uid_deployment and not to_uid_daemonset and not to_uid_statefulset:
                endpoint_details = endpoint_details.filter(
                    to_uid_pod=to_uid_pod,
                    to_uid_service=to_uid_service,
                    to_url_outbound=to_url_outbound
                )

            # endpoint_details_final = list(endpoint_details.values(
            #     'topic',
            #     'type',
            #     'value'
            # ).annotate(
            #     rps=Count('id') / time_diff,
            #     avg_latency=Avg('latency')
            # ))

            groups = endpoint_details.values(
                'topic',
                'value',
            ).annotate(
                request_count=Count('value')
            ).annotate(
                rank=Window(
                    expression=RowNumber(),
                    partition_by=[
                        F('topic'),
                        F('type'),
                    ],
                    order_by=F('request_count').desc()
                )
            ).filter(rank__lte=5)
            
            for endpoint in endpoints:
                topic = endpoint['topic']
                # event_type = endpoint_detail['type']
                rps = endpoint['rps']
                avg_latency = endpoint['avg_latency']
                # traffic.append(
                #     {
                #         'topic': topic,
                #         'type': event_type,
                #         'rps': rps,
                #         'avg_latency': avg_latency,
                #     }
                # )
                if topic not in traffic:
                    traffic[topic] = {'rps': rps, 'avg_latency': avg_latency}
                    # traffic[topic] = {'partitions': {}}
                # if partition not in traffic[topic]['partitions']:
                    # traffic[topic]['partitions'][partition] = {'event_types': {}}
                # if event_type not in traffic[topic]['partitions'][partition]['event_types']:
                    # traffic[topic]['partitions'][partition]['event_types'][event_type] = {'rps': rps, 'avg_latency': avg_latency}
                # if endpoint not in traffic and settings.VERBOSE_LOG:
                #     print(f'**VERBOSE_LOG: Endpoint {endpoint} not found in traffic')
                #     continue
                # if request_method not in traffic[endpoint]['request_methods']:
                #     traffic[endpoint]['request_methods'][request_method] = {'status_codes': {}}
                # if status_code not in traffic[endpoint]['request_methods'][request_method]['status_codes']:
                #     traffic[endpoint]['request_methods'][request_method]['status_codes'][status_code] = {'rps': rps, 'avg_latency': avg_latency}

            # TODO: Also add the most frequent samples
            for group in groups:
                topic = group['topic']
                if topic not in traffic:
                    continue

                if 'most_frequent_samples' not in traffic[topic]:
                    traffic[topic]['most_frequent_samples'] = []
                traffic[topic]['most_frequent_samples'].append(group['value'])
        else:
            endpoints = Request.objects.filter(
                from_uid_deployment=from_uid_deployment,
                from_uid_daemonset=from_uid_daemonset,
                from_uid_statefulset=from_uid_statefulset,
                # from_uid_pod=from_uid_pod,
                # from_uid_service=from_uid_service,
                to_uid_deployment=to_uid_deployment,
                to_uid_daemonset=to_uid_daemonset,
                to_uid_statefulset=to_uid_statefulset,
                # to_uid_pod=to_uid_pod,
                # to_uid_service=to_uid_service,
                cluster=cluster.uid,
                start_time__gte=start_time,
                start_time__lte=end_time,
                protocol=protocol,
                to_port=to_port
            )

            logger.info(f'Endpoints count: {endpoints.count()}')

            if not from_uid_deployment and not from_uid_daemonset and not from_uid_statefulset:
                endpoints = endpoints.filter(
                    from_uid_pod=from_uid_pod,
                    from_uid_service=from_uid_service
                )
            if not to_uid_deployment and not to_uid_daemonset and not to_uid_statefulset:
                endpoints = endpoints.filter(
                    to_uid_pod=to_uid_pod,
                    to_uid_service=to_uid_service,
                    to_url_outbound=to_url_outbound
                )
                
            logger.info(f'Endpoints count after filtering: {endpoints.count()}')

            if status_codes and protocol.lower() in ['http', 'https']:
                status_code_filters = Q()
                for status_code in status_codes:
                    status_code_filters |= Q(status_code__gte=status_code, status_code__lt=status_code + 100)
                endpoints = endpoints.filter(status_code_filters)
                
            logger.info(f'Endpoints count after status code filtering: {endpoints.count()}')

            endpoints = endpoints.values(
                'path'
            ).annotate(
                rps=Count('id') / time_diff,
                avg_latency=Avg('latency')
            ).order_by(sort_with_str)

            page_count = math.ceil(endpoints.count() / page_size)
            endpoints = list(endpoints[pagination_start_offset:pagination_end_offset])

            paths = list(map(lambda x: x['path'], endpoints))
            for endpoint in endpoints:
                traffic[endpoint['path']] = {'request_methods': {}, 'avg_latency': endpoint['avg_latency'], 'rps': endpoint['rps']}

            endpoint_details = Request.objects.filter(
                from_uid_deployment=from_uid_deployment,
                from_uid_daemonset=from_uid_daemonset,
                from_uid_statefulset=from_uid_statefulset,
                # from_uid_pod=from_uid_pod,
                # from_uid_service=from_uid_service,
                to_uid_deployment=to_uid_deployment,
                to_uid_daemonset=to_uid_daemonset,
                to_uid_statefulset=to_uid_statefulset,
                # to_uid_pod=to_uid_pod,
                # to_uid_service=to_uid_service,
                cluster=cluster.uid,
                start_time__gte=start_time,
                start_time__lte=end_time,
                protocol=protocol,
                to_port=to_port,
                path__in=paths
            )
            
            logger.info(f'Endpoint details count: {endpoint_details.count()}')

            if not from_uid_deployment and not from_uid_daemonset and not from_uid_statefulset:
                endpoint_details = endpoint_details.filter(
                    from_uid_pod=from_uid_pod,
                    from_uid_service=from_uid_service
                )
            if not to_uid_deployment and not to_uid_daemonset and not to_uid_statefulset:
                endpoint_details = endpoint_details.filter(
                    to_uid_pod=to_uid_pod,
                    to_uid_service=to_uid_service,
                    to_url_outbound=to_url_outbound
                )

            logger.info(f'Endpoint details count after filtering: {endpoint_details.count()}')

            if status_codes and protocol.lower() in ['http', 'https']:
                status_code_filters = Q()
                for status_code in status_codes:
                    status_code_filters |= Q(status_code__gte=status_code, status_code__lt=status_code + 100)
                endpoint_details = endpoint_details.filter(status_code_filters)
                
            logger.info(f'Endpoint details count after status code filtering: {endpoint_details.count()}')

            endpoint_details_final = list(endpoint_details.values(
                'path',
                'method',
                'status_code'
            ).annotate(
                rps=Count('id') / time_diff,
                avg_latency=Avg('latency')
            ))

            groups = endpoint_details.values(
                'path',
                'method',
                'status_code',
                'non_simplified_path'
            ).annotate(
                request_count=Count('non_simplified_path')
            ).annotate(
                rank=Window(
                    expression=RowNumber(),
                    partition_by=[
                        F('path'),
                        F('method'),
                        F('status_code'),
                    ],
                    order_by=F('request_count').desc()
                )
            ).filter(rank__lte=5)

            
            for group in endpoint_details_final:
                endpoint = group['path']
                request_method = group['method']
                status_code = group['status_code']
                rps = group['rps']
                avg_latency = group['avg_latency']
                if endpoint not in traffic and settings.VERBOSE_LOG:
                    print(f'**VERBOSE_LOG: Endpoint {endpoint} not found in traffic')
                    continue
                if request_method not in traffic[endpoint]['request_methods']:
                    traffic[endpoint]['request_methods'][request_method] = {'status_codes': {}}
                if status_code not in traffic[endpoint]['request_methods'][request_method]['status_codes']:
                    traffic[endpoint]['request_methods'][request_method]['status_codes'][status_code] = {'rps': rps, 'avg_latency': avg_latency}

            for group in groups:
                path = group['path']
                method = group['method']
                status_code = group['status_code']
                if path not in traffic:
                    continue
                if method not in traffic[path]['request_methods']:
                    continue
                if status_code not in traffic[path]['request_methods'][method]['status_codes']:
                    continue

                if 'most_frequent_samples' not in traffic[path]['request_methods'][method]['status_codes'][status_code]:
                    traffic[path]['request_methods'][method]['status_codes'][status_code]['most_frequent_samples'] = []
                traffic[path]['request_methods'][method]['status_codes'][status_code]['most_frequent_samples'].append(group['non_simplified_path'])

        return Response({'type': protocol, 'traffic': traffic, 'page_count': page_count}, status=status.HTTP_200_OK)
            
class ResourceMetricsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    # @silk_wrapper(name='Resource-Metrics')
    def get(self, request):
        verbose_log('ResourceMetricsView - Start')
        cluster, start_time, end_time = validate_service_map_get_method(request)

        resource_type = request.query_params.get('resource_type', None)
        if not resource_type:
            raise exceptions.ValidationError({"msg": "Resource type is required"})
        if resource_type not in ['pod', 'service', 'deployment', 'daemon_set', 'outbound', 'stateful_set']:
            raise exceptions.ValidationError({"msg": f"Invalid resource type {resource_type}"})

        resource_uid = request.query_params.get('resource_uid', None)
        if not resource_uid:
            raise exceptions.ValidationError({"msg": "Resource uid is required"})
        if resource_type != 'outbound':
            try:
                resource_uid = uuid.UUID(resource_uid)
            except Exception:
                raise exceptions.ValidationError({"msg": "Invalid resource uid"}) 

        namespaces = request.query_params.get('namespaces', None)
        namespaces = namespaces.split(',') if namespaces else None
        # Convert start_time str to timestamp in ms
        end_time = datetime.fromtimestamp(int(end_time) / 1000)
        start_time = datetime.fromtimestamp(int(start_time) / 1000)

        # TODO: See if these can be removed
        cached_replica_sets, cached_deployments, cached_pods, cached_endpoints, cached_services, cached_daemon_sets, cached_outbounds, endpoint_to_service, cached_stateful_sets = cache_alaz_resources(
            cluster, namespaces)
        
        verbose_log('ResourceMetricsView - Cache done')

        to_uid_pod = None
        to_uid_service = None
        to_url_outbound = None
        to_uid_deployment = None
        to_uid_daemonset = None
        to_uid_statefulset = None

        # TODO: Move these to a helper
        pointing_services = []
        if resource_type == 'pod':
            to_resource = cached_pods.get(resource_uid, None)
            if not to_resource:
                raise exceptions.NotFoundError({"msg": "To resource not found"})
            to_uid_pod = resource_uid
            services = alaz_find_services_pointing_to_pod(to_resource, cached_services, cached_endpoints, endpoint_to_service)
            pointing_services.extend(services)
        elif resource_type == 'service':
            to_resource = cached_services.get(resource_uid, None)
            if not to_resource:
                raise exceptions.NotFoundError({"msg": "To resource not found"})
            to_uid_service = resource_uid
        elif resource_type == 'outbound':
            outbound = Outbound(resource_uid)
            to_resource = outbound
            cached_outbounds[resource_uid] = outbound
            to_url_outbound = resource_uid
        elif resource_type == 'deployment':
            to_resource = cached_deployments.get(resource_uid, None)
            if not to_resource:
                raise exceptions.NotFoundError({"msg": "To resource not found"})
            to_uid_deployment = resource_uid
            replicasets = get_replicasets_of_deployment(to_resource, {'cached_deployments': cached_deployments})
            for replicaset in replicasets:
                pods = get_pods_of_replicaset(replicaset, {'cached_replicasets': cached_replica_sets})
                for pod in pods:
                    services = alaz_find_services_pointing_to_pod(pod, cached_services, cached_endpoints, endpoint_to_service)
                    pointing_services.extend(services)
        elif resource_type == 'daemon_set':
            to_resource = cached_daemon_sets.get(resource_uid, None)
            if not to_resource:
                raise exceptions.NotFoundError({"msg": "To resource not found"})
            to_uid_daemonset = resource_uid
            pods = get_pods_of_daemonset(to_resource, {'cached_daemon_sets': cached_daemon_sets})
            for pod in pods:
                services = alaz_find_services_pointing_to_pod(pod, cached_services, cached_endpoints, endpoint_to_service)
                pointing_services.extend(services)
        elif resource_type == 'stateful_set':
            to_resource = cached_stateful_sets.get(resource_uid, None)
            if not to_resource:
                raise exceptions.NotFoundError({"msg": "To resource not found"})
            to_uid_statefulset = resource_uid
            pods = get_pods_of_statefulset(to_resource, {'cached_stateful_sets': cached_stateful_sets})
            for pod in pods:
                services = alaz_find_services_pointing_to_pod(pod, cached_services, cached_endpoints, endpoint_to_service)
                pointing_services.extend(services)
        else:
            raise exceptions.ValidationError({"msg": "Invalid to type"})

        # Remove all duplicate services with the same uid
        unique_pointing_services = {}
        for service in pointing_services:
            unique_pointing_services[service.uid] = service
        pointing_services = [service.uid for service in unique_pointing_services.values()]
        pointing_services_str = "','".join(pointing_services)
        
        verbose_log('ResourceMetricsView - Resource found')
        
        time_diff = (end_time - start_time).total_seconds() if end_time > start_time else 1

        final_top_5_latencies = []
        final_top_5_rps = []
        http_codes = {}

        dest_filters = Q()

        if pointing_services:
            dest_filters |= Q(to_uid_service__isnull=False, to_uid_service__in=pointing_services)

        if to_uid_deployment:
            dest_filters |= Q(to_uid_deployment=to_uid_deployment)
        elif to_uid_daemonset:
            dest_filters |= Q(to_uid_daemonset=to_uid_daemonset)
        elif to_uid_statefulset:
            dest_filters |= Q(to_uid_statefulset=to_uid_statefulset)
        elif to_uid_pod:
            dest_filters |= Q(to_uid_pod=to_uid_pod)
        elif to_uid_service:
            dest_filters |= Q(to_uid_service=to_uid_service)
        elif to_url_outbound:
            dest_filters |= Q(to_url_outbound=to_url_outbound)

        verbose_log('ResourceMetricsView - Filters done')

        requests = Request.objects.filter(
            dest_filters,
            cluster=cluster.uid,
            start_time__gte=start_time,
            start_time__lte=end_time
        )

        top_5_latencies_requests = list(requests.values(
            'protocol', 'path').annotate(
            avg_latency = Avg('latency'),
        ).order_by('-avg_latency')[:5])

        for group in top_5_latencies_requests:
            group['group_type'] = type_request

        top_5_rps_requests = list(requests.values(
            'protocol', 'path').annotate(
            rps = Count('id') / time_diff,
        ).order_by('-rps')[:5])
        
        for group in top_5_rps_requests:
            group['group_type'] = type_request
        
        kafka_events = KafkaEvent.objects.filter(
            dest_filters,
            cluster=cluster.uid,
            start_time__gte=start_time,
            start_time__lte=end_time
        )

        top_5_latencies_kafka_events = list(kafka_events.values(
            'topic', 'type').annotate(
            avg_latency = Avg('latency'),
        ).order_by('-avg_latency')[:5])   

        for group in top_5_latencies_kafka_events:
            group['group_type'] = type_kafka_event

        top_5_rps_kafka_events = list(kafka_events.values(
            'topic', 'type').annotate(
            rps = Count('id') / time_diff,
        ).order_by('-rps')[:5])
        
        for group in top_5_rps_kafka_events:
            group['group_type'] = type_kafka_event

        # For each elem in top_5_latencies_requests and top_5_latencies_kafka_events, compare them get the top 5
        
        # Step 1: Combine the lists
        combined_latencies = top_5_latencies_requests + top_5_latencies_kafka_events

        # Step 2: Sort the combined list by avg_latency in descending order
        combined_latencies_sorted = sorted(combined_latencies, key=lambda x: x['avg_latency'], reverse=True)

        # Step 3: Select the top 5
        top_5_latencies = combined_latencies_sorted[:5]

        # Do the same for rps
        
        # Step 1: Combine the lists
        combined_rps = top_5_rps_requests + top_5_rps_kafka_events
        
        # Step 2: Sort the combined list by rps in descending order
        combined_rps_sorted = sorted(combined_rps, key=lambda x: x['rps'], reverse=True)
        
        # Step 3: Select the top 5
        top_5_rps = combined_rps_sorted[:5]

        req_count_total = 0
        req_count_5xx = 0

        status_code_count = settings.RESOURCE_METRICS_STATUS_CODE_COUNT
        if settings.VERBOSE_LOG:
            print(f'**VERBOSE_LOG: end_time: {end_time}, start_time: {start_time}, interval: {int((end_time - start_time).total_seconds() / status_code_count)}')
        interval = (end_time - start_time).total_seconds() / status_code_count

        verbose_log('ResourceMetricsView - Opening cursor')
        cursor = connection.cursor()
        if to_uid_pod:
            query = '''
                WITH intervals AS (
                    SELECT
                        generate_series(
                            date_trunc('second', %s),
                            date_trunc('second', %s),
                            '%s seconds'::interval
                        ) AS time_interval
                )
                SELECT
                    intervals.time_interval,
                    status_code,
                    COUNT(*) AS row_count
                FROM
                    intervals
                LEFT JOIN
                    (SELECT * from core_request 
                        where 
                            start_time >= %s and 
                            start_time < %s and 
                            to_uid_pod = %s and
                            cluster = %s
                    ) as subq ON date_trunc('second', start_time) >= intervals.time_interval
                        AND date_trunc('second', start_time) < intervals.time_interval + '%s seconds'::interval
                GROUP BY intervals.time_interval, status_code
                ORDER BY intervals.time_interval;
                '''
            if pointing_services:
                query = query.replace('to_uid_pod = %s', f'(to_uid_pod = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
            cursor.execute(query
                , [start_time, end_time, interval, start_time, end_time, to_uid_pod, cluster.uid, interval])
        elif to_uid_service:
            query = '''
                WITH intervals AS (
                    SELECT
                        generate_series(
                            date_trunc('second', %s),
                            date_trunc('second', %s),
                            '%s seconds'::interval
                        ) AS time_interval
                )
                SELECT
                    intervals.time_interval,
                    status_code,
                    COUNT(*) AS row_count
                FROM
                    intervals
                LEFT JOIN
                    (SELECT * from core_request 
                        where 
                            start_time >= %s and 
                            start_time < %s and 
                            to_uid_service = %s and
                            cluster = %s
                    ) as subq ON date_trunc('second', start_time) >= intervals.time_interval
                        AND date_trunc('second', start_time) < intervals.time_interval + '%s seconds'::interval
                GROUP BY intervals.time_interval, status_code
                ORDER BY intervals.time_interval;
                '''
            cursor.execute(query, [start_time, end_time, interval, start_time, end_time, to_uid_service, cluster.uid, interval])
        elif to_url_outbound:
            query = '''
                WITH intervals AS (
                    SELECT
                        generate_series(
                            date_trunc('second', %s),
                            date_trunc('second', %s),
                            '%s seconds'::interval
                        ) AS time_interval
                )
                SELECT
                    intervals.time_interval,
                    status_code,
                    COUNT(*) AS row_count
                FROM
                    intervals
                LEFT JOIN
                    (SELECT * from core_request 
                        where 
                            start_time >= %s and 
                            start_time < %s and 
                            to_url_outbound = %s and
                            cluster = %s
                    ) as subq ON date_trunc('second', start_time) >= intervals.time_interval
                        AND date_trunc('second', start_time) < intervals.time_interval + '%s seconds'::interval
                GROUP BY intervals.time_interval, status_code
                ORDER BY intervals.time_interval;
                '''
            cursor.execute(query, [start_time, end_time, interval, start_time, end_time, to_url_outbound, cluster.uid, interval])
        elif to_uid_deployment:
            query = '''
                WITH intervals AS (
                    SELECT
                        generate_series(
                            date_trunc('second', %s),
                            date_trunc('second', %s),
                            '%s seconds'::interval
                        ) AS time_interval
                )
                SELECT
                    intervals.time_interval,
                    status_code,
                    COUNT(*) AS row_count
                FROM
                    intervals
                LEFT JOIN
                    (SELECT * from core_request 
                        where 
                            start_time >= %s and 
                            start_time < %s and 
                            to_uid_deployment = %s and
                            cluster = %s
                    ) as subq ON date_trunc('second', start_time) >= intervals.time_interval
                        AND date_trunc('second', start_time) < intervals.time_interval + '%s seconds'::interval
                GROUP BY intervals.time_interval, status_code
                ORDER BY intervals.time_interval;            
                '''
            if pointing_services:
                query = query.replace('to_uid_deployment = %s', f'(to_uid_deployment = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
            cursor.execute(query, [start_time, end_time, interval, start_time, end_time, to_uid_deployment, cluster.uid, interval])
        elif to_uid_daemonset:
            query = '''
                WITH intervals AS (
                    SELECT
                        generate_series(
                            date_trunc('second', %s),
                            date_trunc('second', %s),
                            '%s seconds'::interval
                        ) AS time_interval
                )
                SELECT
                    intervals.time_interval,
                    status_code,
                    COUNT(*) AS row_count
                FROM
                    intervals
                LEFT JOIN
                    (SELECT * from core_request 
                        where 
                            start_time >= %s and 
                            start_time < %s and 
                            to_uid_daemonset = %s and
                            cluster = %s
                    ) as subq ON date_trunc('second', start_time) >= intervals.time_interval
                        AND date_trunc('second', start_time) < intervals.time_interval + '%s seconds'::interval
                GROUP BY intervals.time_interval, status_code
                ORDER BY intervals.time_interval;
                '''
            if pointing_services:
                query = query.replace('to_uid_daemonset = %s', f'(to_uid_daemonset = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
            cursor.execute(query, [start_time, end_time, interval, start_time, end_time, to_uid_daemonset, cluster.uid, interval])
        elif to_uid_statefulset:
            query = '''
                WITH intervals AS (
                    SELECT
                        generate_series(
                            date_trunc('second', %s),
                            date_trunc('second', %s),
                            '%s seconds'::interval
                        ) AS time_interval
                )
                SELECT
                    intervals.time_interval,
                    status_code,
                    COUNT(*) AS row_count
                FROM
                    intervals
                LEFT JOIN
                    (SELECT * from core_request 
                        where 
                            start_time >= %s and 
                            start_time < %s and 
                            to_uid_statefulset = %s and
                            cluster = %s
                    ) as subq ON date_trunc('second', start_time) >= intervals.time_interval
                        AND date_trunc('second', start_time) < intervals.time_interval + '%s seconds'::interval
                GROUP BY intervals.time_interval, status_code
                ORDER BY intervals.time_interval;
                '''
            if pointing_services:
                query = query.replace('to_uid_statefulset = %s', f'(to_uid_statefulset = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
            cursor.execute(query, [start_time, end_time, interval, start_time, end_time, to_uid_statefulset, cluster.uid, interval])

        status_codes = cursor.fetchall()
        verbose_log('ResourceMetricsView - Status codes done')

        for row in status_codes:
            timestamp = int(row[0].timestamp() * 1000)
            status_code = row[1]
            request_count = row[2]
            if timestamp not in http_codes:
                http_codes[timestamp] = {}
            if status_code is None:
                continue
            if status_code not in http_codes[timestamp]:
                http_codes[timestamp][status_code] = 0
            http_codes[timestamp][status_code] += request_count
            if status_code >= 500:
                req_count_5xx += request_count
            req_count_total += request_count

        # Remove the ending and starting timestamps where no data is present
        first_invalid_timestamp = None
        first_valid_timestamp = None
        for timestamp, data in reversed(http_codes.items()):
            if data == {}:
                first_invalid_timestamp = timestamp
            else:
                break

        for timestamp, data in http_codes.items():
            if data != {}:
                first_valid_timestamp = timestamp
                break

        processed_status_code_timestamps = []
        for timestamp, data in http_codes.items():
            if first_invalid_timestamp and timestamp >= first_invalid_timestamp:
                break
            elif first_valid_timestamp and timestamp < first_valid_timestamp:
                continue
            data['timestamp'] = timestamp
            processed_status_code_timestamps.append(data)

        http_codes = processed_status_code_timestamps

        percentage_of_5xx = req_count_5xx / req_count_total if req_count_total > 0 else 0

        endpoints = dict()
        for latency in top_5_latencies:
            if latency['group_type'] == type_kafka_event:
                topic = latency['topic']
                event_type = latency['type']
                endpoints[(topic, event_type)] = set()
            else:
                protocol = latency['protocol']
                path = latency['path']
                endpoints[(protocol, path)] = set()

        for rps in top_5_rps:
            if rps['group_type'] == type_kafka_event:
                topic = rps['topic']
                event_type = rps['type']
                endpoints[(topic, event_type)] = set()
            else:
                protocol = rps['protocol']
                path = rps['path']
                endpoints[(protocol, path)] = set()

        verbose_log('ResourceMetricsView - Top 5 done')
        for latency in top_5_latencies:
            group_type = latency['group_type']
            if group_type == type_kafka_event:
                topic = latency['topic']
                event_type = latency['type']
            else:
                protocol = latency['protocol']
                path = latency['path']


            if to_uid_pod:
                if group_type == type_kafka_event:
                    query = '''
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY latency) as p99,
                        min(latency) as min_latency,
                        max(latency) as max_latency,
                        avg(latency) as avg_latency
                        FROM core_kafkaevent where to_uid_pod = %s and cluster = %s and start_time >= %s and start_time <= %s and topic = %s and type = %s
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_pod = %s', f'(to_uid_pod = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [to_uid_pod, cluster.uid, start_time, end_time, topic, event_type]
                    )
                else:
                    query = '''
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY latency) as p99,
                        min(latency) as min_latency,
                        max(latency) as max_latency,
                        avg(latency) as avg_latency
                        FROM core_request where to_uid_pod = %s and cluster = %s and start_time >= %s and start_time <= %s and protocol = %s and path = %s
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_pod = %s', f'(to_uid_pod = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [to_uid_pod, cluster.uid, start_time, end_time, protocol, path]
                )
            elif to_uid_service:
                if group_type == type_kafka_event:
                    query = '''
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY latency) as p99,
                        min(latency) as min_latency,
                        max(latency) as max_latency,
                        avg(latency) as avg_latency
                        FROM core_kafkaevent where to_uid_service = %s and cluster = %s and start_time >= %s and start_time <= %s and topic = %s and type = %s
                        '''
                    cursor.execute(query, [to_uid_service, cluster.uid, start_time, end_time, topic, event_type]
                    )
                else:
                    query = '''
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY latency) as p99,
                        min(latency) as min_latency,
                        max(latency) as max_latency,
                        avg(latency) as avg_latency
                        FROM core_request where to_uid_service = %s and cluster = %s and start_time >= %s and start_time <= %s and protocol = %s and path = %s
                    '''
                    cursor.execute(query, [to_uid_service, cluster.uid, start_time, end_time, protocol, path]
                    )
            elif to_url_outbound:
                if group_type == type_kafka_event:
                    query = '''
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY latency) as p99,
                        min(latency) as min_latency,
                        max(latency) as max_latency,
                        avg(latency) as avg_latency
                        FROM core_kafkaevent where to_url_outbound = %s and cluster = %s and start_time >= %s and start_time <= %s and topic = %s and type = %s
                        '''
                    cursor.execute(query, [to_url_outbound, cluster.uid, start_time, end_time, topic, event_type]
                    )
                else:
                    query = '''
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY latency) as p99,
                        min(latency) as min_latency,
                        max(latency) as max_latency,
                        avg(latency) as avg_latency
                        FROM core_request where to_url_outbound = %s and cluster = %s and start_time >= %s and start_time <= %s and protocol = %s and path = %s
                        '''
                    cursor.execute(query, [to_url_outbound, cluster.uid, start_time, end_time, protocol, path]
                    )
            elif to_uid_deployment:
                if group_type == type_kafka_event:
                    query = '''
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY latency) as p99,
                        min(latency) as min_latency,
                        max(latency) as max_latency,
                        avg(latency) as avg_latency
                        FROM core_kafkaevent where to_uid_deployment = %s and cluster = %s and start_time >= %s and start_time <= %s and topic = %s and type = %s
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_deployment = %s', f'(to_uid_deployment = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [to_uid_deployment, cluster.uid, start_time, end_time, topic, event_type]
                    )
                else:
                    query = '''
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY latency) as p99,
                        min(latency) as min_latency,
                        max(latency) as max_latency,
                        avg(latency) as avg_latency
                        FROM core_request where to_uid_deployment = %s and cluster = %s and start_time >= %s and start_time <= %s and protocol = %s and path = %s
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_deployment = %s', f'(to_uid_deployment = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [to_uid_deployment, cluster.uid, start_time, end_time, protocol, path]
                    )
            elif to_uid_daemonset:
                if group_type == type_kafka_event:
                    query = '''
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY latency) as p99,
                        min(latency) as min_latency,
                        max(latency) as max_latency,
                        avg(latency) as avg_latency
                        FROM core_kafkaevent where to_uid_daemonset = %s and cluster = %s and start_time >= %s and start_time <= %s and topic = %s and type = %s
                        '''
                    cursor.execute(query, [to_uid_daemonset, cluster.uid, start_time, end_time, topic, event_type]
                    )
                else:
                    query = '''
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY latency) as p99,
                        min(latency) as min_latency,
                        max(latency) as max_latency,
                        avg(latency) as avg_latency
                        FROM core_request where to_uid_daemonset = %s and cluster = %s and start_time >= %s and start_time <= %s and protocol = %s and path = %s
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_daemonset = %s', f'(to_uid_daemonset = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [to_uid_daemonset, cluster.uid, start_time, end_time, protocol, path]
                )
            elif to_uid_statefulset:
                if type == type_kafka_event:
                    query = '''
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY latency) as p99,
                        min(latency) as min_latency,
                        max(latency) as max_latency,
                        avg(latency) as avg_latency
                        FROM core_kafkaevent where to_uid_statefulset = %s and cluster = %s and start_time >= %s and start_time <= %s and topic = %s and type = %s
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_statefulset = %s', f'(to_uid_statefulset = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [to_uid_statefulset, cluster.uid, start_time, end_time, topic, event_type]
                    )
                else:
                    query = '''
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY latency) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY latency) as p99,
                        min(latency) as min_latency,
                        max(latency) as max_latency,
                        avg(latency) as avg_latency
                        FROM core_request where to_uid_statefulset = %s and cluster = %s and start_time >= %s and start_time <= %s and protocol = %s and path = %s
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_statefulset = %s', f'(to_uid_statefulset = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [to_uid_statefulset, cluster.uid, start_time, end_time, protocol, path]
                    )

            percentile_data = cursor.fetchone()

            data = {}
            
            if group_type == type_kafka_event:
                data['protocol'] = type_kafka_event
                data['topic'] = topic
                data['type'] = event_type
            else:
                data['protocol'] = protocol
                data['endpoint'] = path

            data['latency_p95'] = percentile_data[0]
            data['latency_p99'] = percentile_data[1]
            data['min_latency'] = percentile_data[2]
            data['max_latency'] = percentile_data[3]
            data['avg_latency'] = percentile_data[4]
            # data['sources'] = list(endpoints[(protocol, path)])
            final_top_5_latencies.append(data)
            
        verbose_log('ResourceMetricsView - Latencies done')

        for rps in top_5_rps:
            group_type = rps['group_type']
            if group_type == type_kafka_event:
                topic = rps['topic']
                event_type = rps['type']
            else:
                protocol = rps['protocol']
                path = rps['path']

            if to_uid_pod:
                if group_type == type_kafka_event:
                    query = '''
                    WITH generate_series AS (
                    SELECT generate_series(
                    date_trunc('second', %s),  -- Start time: Current time minus 5 minutes
                    date_trunc('second', %s),                        -- End time: Current time
                    '1 second'::interval
                    ) AS time_interval
                    )
                    SELECT
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY request_count) as p95,
                    percentile_cont(0.99) WITHIN GROUP (ORDER BY request_count) as p99,
                    min(request_count) as min_rps,
                    max(request_count) as max_rps,
                    avg(request_count) as rps
                    FROM
                    (SELECT
                        gs.time_interval as second_bucket,
                        COALESCE(COUNT(cr.id), 0) AS request_count
                    FROM
                        generate_series gs
                    LEFT JOIN
                        (SELECT * FROM public.core_kafkaevent
                    WHERE
                        start_time >= %s and
                        start_time < %s and
                        to_uid_pod = %s and
                        cluster = %s and 
                        topic = %s and
                        type = %s) cr
                    ON
                        date_trunc('second', cr.start_time) = gs.time_interval
                    GROUP BY
                        gs.time_interval
                    ORDER BY
                        gs.time_interval) as buckets;
                    '''
                    if pointing_services:
                        query = query.replace('to_uid_pod = %s', f'(to_uid_pod = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [start_time, end_time, start_time, end_time, to_uid_pod, cluster.uid, topic, event_type])
                else:
                    query = '''
                        WITH generate_series AS (
                        SELECT generate_series(
                        date_trunc('second', %s),  -- Start time: Current time minus 5 minutes
                        date_trunc('second', %s),                        -- End time: Current time
                        '1 second'::interval
                        ) AS time_interval
                        )
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY request_count) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY request_count) as p99,
                        min(request_count) as min_rps,
                        max(request_count) as max_rps,
                        avg(request_count) as rps
                        FROM
                        (SELECT
                            gs.time_interval as second_bucket,
                            COALESCE(COUNT(cr.id), 0) AS request_count
                        FROM
                            generate_series gs
                        LEFT JOIN
                            (SELECT * FROM public.core_request
                        WHERE
                            start_time >= %s and
                            start_time < %s and
                            to_uid_pod = %s and
                            cluster = %s and 
                            protocol = %s and
                            path = %s) cr
                        ON
                            date_trunc('second', cr.start_time) = gs.time_interval
                        GROUP BY
                            gs.time_interval
                        ORDER BY
                            gs.time_interval) as buckets;
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_pod = %s', f'(to_uid_pod = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [start_time, end_time, start_time, end_time, to_uid_pod, cluster.uid, protocol, path])

            elif to_uid_service:
                if group_type == type_kafka_event:
                    query = '''
                        WITH generate_series AS (
                        SELECT generate_series(
                        date_trunc('second', %s),  -- Start time: Current time minus 5 minutes
                        date_trunc('second', %s),                        -- End time: Current time
                        '1 second'::interval
                        ) AS time_interval
                        )
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY request_count) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY request_count) as p99,
                        min(request_count) as min_rps,
                        max(request_count) as max_rps,
                        avg(request_count) as rps
                        FROM
                        (SELECT
                            gs.time_interval as second_bucket,
                            COALESCE(COUNT(cr.id), 0) AS request_count
                        FROM
                            generate_series gs
                        LEFT JOIN
                            (SELECT * FROM public.core_kafkaevent
                        WHERE
                            start_time >= %s and
                            start_time < %s and
                            to_uid_service = %s and
                            cluster = %s and 
                            topic = %s and
                            type = %s) cr
                        ON
                            date_trunc('second', cr.start_time) = gs.time_interval
                        GROUP BY
                            gs.time_interval
                        ORDER BY
                            gs.time_interval) as buckets;
                        '''
                    cursor.execute(query, [start_time, end_time, start_time, end_time, to_uid_service, cluster.uid, topic, event_type])
                else:
                    query = '''
                        WITH generate_series AS (
                        SELECT generate_series(
                        date_trunc('second', %s),  -- Start time: Current time minus 5 minutes
                        date_trunc('second', %s),                        -- End time: Current time
                        '1 second'::interval
                        ) AS time_interval
                        )
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY request_count) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY request_count) as p99,
                        min(request_count) as min_rps,
                        max(request_count) as max_rps,
                        avg(request_count) as rps
                        FROM
                        (SELECT
                            gs.time_interval as second_bucket,
                            COALESCE(COUNT(cr.id), 0) AS request_count
                        FROM
                            generate_series gs
                        LEFT JOIN
                            (SELECT * FROM public.core_request
                        WHERE
                            start_time >= %s and
                            start_time < %s and
                            to_uid_service = %s and
                            cluster = %s and 
                            protocol = %s and
                            path = %s) cr
                        ON
                            date_trunc('second', cr.start_time) = gs.time_interval
                        GROUP BY
                            gs.time_interval
                        ORDER BY
                            gs.time_interval) as buckets;
                        '''
                    cursor.execute(query, [start_time, end_time, start_time, end_time, to_uid_service, cluster.uid, protocol, path])
            elif to_url_outbound:
                if group_type == type_kafka_event:
                    query = '''
                        WITH generate_series AS (
                        SELECT generate_series(
                        date_trunc('second', %s),  -- Start time: Current time minus 5 minutes
                        date_trunc('second', %s),                        -- End time: Current time
                        '1 second'::interval
                        ) AS time_interval
                        )
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY request_count) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY request_count) as p99,
                        min(request_count) as min_rps,
                        max(request_count) as max_rps,
                        avg(request_count) as rps
                        FROM
                        (SELECT
                            gs.time_interval as second_bucket,
                            COALESCE(COUNT(cr.id), 0) AS request_count
                        FROM
                            generate_series gs
                        LEFT JOIN
                            (SELECT * FROM public.core_kafkaevent
                        WHERE
                            start_time >= %s and
                            start_time < %s and
                            to_url_outbound = %s and
                            cluster = %s and 
                            topic = %s and
                            type = %s) cr
                        ON
                            date_trunc('second', cr.start_time) = gs.time_interval
                        GROUP BY
                            gs.time_interval
                        ORDER BY
                            gs.time_interval) as buckets;
                        '''
                    cursor.execute(query, [start_time, end_time, start_time, end_time, to_url_outbound, cluster.uid, topic, event_type])
                else:
                    query = '''
                        WITH generate_series AS (
                        SELECT generate_series(
                        date_trunc('second', %s),  -- Start time: Current time minus 5 minutes
                        date_trunc('second', %s),                        -- End time: Current time
                        '1 second'::interval
                        ) AS time_interval
                        )
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY request_count) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY request_count) as p99,
                        min(request_count) as min_rps,
                        max(request_count) as max_rps,
                        avg(request_count) as rps
                        FROM
                        (SELECT
                            gs.time_interval as second_bucket,
                            COALESCE(COUNT(cr.id), 0) AS request_count
                        FROM
                            generate_series gs
                        LEFT JOIN
                            (SELECT * FROM public.core_request
                        WHERE
                            start_time >= %s and
                            start_time < %s and
                            to_url_outbound = %s and
                            cluster = %s and 
                            protocol = %s and
                            path = %s) cr
                        ON
                            date_trunc('second', cr.start_time) = gs.time_interval
                        GROUP BY
                            gs.time_interval
                        ORDER BY
                            gs.time_interval) as buckets;
                        '''
                    cursor.execute(query, [start_time, end_time, start_time, end_time, to_url_outbound, cluster.uid, protocol, path])
            elif to_uid_deployment:
                if group_type == type_kafka_event:
                    query = '''
                        WITH generate_series AS (
                        SELECT generate_series(
                        date_trunc('second', %s),  -- Start time: Current time minus 5 minutes
                        date_trunc('second', %s),                        -- End time: Current time
                        '1 second'::interval
                        ) AS time_interval
                        )
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY request_count) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY request_count) as p99,
                        min(request_count) as min_rps,
                        max(request_count) as max_rps,
                        avg(request_count) as rps
                        FROM
                        (SELECT
                            gs.time_interval as second_bucket,
                            COALESCE(COUNT(cr.id), 0) AS request_count
                        FROM
                            generate_series gs
                        LEFT JOIN
                            (SELECT * FROM public.core_kafkaevent
                        WHERE
                            start_time >= %s and
                            start_time < %s and
                            to_uid_deployment = %s and
                            cluster = %s and 
                            topic = %s and
                            type = %s) cr
                        ON
                            date_trunc('second', cr.start_time) = gs.time_interval
                        GROUP BY
                            gs.time_interval
                        ORDER BY
                            gs.time_interval) as buckets;
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_deployment = %s', f'(to_uid_deployment = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [start_time, end_time, start_time, end_time, to_uid_deployment, cluster.uid, topic, event_type])
                else:
                    query = '''
                        WITH generate_series AS (
                        SELECT generate_series(
                        date_trunc('second', %s),  -- Start time: Current time minus 5 minutes
                        date_trunc('second', %s),                        -- End time: Current time
                        '1 second'::interval
                        ) AS time_interval
                        )
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY request_count) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY request_count) as p99,
                        min(request_count) as min_rps,
                        max(request_count) as max_rps,
                        avg(request_count) as rps
                        FROM
                        (SELECT
                            gs.time_interval as second_bucket,
                            COALESCE(COUNT(cr.id), 0) AS request_count
                        FROM
                            generate_series gs
                        LEFT JOIN
                            (SELECT * FROM public.core_request
                        WHERE
                            start_time >= %s and
                            start_time < %s and
                            to_uid_deployment = %s and
                            cluster = %s and 
                            protocol = %s and
                            path = %s) cr
                        ON
                            date_trunc('second', cr.start_time) = gs.time_interval
                        GROUP BY
                            gs.time_interval
                        ORDER BY
                            gs.time_interval) as buckets;
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_deployment = %s', f'(to_uid_deployment = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [start_time, end_time, start_time, end_time, to_uid_deployment, cluster.uid, protocol, path])
            elif to_uid_daemonset:
                if group_type == type_kafka_event:
                    query = '''
                        WITH generate_series AS (
                        SELECT generate_series(
                        date_trunc('second', %s),  -- Start time: Current time minus 5 minutes
                        date_trunc('second', %s),                        -- End time: Current time
                        '1 second'::interval
                        ) AS time_interval
                        )
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY request_count) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY request_count) as p99,
                        min(request_count) as min_rps,
                        max(request_count) as max_rps,
                        avg(request_count) as rps
                        FROM
                        (SELECT
                            gs.time_interval as second_bucket,
                            COALESCE(COUNT(cr.id), 0) AS request_count
                        FROM
                            generate_series gs
                        LEFT JOIN
                            (SELECT * FROM public.core_kafkaevent
                        WHERE
                            start_time >= %s and
                            start_time < %s and
                            to_uid_daemonset = %s and
                            cluster = %s and 
                            topic = %s and
                            type = %s) cr
                        ON
                            date_trunc('second', cr.start_time) = gs.time_interval
                        GROUP BY
                            gs.time_interval
                        ORDER BY
                            gs.time_interval) as buckets;
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_daemonset = %s', f'(to_uid_daemonset = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [start_time, end_time, start_time, end_time, to_uid_daemonset, cluster.uid, topic, event_type])
                else:
                    query = '''
                        WITH generate_series AS (
                        SELECT generate_series(
                        date_trunc('second', %s),  -- Start time: Current time minus 5 minutes
                        date_trunc('second', %s),                        -- End time: Current time
                        '1 second'::interval
                        ) AS time_interval
                        )
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY request_count) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY request_count) as p99,
                        min(request_count) as min_rps,
                        max(request_count) as max_rps,
                        avg(request_count) as rps
                        FROM
                        (SELECT
                            gs.time_interval as second_bucket,
                            COALESCE(COUNT(cr.id), 0) AS request_count
                        FROM
                            generate_series gs
                        LEFT JOIN
                            (SELECT * FROM public.core_request
                        WHERE
                            start_time >= %s and
                            start_time < %s and
                            to_uid_daemonset = %s and
                            cluster = %s and 
                            protocol = %s and
                            path = %s) cr
                        ON
                            date_trunc('second', cr.start_time) = gs.time_interval
                        GROUP BY
                            gs.time_interval
                        ORDER BY
                            gs.time_interval) as buckets;
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_daemonset = %s', f'(to_uid_daemonset = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [start_time, end_time, start_time, end_time, to_uid_daemonset, cluster.uid, protocol, path])
            elif to_uid_statefulset:
                if group_type == type_kafka_event:
                    query = '''
                        WITH generate_series AS (
                        SELECT generate_series(
                        date_trunc('second', %s),  -- Start time: Current time minus 5 minutes
                        date_trunc('second', %s),                        -- End time: Current time
                        '1 second'::interval
                        ) AS time_interval
                        )
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY request_count) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY request_count) as p99,
                        min(request_count) as min_rps,
                        max(request_count) as max_rps,
                        avg(request_count) as rps
                        FROM
                        (SELECT
                            gs.time_interval as second_bucket,
                            COALESCE(COUNT(cr.id), 0) AS request_count
                        FROM
                            generate_series gs
                        LEFT JOIN
                            (SELECT * FROM public.core_kafkaevent
                        WHERE
                            start_time >= %s and
                            start_time < %s and
                            to_uid_statefulset = %s and
                            cluster = %s and 
                            topic = %s and
                            type = %s) cr
                        ON
                            date_trunc('second', cr.start_time) = gs.time_interval
                        GROUP BY
                            gs.time_interval
                        ORDER BY
                            gs.time_interval) as buckets;
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_statefulset = %s', f'(to_uid_statefulset = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [start_time, end_time, start_time, end_time, to_uid_statefulset, cluster.uid, topic, event_type])
                else:
                    query = '''
                        WITH generate_series AS (
                        SELECT generate_series(
                        date_trunc('second', %s),  -- Start time: Current time minus 5 minutes
                        date_trunc('second', %s),                        -- End time: Current time
                        '1 second'::interval
                        ) AS time_interval
                        )
                        SELECT
                        percentile_cont(0.95) WITHIN GROUP (ORDER BY request_count) as p95,
                        percentile_cont(0.99) WITHIN GROUP (ORDER BY request_count) as p99,
                        min(request_count) as min_rps,
                        max(request_count) as max_rps,
                        avg(request_count) as rps
                        FROM
                        (SELECT
                            gs.time_interval as second_bucket,
                            COALESCE(COUNT(cr.id), 0) AS request_count
                        FROM
                            generate_series gs
                        LEFT JOIN
                            (SELECT * FROM public.core_request
                        WHERE
                            start_time >= %s and
                            start_time < %s and
                            to_uid_statefulset = %s and
                            cluster = %s and 
                            protocol = %s and
                            path = %s) cr
                        ON
                            date_trunc('second', cr.start_time) = gs.time_interval
                        GROUP BY
                            gs.time_interval
                        ORDER BY
                            gs.time_interval) as buckets;
                        '''
                    if pointing_services:
                        query = query.replace('to_uid_statefulset = %s', f'(to_uid_statefulset = %s or (to_uid_service is not null and to_uid_service in (\'{pointing_services_str}\')))')
                    cursor.execute(query, [start_time, end_time, start_time, end_time, to_uid_statefulset, cluster.uid, protocol, path])

            percentile_data = cursor.fetchone()
            data = {}
            if group_type == type_kafka_event:
                data['protocol'] = type_kafka_event
                data['topic'] = topic
                data['type'] = event_type
            else:
                data['protocol'] = protocol
                data['endpoint'] = path

            data['rps_p95'] = percentile_data[0]
            data['rps_p99'] = percentile_data[1]
            data['min_rps'] = percentile_data[2]
            data['max_rps'] = percentile_data[3]
            data['rps'] = percentile_data[4]
            # data['sources'] = list(endpoints[(protocol, path)])
            final_top_5_rps.append(data)
            
        cursor.close()
            
        verbose_log('ResourceMetricsView - RPS done')

        return Response({'top_5_latencies': final_top_5_latencies, 'top_5_rps': final_top_5_rps, 'http_codes': http_codes, 'percentage_of_5xx': percentage_of_5xx}, status=status.HTTP_200_OK)

class MetricsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def get(self, request):
        cluster, start_time, end_time = validate_metrics_get_method(request)

        instance = request.query_params.get('instance', None)
        if not instance:
            raise exceptions.ValidationError({"msg": "Instance is required"})

        max_data_points = settings.PROMETHEUS_MAX_DATA_POINTS
        scrape_interval_seconds = settings.PROMETHEUS_SCRAPE_INTERVAL_SECONDS
        interval = str(scrape_interval_seconds * 4) + 's'
        data_points = settings.PROMETHEUS_DATA_POINTS
        step_size = int((end_time - start_time) / data_points)
        start_time /= 1000  # convert to seconds
        end_time /= 1000  # convert to seconds
        step_size /= 1000  # convert to seconds
        job = 'alaz'

        if step_size <= 0:
            raise exceptions.ValidationError({"msg": "Step size must be positive"})

        data_points = int((end_time - start_time) / step_size) + 1
        if data_points > max_data_points:
            raise exceptions.ValidationError({"msg": f"Maximum data points allowed is {max_data_points}"})

        if end_time - start_time < scrape_interval_seconds:
            raise exceptions.ValidationError({"msg": f"Time range must be at least {scrape_interval_seconds} seconds"})

        requester = PrometheusRequester()

        node_cpu_seconds_total_data = requester.get_node_cpu_seconds_total_range(start_time, end_time, step_size, instance, cluster.monitoring_id, job)
        cpu_seconds_total_list = alaz_process_node_cpu_seconds_total_data(node_cpu_seconds_total_data)
        earliest_valid_data = None
        latest_valid_data = None

        for _, cpu_seconds_total in cpu_seconds_total_list:
            if earliest_valid_data is None:
                earliest_valid_data = float(cpu_seconds_total)
            latest_valid_data = float(cpu_seconds_total)

        if settings.VERBOSE_LOG:
            print(f'**VERBOSE_LOG: CPU seconds total list: {cpu_seconds_total_list}')
            print(f'**VERBOSE_LOG: Earliest valid data: {earliest_valid_data}')
            print(f'**VERBOSE_LOG: Latest valid data: {latest_valid_data}')

        if earliest_valid_data is None or latest_valid_data is None or abs(earliest_valid_data - latest_valid_data) < 0.0001:
            logger.warn(f'AlazMetricsView: get: No data found for instance: {instance}, monitoring_id: {cluster.monitoring_id}, job: {job}, earliest_valid_data: {earliest_valid_data}, latest_valid_data: {latest_valid_data}')
            # raise exceptions.NotFoundError({"msg": "No data found"})
            return Response(status=status.HTTP_204_NO_CONTENT)

        all_data = requester.get_all_metrics(instance, interval, start_time, end_time, step_size, cluster.monitoring_id, job)
        
        cluster_real_owner = find_team_owner_if_exists(cluster.user)

        result = {'cluster': ClusterSerializer(cluster).data, 'instance': instance}

        if not cluster.is_alive:
            valid_until = cluster.last_heartbeat
        else:
            valid_until = None

        # print(f'Valid until: {valid_until}. Cluster is alive: {cluster.is_alive}')

        metrics_data = alaz_process_prometheus_data(all_data, valid_until=valid_until)
        for key, val in metrics_data.items():
            result[key] = val

        prometheus_requester = PrometheusRequester()
        gpu_uids = prometheus_requester.get_gpu_uids(instance, cluster.monitoring_id, job)
        now = datetime.now(UTC)

        if gpu_uids == []:
            result['gpu_exists'] = False
        else:
            result['gpu_exists'] = True
            result['gpu_section'] = {}
            result['gpu_section']['gpu_uids'] = gpu_uids

            gpu_metrics, globals = prometheus_requester.get_gpu_metrics(instance, cluster.monitoring_id, job, start_time, end_time, step_size, interval, gpu_uids)

            if valid_until:
                gpu_metrics, globals = mute_gpu_metrics_after(gpu_metrics, globals, valid_until)

            result['gpu_section']['metrics'] = gpu_metrics

            for key in globals:
                if cluster_real_owner.passive_from and cluster_real_owner.passive_from < now:
                    globals[key] = '-'
                result['gpu_section'][key] = globals[key]

        if instance not in cluster.last_data_ts['active'] and instance not in cluster.instances:
            instance = 'default_instance'

        if instance in cluster.last_data_ts['active'] and 'metrics' in cluster.last_data_ts['active'][instance]:
            last_data_timestamp_ms = cluster.last_data_ts['active'][instance]['metrics']
        elif instance in cluster.instances:
            last_data_timestamp_ms = cluster.instances[instance]
        else:
            raise exceptions.NotFoundError({"msg": "Last data timestamp could not be found"})
        result['last_data_timestamp_ms'] = last_data_timestamp_ms

        return Response(result, status=status.HTTP_200_OK)


class MetricsScrapeView(GenericAPIView):

    def get_throttles(self):
        if self.request.method.lower() == 'get':
            return [throttling.ConcurrencyThrottleGetApiKey(), ]
        else:
            return [throttling.ConcurrencyAlazScrapePostThrottle(), ]
        
    def get_permissions(self):
        if self.request.method.lower() == 'get':
            return [BasicAuth(), ]
        else:
            return []

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def get(self, request):
        data = redis_client.hgetall(settings.REDIS_METRICS_KEY)
        if not data:
            return Response({"msg": "No data found"}, status=status.HTTP_204_NO_CONTENT)
        summed_data = []
        for val in data.values():
            summed_data.append(val)
        data = '\n'.join(summed_data)

        return HttpResponse(data, content_type="text/plain")

    # @silk_wrapper(name='Metrics-Scrape-Post')
    def post(self, request):
        monitoring_id = request.query_params.get('monitoring_id', None)
        if not monitoring_id:
            raise exceptions.ValidationError({"msg": "Monitoring id is required"})
        try:
            uuid.UUID(monitoring_id)
        except:
            raise exceptions.ValidationError({"msg": "Monitoring id is not valid"})
        # Use the master db to prevent replication lag
        cluster_qs = Cluster.objects.using('default').filter(monitoring_id=monitoring_id)
        if not cluster_qs.exists():
            raise exceptions.ValidationError({"msg": f"Cluster for the monitoring id is not found: {monitoring_id}"})
        cluster = cluster_qs.first()
        job_name = 'alaz'

        data = request.body.decode('utf-8')
        instance = request.query_params.get('instance', None)
        if not instance:
            raise exceptions.ValidationError({"msg": "Instance is required"})

        if settings.ANTEON_ENV != 'onprem':
            if not find_team_owner_if_exists(cluster.user).active:
                # Set instance as passive
                active_instances = list(cluster.last_data_ts['active'].keys())
                if instance in active_instances:
                    data = cluster.last_data_ts['active'][instance]
                    cluster.last_data_ts['passive'][instance] = data
                    del cluster.last_data_ts['active'][instance]
                    save_cluster(cluster)
                    
                return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)


        instance_count_ok = check_user_instance_count(cluster, instance, type='metrics')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)
        
        processed_data = []
        help_line = ""
        invalid_metric_block = False
        default_setting = get_default_setting()

        metrics_count = 0
        for line in data.split('\n'):
            line = line.strip()

            if line.startswith("# HELP"):
                # Store the HELP line to be added later if the TYPE line is valid
                help_line = line
                continue

            if line.startswith("# TYPE"):
                parts = line.split(" ")
                if len(parts) < 4:
                    # Invalid TYPE line, so we ignore this block
                    invalid_metric_block = True
                    help_line = ""  # Discard the stored HELP line
                    continue
                else:
                    # Valid TYPE line, append the stored HELP line and the current TYPE line
                    invalid_metric_block = False
                    if help_line:
                        processed_data.append(help_line)
                        help_line = ""
                    processed_data.append(line)
                    continue

            if invalid_metric_block:
                # Ignore lines belonging to an invalid metric block
                continue

            # Process and append other lines
            last_brace_close = line.rfind("}")
            if last_brace_close != -1:
                new_line = line[:last_brace_close] + "}" + line[last_brace_close:]
                new_line = new_line.replace(
                    "{", '{{exported_job="{job_name}",exported_instance="{instance}",monitoring_id="{monitoring_id}",', 1)
                new_line = new_line.format(instance=instance, monitoring_id=monitoring_id, job_name=job_name)
                processed_data.append(new_line)
            elif len(line) > 0 and not line.startswith("#"):
                words = line.split(" ")
                if metric_used(words[0], default_setting):
                    metrics_count += 1
                words[0] += '{{exported_job="{job_name}",exported_instance="{instance}",monitoring_id="{monitoring_id}"}}'.format(
                    instance=instance, monitoring_id=monitoring_id, job_name=job_name)
                new_line = " ".join(words)
                processed_data.append(new_line)
            else:
                processed_data.append(line)

        processed_data = '\n'.join(processed_data)

        redis_client.hset(settings.REDIS_METRICS_KEY, f'{monitoring_id}:{instance}', processed_data)

        cluster = alaz_set_instance_as_active(cluster, instance, 'metrics')
        now = int(datetime.now(UTC).timestamp() * 1000)
        cluster.instances[instance] = now
        save_cluster(cluster)

        if settings.ANTEON_ENV != 'onprem':
            user_or_owner = find_team_owner_if_exists(cluster.user)
            MetricCount.objects.create(user=user_or_owner, cluster=cluster, count=metrics_count)

        return Response({"msg": "Data pushed"}, status=status.HTTP_200_OK)


class MetricsInstancesView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def get(self, request):
        cluster, start_time, end_time = validate_metrics_get_method(request)

        instances = get_instances_of_cluster(cluster, start_time)

        settings_serializer = LatestAlazVersionSerializer(Setting.objects.get(name='default'))
        response = {'cluster': ClusterSerializer(cluster).data, "settings" : settings_serializer.data, 'instances': []}

        active_instances = list(cluster.last_data_ts['active'].keys())
        for instance in instances:
            if instance in active_instances:
                response['instances'].append({'instance': instance, 'active': True})
            else:
                response['instances'].append({'instance': instance, 'active': False})
            
        # 
        return Response(response, status=status.HTTP_200_OK)


class PodEventsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyAlazDefaultThrottleApiKey]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    @log_endpoint
    def post(self, request):
        cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='events')
        data = request.data
        # verbose_log(f'PodEventsView: post: Data: {data}')

        if settings.ANTEON_ENV != 'onprem':
            if not find_team_owner_if_exists(cluster.user).active:
                return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Alaz pod events are already processed"}, status=status.HTTP_200_OK)

        response = {"msg": "Alaz pod events are processed", "errors": []}
        events = data['events']

        instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)

        last_valid_events = {}
        deleted_pod_uids = []
        uids = set()

        event_num = 1
        for data in events:
            try:
                if "event_type" not in data or "name" not in data or "namespace" not in data or "ip" not in data or "owner_type" not in data or "owner_name" not in data or "owner_id" not in data or 'uid' not in data:
                    raise exceptions.ValidationError({'msg': "Invalid pod event"})

                pod_id = data['uid']
                data['cluster'] = cluster.uid
                owner_type = data['owner_type']
                owner_id = data['owner_id']
                data['replicaset_owner'] = None
                data['daemonset_owner'] = None
                data['statefulset_owner'] = None

                if owner_type == "ReplicaSet":
                    data['replicaset_owner'] = owner_id
                elif owner_type == 'DaemonSet':
                    data['daemonset_owner'] = owner_id
                elif owner_type == 'StatefulSet':
                    data['statefulset_owner'] = owner_id
                elif owner_type == 'Job' or owner_type == '':
                    pass
                else:
                    raise exceptions.ValidationError({'msg': "Invalid owner type for pod"})

                if type(data['uid']) != str or data['uid'] == "" or type(data['name']) != str or data['name'] == "" or type(data['namespace']) != str or data['namespace'] == "" or type(data['ip']) != str or data['ip'] == "":
                    raise exceptions.ValidationError({'msg': "Invalid pod values"})

                if data['event_type'] == "ADD" or data['event_type'] == "UPDATE":
                    if pod_id not in last_valid_events:
                        last_valid_events[pod_id] = {'payload': data, 'deleted': False, 'processed': False}
                    else:
                        last_valid_events[pod_id]['payload'] = data
                elif data['event_type'] == 'DELETE':
                    deleted_pod_uids.append(pod_id)
                    if pod_id not in last_valid_events:
                        last_valid_events[pod_id] = {'payload': None, 'deleted': True, 'processed': False}
                    else:
                        last_valid_events[pod_id]['deleted'] = True

                else:
                    raise exceptions.ValidationError({'msg': "Invalid event type"})

                uids.add(pod_id)
            except Exception as exc:
                errors = str(exc.args)
                response['errors'].append({'event_num': event_num, 'event': data, 'errors': errors})

            event_num += 1
        
        bulk_update = []
        now = datetime.now(UTC)
        try:
            if settings.ALAZ_SAVE_CONTAINER_EVENTS:
                Container.objects.using('default').filter(pod__in=deleted_pod_uids).delete()
            pods = Pod.objects.filter(uid__in=uids)

            # Process UPDATE and DELETEs
            update_count = 0
            for pod in pods:
                if pod.deleted:
                    last_valid_events[str(pod.uid)]['processed'] = True
                    continue
                if last_valid_events[str(pod.uid)]['payload'] is None:
                    pod.deleted = True
                    pod.date_updated = now
                    update_count += 1
                    bulk_update.append(pod)
                    last_valid_events[str(pod.uid)]['processed'] = True
                    continue

                pod.name = last_valid_events[str(pod.uid)]['payload']['name']
                pod.namespace = last_valid_events[str(pod.uid)]['payload']['namespace']
                pod.cluster = last_valid_events[str(pod.uid)]['payload']['cluster']
                pod.ip = last_valid_events[str(pod.uid)]['payload']['ip']
                pod.replicaset_owner = last_valid_events[str(pod.uid)]['payload']['replicaset_owner']
                pod.daemonset_owner = last_valid_events[str(pod.uid)]['payload']['daemonset_owner']
                pod.statefulset_owner = last_valid_events[str(pod.uid)]['payload']['statefulset_owner']
                pod.deleted = last_valid_events[str(pod.uid)]['deleted']
                pod.date_updated = now
                update_count += 1
                bulk_update.append(pod)
                
                last_valid_events[str(pod.uid)]['processed'] = True

            Pod.objects.bulk_update(bulk_update, ['cluster', 'name', 'namespace', 'ip', 'replicaset_owner', 'daemonset_owner', 'statefulset_owner', 'deleted', 'date_updated'], batch_size=settings.EVENTS_BULK_BATCH_SIZE)

            # Process ADDs
            to_create = []
            for event in last_valid_events.values():
                if not event['processed'] and event['payload']:
                    payload = {'uid': event['payload']['uid'], 'cluster': cluster.uid, 'name': event['payload']['name'], 'namespace': event['payload']['namespace'], 'ip': event['payload']['ip'], 'replicaset_owner': event['payload']['replicaset_owner'], 'daemonset_owner': event['payload']['daemonset_owner'], 'statefulset_owner': event['payload']['statefulset_owner'], 'deleted': event['deleted']}
                    new_pod = Pod(**payload)
                    to_create.append(new_pod)

            if settings.VERBOSE_LOG:
                print(f'**VERBOSE_LOG: Pods to Create: {len(to_create)} and Pods to Update: {update_count} - Cluster: {cluster.uid}')

            try:
                Pod.objects.bulk_create(to_create, batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            except IntegrityError as exc:
                # Process one by one
                for pod in to_create:
                    try:
                        pod.save()
                    except IntegrityError as exc:
                        logger.error("PodEventsView: IntegrityError exception: ", exc)
            
            redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
            redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

        except IntegrityError as exc:
            logger.error("AlazPodEventsView: IntegrityError exception: ", exc)

        if settings.VERBOSE_LOG and 'errors' in response and len(response['errors']) > 0:
            logger.warn(response)

        return Response(response, status=status.HTTP_200_OK)


class DeploymentEventsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyAlazDefaultThrottleApiKey]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    @log_endpoint
    def post(self, request):
        cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='events')
        data = request.data

        if settings.ANTEON_ENV != 'onprem':
            if not find_team_owner_if_exists(cluster.user).active:
                return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Alaz deployment events are already processed"}, status=status.HTTP_200_OK)

        response = {"msg": "Alaz deployment events are processed", "errors": []}
        events = data['events']
        last_valid_events = {}
        deleted_deployment_uids = []
        uids = set()

        instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)

        event_num = 1
        for data in events:
            try:
                if "event_type" not in data or "name" not in data or "namespace" not in data or "replicas" not in data or 'uid' not in data:
                    raise exceptions.ValidationError({'msg': "Invalid deployment event"})
                
                if type(data['name']) != str or data['name'] == '' or type(data['namespace']) != str or data['namespace'] == '' or type(data['uid']) != str or data['uid'] == '' or type(data['replicas']) != int or data['replicas'] < 0:
                    raise exceptions.ValidationError({'msg': "Invalid values for fields"})

                deployment_id = data['uid']

                if data['event_type'] == "ADD" or data['event_type'] == "UPDATE":
                    data['cluster'] = cluster.uid

                    if deployment_id not in last_valid_events:
                        last_valid_events[deployment_id] = {'payload': data, 'deleted': False, 'processed': False}
                    else:
                        last_valid_events[deployment_id]['payload'] = data

                elif data['event_type'] == 'DELETE':
                    deleted_deployment_uids.append(deployment_id)
                    if deployment_id not in last_valid_events:
                        last_valid_events[deployment_id] = {'payload': None, 'deleted': True, 'processed': False}
                    else:
                        last_valid_events[deployment_id]['deleted'] = True

                else:
                    raise exceptions.ValidationError({'msg': "Invalid event type"})

                uids.add(deployment_id)
            except Exception as exc:
                errors = str(exc.args)
                response['errors'].append({'event_num': event_num, 'event': data, 'errors': errors})

            event_num += 1

        bulk_update = []
        now = datetime.now(UTC)
        try:
            deployments = Deployment.objects.using('default').filter(uid__in=uids)
            verbose_log(f'Number of Deployments to Update: {len(deployments)} - Cluster: {cluster}')
            # Process UPDATE and DELETEs
            updated_count = 0
            for deployment in deployments:
                if deployment.deleted:
                    last_valid_events[str(deployment.uid)]['processed'] = True
                    continue
                if last_valid_events[str(deployment.uid)]['payload'] is None:
                    deployment.deleted = True
                    deployment.date_updated = now
                    updated_count += 1
                    bulk_update.append(deployment)
                    last_valid_events[str(deployment.uid)]['processed'] = True
                    continue
                deployment.name = last_valid_events[str(deployment.uid)]['payload']['name']
                deployment.namespace = last_valid_events[str(deployment.uid)]['payload']['namespace']
                deployment.cluster = last_valid_events[str(deployment.uid)]['payload']['cluster']
                deployment.replicas = last_valid_events[str(deployment.uid)]['payload']['replicas']
                deployment.deleted = last_valid_events[str(deployment.uid)]['deleted']
                deployment.date_updated = now
                updated_count += 1
                bulk_update.append(deployment)

                last_valid_events[str(deployment.uid)]['processed'] = True

            Deployment.objects.bulk_update(bulk_update, ['name', 'namespace', 'cluster', 'replicas', 'deleted', 'date_updated'], batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            # Process ADDs
            to_create = []
            for event in last_valid_events.values():
                if not event['processed'] and event['payload']:
                    payload = {'uid': event['payload']['uid'], 'cluster': cluster.uid, 'name': event['payload']['name'], 'namespace': event['payload']['namespace'], 'replicas': event['payload']['replicas'], 'deleted': event['deleted']}
                    new_deployment = Deployment(**payload)
                    to_create.append(new_deployment)
            
            if settings.VERBOSE_LOG:
                print(f'**VERBOSE_LOG: Deployments to Create: {len(to_create)} and Deployments to Update: {updated_count} - Cluster: {cluster}')

            try:
                Deployment.objects.bulk_create(to_create, batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            except IntegrityError as exc:
                # Process one by one
                for deployment in to_create:
                    try:
                        deployment.save()
                    except IntegrityError as exc:
                        logger.error("DeploymentEventsView: IntegrityError exception: ", exc)

            redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
            redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

        except IntegrityError as exc:
            logger.error("AlazDeploymentEventsView: IntegrityError exception: ", exc)

        if settings.VERBOSE_LOG and 'errors' in response and len(response['errors']) > 0:
            logger.warn(response)

        return Response(response, status=status.HTTP_200_OK)


class ReplicaSetEventsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyAlazDefaultThrottleApiKey]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    # @silk_wrapper(name='ReplicaSet-Events-Post')
    @log_endpoint
    def post(self, request):
        cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='events')
        data = request.data
        
        if settings.ANTEON_ENV != 'onprem':
            if not find_team_owner_if_exists(cluster.user).active:
                return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Alaz replicaset events are already processed"}, status=status.HTTP_200_OK)

        response = {"msg": "Alaz replicaset events are processed", "errors": []}
        events = data['events']

        instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)

        last_valid_events = {}
        deleted_replicaset_uids = []
        uids = set()
        event_num = 1
        for data in events:
            try:
                if "event_type" not in data or "name" not in data or "namespace" not in data or "owner_type" not in data or "owner_name" not in data or "owner_id" not in data or "replicas" not in data or 'uid' not in data:
                    raise exceptions.ValidationError({'msg': "Invalid replicaset event"})
                
                if type(data['name']) != str or data['name'] == '' or type(data['namespace']) != str or data['namespace'] == '' or type(data['uid']) != str or data['uid'] == '' or type(data['replicas']) != int or data['replicas'] < 0:
                    raise exceptions.ValidationError({'msg': "Invalid values for fields"})

                replicaset_id = data['uid']

                if data['event_type'] == "ADD" or data['event_type'] == "UPDATE":
                    data['cluster'] = cluster.uid
                    owner_type = data['owner_type']
                    owner_id = data['owner_id']
                    data['owner'] = None

                    if owner_type == "Deployment":
                        data['owner'] = owner_id
                    else:
                        raise exceptions.ValidationError({'msg': "Invalid owner type for replicaset"})

                    if replicaset_id not in last_valid_events:
                        last_valid_events[replicaset_id] = {'payload': data, 'deleted': False, 'processed': False}
                    else:
                        last_valid_events[replicaset_id]['payload'] = data
                
                elif data['event_type'] == 'DELETE':
                    deleted_replicaset_uids.append(replicaset_id)
                    if replicaset_id not in last_valid_events:
                        last_valid_events[replicaset_id] = {'payload': None, 'deleted': True, 'processed': False}
                    else:
                        last_valid_events[replicaset_id]['deleted'] = True
                
                else:
                    raise exceptions.ValidationError({'msg': "Invalid event type"})
                
                uids.add(replicaset_id)

            except Exception as exc:
                errors = str(exc.args)
                response['errors'].append({'event_num': event_num, 'event': data, 'errors': errors})

            event_num += 1

        bulk_update = []
        now = datetime.now(UTC)
        try:
            replicasets = ReplicaSet.objects.filter(uid__in=uids)

            # Process UPDATE and DELETEs
            updated_count = 0
            for replicaset in replicasets:
                if replicaset.deleted:
                    last_valid_events[str(replicaset.uid)]['processed'] = True
                    continue
                if last_valid_events[str(replicaset.uid)]['payload'] is None:
                    replicaset.deleted = True
                    replicaset.date_updated = now
                    updated_count += 1
                    bulk_update.append(replicaset)
                    last_valid_events[str(replicaset.uid)]['processed'] = True
                    continue
                replicaset.name = last_valid_events[str(replicaset.uid)]['payload']['name']
                replicaset.namespace = last_valid_events[str(replicaset.uid)]['payload']['namespace']
                replicaset.cluster = last_valid_events[str(replicaset.uid)]['payload']['cluster']
                replicaset.owner = last_valid_events[str(replicaset.uid)]['payload']['owner']
                replicaset.replicas = last_valid_events[str(replicaset.uid)]['payload']['replicas']
                replicaset.deleted = last_valid_events[str(replicaset.uid)]['deleted']
                replicaset.date_updated = now
                updated_count += 1
                bulk_update.append(replicaset)
                last_valid_events[str(replicaset.uid)]['processed'] = True

            ReplicaSet.objects.bulk_update(bulk_update, ['name', 'namespace', 'cluster', 'owner', 'replicas', 'deleted', 'date_updated'], batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            # Process ADDs
            to_create = []
            for event in last_valid_events.values():
                if not event['processed'] and event['payload']:
                    payload = {'uid': event['payload']['uid'], 'cluster': cluster.uid, 'name': event['payload']['name'], 'namespace': event['payload']['namespace'], 'owner': event['payload']['owner'], 'replicas': event['payload']['replicas'], 'deleted': event['deleted']}
                    new_replicaset = ReplicaSet(**payload)
                    to_create.append(new_replicaset)
    
            if settings.VERBOSE_LOG:
                print(f'**VERBOSE_LOG: ReplicaSets to Create: {len(to_create)} and ReplicaSets to Update: {updated_count} - Cluster: {cluster.uid}')
                
            try:
                ReplicaSet.objects.bulk_create(to_create, batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            except IntegrityError as exc:
                # Process one by one
                for replicaset in to_create:
                    try:
                        replicaset.save()
                    except IntegrityError as exc:
                        logger.error("ReplicaSetEventsView: IntegrityError exception: ", exc)

            redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
            redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)
        
        except IntegrityError as exc:
            logger.error("AlazReplicaSetEventsView: IntegrityError exception: ", exc)

        if settings.VERBOSE_LOG and 'errors' in response and len(response['errors']) > 0:
            logger.warn(response)

        return Response(response, status=status.HTTP_200_OK)


class ServiceEventsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyAlazDefaultThrottleApiKey]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def post(self, request):
        cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='events')
        data = request.data
        
        if settings.ANTEON_ENV != 'onprem':
            if not find_team_owner_if_exists(cluster.user).active:
                return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Alaz service events are already processed"}, status=status.HTTP_200_OK)

        response = {"msg": "Alaz service events are processed", "errors": []}
        events = data['events']
        last_valid_events = {}
        deleted_service_uids = []
        uids = set()

        instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)

        event_num = 1
        for data in events:
            try:
                if "event_type" not in data or "name" not in data or "namespace" not in data or "type" not in data or "cluster_ips" not in data or "ports" not in data or 'uid' not in data:
                    raise exceptions.ValidationError({'msg': "Invalid service event"})
                
                data['cluster'] = cluster.uid

                if type(data['name']) != str or data['name'] == '' or type(data['namespace']) != str or data['namespace'] == '' or type(data['uid']) != str or data['uid'] == '' or type(data['type']) != str or data['type'] == '' or type(data['cluster_ips']) != list or type(data['ports']) != list:
                    raise exceptions.ValidationError({'msg': "Invalid values for fields"})

                for cluster_ip in data['cluster_ips']:
                    if type(cluster_ip) != str or cluster_ip == '':
                        raise exceptions.ValidationError({'msg': "Invalid cluster ip"})
                    
                for port in data['ports']:
                    if 'src' not in port or 'dest' not in port or 'protocol' not in port or port['src'] < 0 or port['dest'] < 0 or type(port['protocol']) != str or port['protocol'] == '':
                        raise exceptions.ValidationError({'msg': "Invalid port"})
                

                service_id = data['uid']

                if data['event_type'] == "ADD" or data['event_type'] == "UPDATE":
                    if service_id not in last_valid_events:
                        last_valid_events[service_id] = {'payload': data, 'deleted': False, 'processed': False}
                    else:
                        last_valid_events[service_id]['payload'] = data

                elif data['event_type'] == 'DELETE':
                    deleted_service_uids.append(service_id)
                    if service_id not in last_valid_events:
                        last_valid_events[service_id] = {'payload': None, 'deleted': True, 'processed': False}
                    else:
                        last_valid_events[service_id]['deleted'] = True
                
                else:
                    raise exceptions.ValidationError({'msg': "Invalid event type"})
                
                uids.add(service_id)
                
            except Exception as exc:
                errors = str(exc.args)
                response['errors'].append({'event_num': event_num, 'event': data, 'errors': errors})

            event_num += 1

        bulk_update = []
        now = datetime.now(UTC)
        try:
            services = Service.objects.using('default').filter(uid__in=uids)
            # Process UPDATE and DELETEs
            updated_count = 0
            for service in services:
                if service.deleted:
                    last_valid_events[str(service.uid)]['processed'] = True
                    continue
                if last_valid_events[str(service.uid)]['payload'] is None:
                    service.deleted = True
                    service.date_updated = now
                    updated_count += 1
                    bulk_update.append(service)
                    last_valid_events[str(service.uid)]['processed'] = True
                    continue
                service.name = last_valid_events[str(service.uid)]['payload']['name']
                service.namespace = last_valid_events[str(service.uid)]['payload']['namespace']
                service.cluster = last_valid_events[str(service.uid)]['payload']['cluster']
                service.type = last_valid_events[str(service.uid)]['payload']['type']
                service.cluster_ips = last_valid_events[str(service.uid)]['payload']['cluster_ips']
                service.ports = last_valid_events[str(service.uid)]['payload']['ports']
                service.deleted = last_valid_events[str(service.uid)]['deleted']
                service.date_updated = now
                updated_count += 1
                bulk_update.append(service)
                last_valid_events[str(service.uid)]['processed'] = True

            Service.objects.bulk_update(bulk_update, ['name', 'namespace', 'cluster', 'type', 'cluster_ips', 'ports', 'deleted', 'date_updated'], batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            # Process ADDs
            to_create = []
            for event in last_valid_events.values():
                if not event['processed'] and event['payload']:
                    payload = {'uid': event['payload']['uid'], 'cluster': cluster.uid, 'name': event['payload']['name'], 'namespace': event['payload']['namespace'], 'type': event['payload']['type'], 'cluster_ips': event['payload']['cluster_ips'], 'ports': event['payload']['ports'], 'deleted': event['deleted']}
                    new_service = Service(**payload)
                    to_create.append(new_service)

            if settings.VERBOSE_LOG:
                print(f'**VERBOSE_LOG: Services to Create: {len(to_create)} and Services to Update: {updated_count} - Cluster: {cluster}')
                
            try:
                Service.objects.bulk_create(to_create, batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            except IntegrityError as exc:
                # Process one by one
                for service in to_create:
                    try:
                        service.save()
                    except IntegrityError as exc:
                        logger.error("ServiceEventsView: IntegrityError exception: ", exc)

            redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
            redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

        except IntegrityError as exc:
            logger.error("AlazServiceEventsView: IntegrityError exception: ", exc)

        if settings.VERBOSE_LOG and 'errors' in response and len(response['errors']) > 0:
            logger.warn(response)

        return Response(response, status=status.HTTP_200_OK)


class ContainerEventsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyAlazDefaultThrottleApiKey]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    @log_endpoint
    def post(self, request):
        # return Response(status=status.HTTP_200_OK)
        if not settings.ALAZ_SAVE_CONTAINER_EVENTS:
            return Response({"msg": "Alaz container events are not processed (ALAZ_SAVE_CONTAINER_EVENTS=False)"}, status=status.HTTP_200_OK)
        cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='events')
        data = request.data

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Alaz container events are already processed"}, status=status.HTTP_200_OK)

        response = {'msg': 'Alaz container events are processed', 'errors': []}
        events = data['events']

        last_valid_events = {}
        names = set()
        namespaces = set()
        owners = set()

        instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)

        event_num = 1
        for data in events:
            try:
                if "event_type" not in data or "name" not in data or "namespace" not in data or "image" not in data or "pod" not in data or "ports" not in data:
                    raise exceptions.ValidationError({'msg': "Invalid container event"})

                data['cluster'] = cluster.uid
                uid = str(uuid.uuid4())
                data['uid'] = uid

                if type(data['uid']) != str or data['uid'] == "" or type(data['name']) != str or data['name'] == "" or type(data['namespace']) != str or data['namespace'] == "" or type(data['image']) != str or data['image'] == "" or type(data['pod']) != str or data['pod'] == "" or type(data['ports']) != list:
                    raise exceptions.ValidationError({'msg': "Invalid container values"})
                
                for port in data['ports']:
                    if 'port' not in port or 'protocol' not in port or type(port['port']) != int or port['port'] <= 0 or port['port'] > 65535 or type(port['protocol']) != str or port['protocol'] == '':
                        raise exceptions.ValidationError({'msg': "Invalid port values"})

                if data['event_type'] == "ADD" or data['event_type'] == "UPDATE":
                    found = False
                    for key, valid_event in last_valid_events.items():
                        if valid_event['payload']['name'] == data['name'] and valid_event['payload']['namespace'] == data['namespace'] and valid_event['payload']['pod'] == data['pod']:
                            found = True
                            break
                    if not found:
                        last_valid_events[(data['name']), data['namespace'], data['pod']] = {'payload': data, 'deleted': False, 'processed': False}
                    else:
                        last_valid_events[(data['name'], data['namespace'], data['pod'])]['payload'] = data

                else:
                    raise exceptions.ValidationError({'msg': "Invalid event type"})
                
                names.add(data['name'])
                namespaces.add(data['namespace'])
                owners.add(data['pod'])

            except Exception as exc:
                errors = str(exc.args)
                response['errors'].append({'event_num': event_num, 'event': data, 'errors': errors})

            event_num += 1

        bulk_update = []
        try:
            containers = Container.objects.using('default').filter(cluster=cluster.uid, name__in=names, namespace__in=namespaces, pod__in=owners)

            # logger.info(f'Number of candidate update containers: {len(containers)} - Cluster: {cluster.uid}')
            # Process UPDATEs
            cached_containers = {}
            for container in containers:
                cached_containers[(container.name, container.namespace, str(container.pod))] = container
            # logger.info(f'Cached containers: {cached_containers}')
            for key, valid_event in last_valid_events.items():
                updated = False
                container = cached_containers.get(key, None)
                # logger.info(f'Cached container {"exists" if container else "does not exist"}: {key}')
                if not container:
                    continue
                if container.image != valid_event['payload']['image']:
                    updated = True
                    container.image = valid_event['payload']['image']
                if container.ports != valid_event['payload']['ports']:
                    updated = True
                    container.ports = valid_event['payload']['ports']

                if updated:
                    bulk_update.append(container)

                last_valid_events[key]['processed'] = True

            # logger.info(f'Number of containers to update: {len(bulk_update)}')
            Container.objects.bulk_update(bulk_update, ['image', 'ports'], batch_size=settings.EVENTS_BULK_BATCH_SIZE)

            # Process ADDs
            to_create = []
            for event in last_valid_events.values():
                if not event['processed'] and event['payload']:
                    payload = {'uid': event['payload']['uid'], 'cluster': cluster.uid, 'name': event['payload']['name'], 'namespace': event['payload']['namespace'], 'image': event['payload']['image'], 'pod': event['payload']['pod'], 'ports': event['payload']['ports']}
                    new_container = Container(**payload)
                    to_create.append(new_container)

            # logger.info(f'Number of containers to create: {len(to_create)}')
            try:
                Container.objects.bulk_create(to_create, batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            except IntegrityError as exc:
                # Process one by one
                for container in to_create:
                    try:
                        container.save()
                    except IntegrityError as exc:
                        logger.error(f'ContainerEventsView: IntegrityError exception: {exc}')
                            

            redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
            redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

        except Exception as exc:
            logger.error(f"AlazContainerEventsView: IntegrityError exception: {exc}")

        if settings.VERBOSE_LOG and 'errors' in response and len(response['errors']) > 0:
            logger.warn(response)



        return Response(response, status=status.HTTP_200_OK)


class EndpointEventsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyAlazDefaultThrottleApiKey]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    @log_endpoint
    def post(self, request):
        cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='events')
        data = request.data
        
        if settings.ANTEON_ENV != 'onprem':
            if not find_team_owner_if_exists(cluster.user).active:
                return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Alaz endpoint events are already processed"}, status=status.HTTP_200_OK)

        response = {'msg': 'Alaz endpoint events are processed', 'errors': []}
        events = data['events']
        last_valid_events = {}
        deleted_endpoints_uid = []
        uids = set()
        names = set()
        namespaces = set()

        instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)

        event_num = 1
        for data in events:
            try:
                if "event_type" not in data or "name" not in data or "namespace" not in data or "addresses" not in data or 'uid' not in data:
                    raise exceptions.ValidationError({'msg': "Invalid endpoint event"})
                
                endpoint_uid = data['uid']
                data['cluster'] = cluster.uid

                if type(data['uid']) != str or data['uid'] == "" or type(data['name']) != str or data['name'] == "" or type(data['namespace']) != str or data['namespace'] == "" or type(data['addresses']) != list:
                    raise exceptions.ValidationError({'msg': "Invalid endpoint values"})
                
                for address in data['addresses']:
                    if type(address) != dict or 'ips' not in address or type(address['ips']) != list or 'ports' not in address or type(address['ports']) != list:
                        raise exceptions.ValidationError({'msg': "Invalid address values"})
                    ips = address['ips']
                    ports = address['ports']
                    for ip in ips:
                        if type(ip) != dict or 'type' not in ip or type(ip['type']) != str or 'ip' not in ip or type(ip['ip']) != str:
                            raise exceptions.ValidationError({'msg': "Invalid ip values"})
                        if ip['type'] == 'Pod' and ('id' not in ip or type(ip['id']) != str or 'name' not in ip or type(ip['name']) != str or 'namespace' not in ip or type(ip['namespace']) != str):
                            raise exceptions.ValidationError({'msg': "Invalid ip values"})
                    for port in ports:
                        if type(port) != dict or 'port' not in port or type(port['port']) != int or port['port'] <= 0 or port['port'] > 65535 or 'protocol' not in port or type(port['protocol']) != str or port['protocol'] == '':
                            raise exceptions.ValidationError({'msg': "Invalid port values"})

                if data['event_type'] == "ADD" or data['event_type'] == "UPDATE":
                    if endpoint_uid not in last_valid_events:
                        last_valid_events[endpoint_uid] = {'payload': data, 'deleted': False, 'processed': False}
                    else:
                        last_valid_events[endpoint_uid]['payload'] = data

                elif data['event_type'] == 'DELETE':
                    deleted_endpoints_uid.append(endpoint_uid)
                    if endpoint_uid not in last_valid_events:
                        last_valid_events[endpoint_uid] = {'payload': None, 'deleted': True, 'processed': False}
                    else:
                        last_valid_events[endpoint_uid]['deleted'] = True

                else:
                    raise exceptions.ValidationError({'msg': "Invalid event type"})
                
                uids.add(endpoint_uid)
                names.add(data['name'])
                namespaces.add(data['namespace'])

            except Exception as exc:
                errors = str(exc.args)
                response['errors'].append({'event_num': event_num, 'event': data, 'errors': errors})

            event_num += 1

        bulk_update = []
        now = datetime.now(UTC)
        try:
            endpoints = Endpoint.objects.using('default').filter(uid__in=uids)
            # Process UPDATE and DELETEs
            updated_count = 0
            for endpoint in endpoints:
                if endpoint.deleted:
                    last_valid_events[str(endpoint.uid)]['processed'] = True
                    continue
                if last_valid_events[str(endpoint.uid)]['payload'] is None:
                    endpoint.deleted = True
                    endpoint.date_updated = now
                    updated_count += 1
                    bulk_update.append(endpoint)
                    last_valid_events[str(endpoint.uid)]['processed'] = True
                    continue
                endpoint.name = last_valid_events[str(endpoint.uid)]['payload']['name']
                endpoint.namespace = last_valid_events[str(endpoint.uid)]['payload']['namespace']
                endpoint.cluster = last_valid_events[str(endpoint.uid)]['payload']['cluster']
                endpoint.addresses = last_valid_events[str(endpoint.uid)]['payload']['addresses']
                endpoint.deleted = last_valid_events[str(endpoint.uid)]['deleted']
                endpoint.date_updated = now
                updated_count += 1
                bulk_update.append(endpoint)

                last_valid_events[str(endpoint.uid)]['processed'] = True

            Endpoint.objects.bulk_update(bulk_update, ['name', 'namespace', 'cluster', 'addresses', 'deleted', 'date_updated'], batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            # Process ADDs
            to_create = []
            for event in last_valid_events.values():
                # Check for event['payload'] as non-existing endpoints could also get delete operations
                if not event['processed'] and event['payload']:
                    payload = {'uid': event['payload']['uid'], 'cluster': cluster.uid, 'name': event['payload']['name'], 'namespace': event['payload']['namespace'], 'addresses': event['payload']['addresses'], 'deleted': event['deleted']}
                    new_endpoint = Endpoint(**payload)
                    to_create.append(new_endpoint)

            if settings.VERBOSE_LOG:
                print(f'**VERBOSE_LOG: Endpoints to Create: {len(to_create)} and Endpoints to Update: {updated_count} - Cluster: {cluster}')

            try:
                Endpoint.objects.bulk_create(to_create, batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            except IntegrityError as exc:
                # Process one by one
                for endpoint in to_create:
                    try:
                        endpoint.save()
                    except IntegrityError as exc:
                        logger.error("EndpointEventsView: IntegrityError exception: ", exc)

            redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
            redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

        except IntegrityError as exc:
            logger.error("AlazEndpointEventsView: IntegrityError exception: ", exc)

        if settings.VERBOSE_LOG and 'errors' in response and len(response['errors']) > 0:
            logger.warn(response)

        return Response(response, status=status.HTTP_200_OK)


class DaemonSetEventsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyAlazDefaultThrottleApiKey]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    @log_endpoint
    def post(self, request):
        cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='events')
        data = request.data
        
        if settings.ANTEON_ENV != 'onprem':
            if not find_team_owner_if_exists(cluster.user).active:
                return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Alaz daemonset events are already processed"}, status=status.HTTP_200_OK)

        response = {'msg': 'Alaz daemonset events are processed', 'errors': []}
        events = data['events']
        last_valid_events = {}
        deleted_daemonset_uids = []
        uids = set()

        instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)
        
        event_num = 1
        for data in events:
            try:
                if "event_type" not in data or "name" not in data or "namespace" not in data or 'uid' not in data:
                    raise exceptions.ValidationError({'msg': "Invalid daemonset event"})

                daemonset_uid = data['uid']
                data['cluster'] = cluster.uid

                if type(data['name']) != str or data['name'] == '' or type(data['namespace']) != str or data['namespace'] == '' or type(data['uid']) != str or data['uid'] == '':
                    raise exceptions.ValidationError({'msg': "Invalid values for fields"})

                if data['event_type'] == "ADD" or data['event_type'] == "UPDATE":
                    if daemonset_uid not in last_valid_events:
                        last_valid_events[daemonset_uid] = {'payload': data, 'deleted': False, 'processed': False}
                    else:
                        last_valid_events[daemonset_uid]['payload'] = data

                elif data['event_type'] == 'DELETE':
                    deleted_daemonset_uids.append(daemonset_uid)
                    if daemonset_uid not in last_valid_events:
                        last_valid_events[daemonset_uid] = {'payload': None, 'deleted': True, 'processed': False}
                    else:
                        last_valid_events[daemonset_uid]['deleted'] = True

                else:
                    raise exceptions.ValidationError({'msg': "Invalid event type"})
                
                uids.add(daemonset_uid)

            except Exception as exc:
                errors = str(exc.args)
                response['errors'].append({'event_num': event_num, 'event': data, 'errors': errors})

            event_num += 1

        bulk_update = []
        now = datetime.now(UTC)
        try:
            daemonsets = DaemonSet.objects.using('default').filter(uid__in=uids)
            # Process UPDATE and DELETEs
            update_count = 0
            for daemonset in daemonsets:
                if daemonset.deleted:
                    last_valid_events[str(daemonset.uid)]['processed'] = True
                    continue
                if last_valid_events[str(daemonset.uid)]['payload'] is None:
                    daemonset.deleted = True
                    daemonset.date_updated = now
                    update_count += 1
                    bulk_update.append(daemonset)
                    last_valid_events[str(daemonset.uid)]['processed'] = True
                    continue
                daemonset.name = last_valid_events[str(daemonset.uid)]['payload']['name']
                daemonset.namespace = last_valid_events[str(daemonset.uid)]['payload']['namespace']
                daemonset.cluster = last_valid_events[str(daemonset.uid)]['payload']['cluster']
                daemonset.deleted = last_valid_events[str(daemonset.uid)]['deleted']
                daemonset.date_updated = now
                bulk_update.append(daemonset)
                last_valid_events[str(daemonset.uid)]['processed'] = True

            DaemonSet.objects.bulk_update(bulk_update, ['name', 'namespace', 'cluster', 'deleted', 'date_updated'], batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            # Process ADDs
            to_create = []
            for event in last_valid_events.values():
                if not event['processed'] and event['payload']:
                    payload = {'uid': event['payload']['uid'], 'cluster': cluster.uid, 'name': event['payload']['name'], 'namespace': event['payload']['namespace'], 'deleted': event['deleted']}
                    new_daemonset = DaemonSet(**payload)
                    to_create.append(new_daemonset)

            if settings.VERBOSE_LOG:
                print(f'**VERBOSE_LOG: DaemonSets to Create: {len(to_create)} and DaemonSets to Update: {update_count} - Cluster: {cluster.uid}')
                
            try:
                DaemonSet.objects.bulk_create(to_create, batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            except IntegrityError as exc:
                # Process one by one
                for daemonset in to_create:
                    try:
                        daemonset.save()
                    except IntegrityError as exc:
                        logger.error("DaemonSetEventsView: IntegrityError exception: ", exc)
        
            redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
            redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

        except IntegrityError as exc:
            logger.error("AlazDaemonSetEventsView: IntegrityError exception: ", exc)

        if settings.VERBOSE_LOG and 'errors' in response and len(response['errors']) > 0:
            logger.warn(response)

        return Response(response, status=status.HTTP_200_OK)


class StatefulSetEventsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyAlazDefaultThrottleApiKey]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    @log_endpoint
    def post(self, request):
        cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='events')
        data = request.data
        
        if settings.ANTEON_ENV != 'onprem':
            if not find_team_owner_if_exists(cluster.user).active:
                return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Alaz StatefulSet events are already processed"}, status=status.HTTP_200_OK)

        response = {'msg': 'Alaz StatefulSet events are processed', 'errors': []}
        events = data['events']
        last_valid_events = {}
        deleted_statefulset_uids = []
        uids = set()

        instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)
        
        event_num = 1
        for data in events:
            try:
                if "event_type" not in data or "name" not in data or "namespace" not in data or 'uid' not in data:
                    raise exceptions.ValidationError({'msg': "Invalid StatefulSet event"})

                statefulset_uid = data['uid']
                data['cluster'] = cluster.uid

                if type(data['name']) != str or data['name'] == '' or type(data['namespace']) != str or data['namespace'] == '' or type(data['uid']) != str or data['uid'] == '':
                    raise exceptions.ValidationError({'msg': "Invalid values for fields"})

                if data['event_type'] == "ADD" or data['event_type'] == "UPDATE":
                    if statefulset_uid not in last_valid_events:
                        last_valid_events[statefulset_uid] = {'payload': data, 'deleted': False, 'processed': False}
                    else:
                        last_valid_events[statefulset_uid]['payload'] = data

                elif data['event_type'] == 'DELETE':
                    deleted_statefulset_uids.append(statefulset_uid)
                    if statefulset_uid not in last_valid_events:
                        last_valid_events[statefulset_uid] = {'payload': None, 'deleted': True, 'processed': False}
                    else:
                        last_valid_events[statefulset_uid]['deleted'] = True

                else:
                    raise exceptions.ValidationError({'msg': "Invalid event type"})
                
                uids.add(statefulset_uid)

            except Exception as exc:
                errors = str(exc.args)
                response['errors'].append({'event_num': event_num, 'event': data, 'errors': errors})

            event_num += 1

        bulk_update = []
        now = datetime.now(UTC)
        try:
            statefulsets = StatefulSet.objects.using('default').filter(uid__in=uids)
            # Process UPDATE and DELETEs
            update_count = 0
            for statefulset in statefulsets:
                if statefulset.deleted:
                    last_valid_events[str(statefulset.uid)]['processed'] = True
                    continue
                if last_valid_events[str(statefulset.uid)]['payload'] is None:
                    statefulset.deleted = True
                    statefulset.date_updated = now
                    update_count += 1
                    bulk_update.append(statefulset)
                    last_valid_events[str(statefulset.uid)]['processed'] = True
                    continue
                statefulset.name = last_valid_events[str(statefulset.uid)]['payload']['name']
                statefulset.namespace = last_valid_events[str(statefulset.uid)]['payload']['namespace']
                statefulset.cluster = last_valid_events[str(statefulset.uid)]['payload']['cluster']
                statefulset.deleted = last_valid_events[str(statefulset.uid)]['deleted']
                statefulset.date_updated = now
                bulk_update.append(statefulset)
                last_valid_events[str(statefulset.uid)]['processed'] = True

            StatefulSet.objects.bulk_update(bulk_update, ['name', 'namespace', 'cluster', 'deleted', 'date_updated'], batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            # Process ADDs
            to_create = []
            for event in last_valid_events.values():
                if not event['processed'] and event['payload']:
                    payload = {'uid': event['payload']['uid'], 'cluster': cluster.uid, 'name': event['payload']['name'], 'namespace': event['payload']['namespace'], 'deleted': event['deleted']}
                    new_statefulset = StatefulSet(**payload)
                    to_create.append(new_statefulset)

            if settings.VERBOSE_LOG:
                print(f'**VERBOSE_LOG: StatefulSets to Create: {len(to_create)} and StatefulSets to Update: {update_count} - Cluster: {cluster.uid}')
                
            try:
                StatefulSet.objects.bulk_create(to_create, batch_size=settings.EVENTS_BULK_BATCH_SIZE)
            except IntegrityError as exc:
                # Process one by one
                for statefulset in to_create:
                    try:
                        statefulset.save()
                    except IntegrityError as exc:
                        logger.error("StatefulSetEventsView: IntegrityError exception: ", exc)
        
            redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
            redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

        except IntegrityError as exc:
            logger.error("AlazStatefulSetEventsView: IntegrityError exception: ", exc)

        if settings.VERBOSE_LOG and 'errors' in response and len(response['errors']) > 0:
            logger.warn(response)

        return Response(response, status=status.HTTP_200_OK)

class HealthCheckView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyAlazHealthCheckThrottleApiKey]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def get_src_ip(self, request):
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        logger.info(f'X-Forwarded-For: {x_forwarded_for}')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]  # Just take the first IP
        else:
            ip = request.META.get('REMOTE_ADDR')
        logger.info(f'IP: {ip}')
        return ip

    # @silk_wrapper(name='Healthcheck')
    def put(self, request):
        # self.get_src_ip(request)
        data = request.data
        # verbose_log(f'Healthcheck data: {data}')
        cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='healthcheck')
        
        if settings.ANTEON_ENV != 'onprem':
            if not find_team_owner_if_exists(cluster.user).active:
                verbose_log(f"User is not active: {cluster.user} {find_team_owner_if_exists(cluster.user)}")
                return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)

        alaz_info = data['alaz_info']
        if type(alaz_info) != dict or len(alaz_info) == 0:
            raise exceptions.InvalidRequestError({'msg': "Modes must be a non empty dictionary"})
        monitoring_id = data["metadata"]["monitoring_id"]
        idempotency_key = data["metadata"]["idempotency_key"]

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "Heartbeat already received"}, status=status.HTTP_200_OK)

        user = cluster.user
        owner_or_user = find_team_owner_if_exists(user)
        limit_active_instances_of_user(owner_or_user)
        # Get the new cluster (required if its instances got limited)
        # Connect to the master db to prevent replication lag
        cluster = Cluster.objects.using('default').filter(monitoring_id=monitoring_id).first()

        instance_count_ok = check_user_instance_count(cluster, instance, type='healthcheck')
        if not instance_count_ok:
            return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_402_PAYMENT_REQUIRED)

        redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
        redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

        cluster = alaz_set_instance_as_active(cluster, instance, 'healthcheck')
        now = int(datetime.now(UTC).timestamp() * 1000)
        cluster.instances[instance] = now
        cluster.is_alive = True
        cluster.alaz_info = alaz_info
        cluster.alaz_version = alaz_version
        cluster.last_heartbeat = datetime.now(UTC)
        cluster.heartbeat_mail_sent=False
        cluster.fresh = False
        
        save_cluster(cluster)

        try:
            if 'telemetry' in data:
                default_setting_qs = Setting.objects.filter(name='default')
                if default_setting_qs.exists():
                    default_setting = default_setting_qs.first()
                    onprem_key = default_setting.onprem_key
                    telemetry = data['telemetry']

                    telemetry_data_qs = Telemetry.objects.filter(onprem_key=onprem_key, monitoring_id=cluster.monitoring_id)
                    if telemetry_data_qs.exists():
                        telemetry_data = telemetry_data_qs.first()
                        kernel_versions = telemetry_data.data.get('kernel_versions', [])
                        # If there is an existing kernel version in the wrong format, add it to the list if it does not exist
                        if 'kernel_version' in telemetry_data.data and telemetry_data.data['kernel_version'] not in kernel_versions:
                            kernel_versions.append(telemetry_data.data['kernel_version'])
                        # Add the incoming kernel version if it does not exist
                        if telemetry['kernel_version'] not in kernel_versions:
                            kernel_versions.append(telemetry['kernel_version'])
                        del telemetry['kernel_version']
                        telemetry_data.data = telemetry
                        telemetry_data.data['kernel_versions'] = kernel_versions
                        telemetry_data.timestamp = datetime.now(UTC)
                        telemetry_data.save()
                    else:
                        if 'kernel_version' in telemetry:
                            telemetry['kernel_versions'] = [telemetry['kernel_version']]
                            del telemetry['kernel_version']
                        else:
                            telemetry['kernel_versions'] = []
                        Telemetry.objects.create(onprem_key=onprem_key, monitoring_id=cluster.monitoring_id, data=telemetry, timestamp=datetime.now(UTC))
                else:
                    logger.warn("Default setting not found, will not process telemetry data.")
        except Exception as exc:
            logger.error("Error while processing telemetry data: ", exc)

        return Response({"msg": "Heartbeat received"}, status=status.HTTP_200_OK)
        

class UserLastDataTsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyAlazDefaultThrottleApiKey]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})
    
    def get(self, request, *args, **kwargs):
        user_id = kwargs.get('user_id', None)
        if not user_id:
            raise exceptions.InvalidRequestError({'msg': "User id is required"})
        
        try:
            user = get_user_model().objects.get(pk=user_id)
        except Exception:
            raise exceptions.NotFoundError({'msg': "User not found"})
        
        timestamp, monitoring_id = get_last_data_ts_of_user(user)
        return Response({'timestamp': timestamp, 'monitoring_id': monitoring_id}, status=status.HTTP_200_OK)


class LogResourcesView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})
    
    def get(self, request, *args, **kwargs):
        monitoring_id = request.query_params.get('monitoring_id', None)
        if not monitoring_id:
            raise exceptions.InvalidRequestError({'msg': "Monitoring id is required"})
        
        user_id = request.query_params.get('user_id', None)
        if not user_id:
            raise exceptions.InvalidRequestError({'msg': "User id is required"})
        
        namespace = request.query_params.get('namespace', None)
        if not namespace:
            raise exceptions.InvalidRequestError({'msg': "Namespace is required"})
        
        try:
            user = get_user_model().objects.get(pk=user_id)
        except Exception:
            raise exceptions.NotFoundError({'msg': "User not found"})
        
        try:
            cluster = Cluster.objects.get(monitoring_id=monitoring_id)
        except Exception:
            raise exceptions.NotFoundError({'msg': "Monitoring id is invalid"})
        
        try:
            check_monitoring_read_permission(user, cluster)
        except Exception as exc:
            raise exceptions.NotFoundError({'msg': 'Monitoring id is invalid'})
        
        sources = get_log_sources(cluster, namespace)
        # sources = get_log_sources(str(cluster.monitoring_id), namespace)
        if not sources:
            return Response({}, status=status.HTTP_200_OK)

        return Response(sources, status=status.HTTP_200_OK)
            

class LogsDownloadView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})
    
    def get(self, request, *args, **kwargs):
        monitoring_id = request.query_params.get('monitoring_id', None)
        if not monitoring_id:
            raise exceptions.InvalidRequestError({'msg': "Monitoring id is required"})
        
        user_id = request.query_params.get('user_id', None)
        if not user_id:
            raise exceptions.InvalidRequestError({'msg': "User id is required"})

        pod_uid = request.query_params.get('pod_uid', None)
        if not pod_uid or pod_uid == 'undefined':
            raise exceptions.InvalidRequestError({'msg': "Pod uid is required"})

        container_name = request.query_params.get('container_name', None)
        if not container_name or container_name == 'undefined':
            raise exceptions.InvalidRequestError({'msg': "Container name is required"})
        
        container_num = request.query_params.get('container_num', None)
        if not container_num or container_num == 'undefined':
            raise exceptions.InvalidRequestError({'msg': "Container num is required"})

        try:
            user = get_user_model().objects.get(pk=user_id)
        except Exception:
            raise exceptions.NotFoundError({'msg': "User not found"})
        
        try:
            cluster = Cluster.objects.get(monitoring_id=monitoring_id)
        except Exception:
            raise exceptions.NotFoundError({'msg': "Monitoring id is invalid"})
        
        try:
            check_monitoring_read_permission(user, cluster)
        except Exception as exc:
            raise exceptions.NotFoundError({'msg': 'Monitoring id is invalid'})
        
        try:
            logs = get_all_logs(str(cluster.monitoring_id), pod_uid, container_name, container_num)
            if not logs:
                return Response({'msg': "No logs found"}, status=status.HTTP_200_OK)
            merged_logs = []
            current_log = []
            for log in logs:
                partial = log[0]
                log_text = log[1]
                current_log.append(log_text)
                if partial == 'false': # If not partial, then it is the end of the log
                    merged_logs.append(''.join(current_log))
                    current_log = []
                
            if current_log:
                merged_logs.append(''.join(current_log))
            
            response = '\n'.join(merged_logs)
            return HttpResponse(response, content_type='application/octet-stream')
        except Exception as exc:
            logger.error("Error while fetching and processing the logs: ", exc)
            raise exceptions.InvalidRequestError({'msg': "Error while fetching and processing the logs"})

# if settings.TCPDUMP_ENABLED:
#     class TcpDumpView(GenericAPIView):
#         throttle_classes = (throttling.ConcurrencyAlazTCPDumpThrottleApiKey,)

#         def throttled(self, request, wait):
#             raise Throttled(detail={"msg": "Request was throttled"})

#         def post(self, request):
#             cluster, idempotency_key, alaz_version, instance = validate_alaz_post_method(request, method_type='requests')
#             data = request.data
#             # if settings.ANTEON_ENV != 'onprem':
#                 # if not find_team_owner_if_exists(cluster.user).active:
#                     # return Response({"msg": "User is not active"}, status=status.HTTP_402_PAYMENT_REQUIRED)
                
#             # verbose_log(f'Alaz requests payload: {data}')

#             if 'resource_type' not in request.data['metadata']:
#                 raise exceptions.InvalidRequestError({'msg': "Resource type is required"})
#             resource_type = request.data['metadata']['resource_type']
#             if 'resource_id' not in request.data['metadata']:
#                 raise exceptions.InvalidRequestError({'msg': "Resource id is required"})
#             resource_id = request.data['metadata']['resource_id']

#             if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
#                 return Response({"msg": "Alaz requests are already processed"}, status=status.HTTP_200_OK)

#             # cached_replica_sets, cached_deployments, cached_pods, cached_endpoints, cached_services, cached_daemon_sets, cached_outbounds, endpoint_to_service = cache_alaz_resources(cluster, None)
#             # instance_count_ok = check_user_instance_count(cluster, instance, type='service_map')
#             # if not instance_count_ok:
#                 # return Response({"msg": "Instance count is exceeded"}, status=status.HTTP_200_OK)

#             requests = data["requests"]
#             alaz_requests = []
#             response = {'msg': 'Alaz requests are processed', 'errors': []}
#             request_num = 1

#             # pods_to_deployments_hset = settings.PODS_TO_DEPLOYMENTS_HSET
#             # pods_to_daemonsets_hset = settings.PODS_TO_DAEMONSETS_HSET
#             # pods_to_statefulsets_hset = settings.PODS_TO_STATEFULSETS_HSET
#             # node_id = instance
#             # last_data_ts = None
#             for request in requests:
#                 # Fields do not have keys, their order is:
#                 # 1- timestamp
#                 # 2- from_ip
#                 # 3- to_ip
#                 # 4- from_port
#                 # 5- to_port
#                 # 6- payload
#                 try:
#                     if len(request) != 6:
#                         raise exceptions.InvalidRequestError({'msg': "Request fields are not valid"})

#                     timestamp = int(request[0]) * 1_000
#                     from_ip = request[1]
#                     to_ip = request[2]
#                     from_port = request[3]
#                     to_port = request[4]
#                     payload = request[5]

#                     kwargs = [
#                         timestamp,
#                         str(cluster.monitoring_id),
#                         str(cluster.uid),
#                         resource_type,
#                         resource_id,
#                         from_ip,
#                         to_ip,
#                         from_port,
#                         to_port,
#                         payload
#                     ]

#                     # from_uid_pod = None
#                     # from_uid_service = None
#                     # from_uid_deployment = None
#                     # from_uid_daemonset = None
#                     # from_uid_statefulset = None

#                     # if from_type == "pod":
#                     #     from_uid_pod = from_id
#                     # elif from_type == "service":
#                     #     from_uid_service = from_id
#                     # else:
#                     #     raise exceptions.InvalidRequestError({'msg': f"Invalid from type: {from_type}"})

#                     # if from_type == 'pod':
#                     #     deployment_uid = redis_client.hget(pods_to_deployments_hset, from_id)
#                     #     if deployment_uid:
#                     #         from_uid_deployment = deployment_uid
#                     #     else:
#                     #         daemonset_uid = redis_client.hget(pods_to_daemonsets_hset, from_id)
#                     #         if daemonset_uid:
#                     #             from_uid_daemonset = daemonset_uid
#                     #         else:
#                     #             statefulset_uid = redis_client.hget(pods_to_statefulsets_hset, from_id)
#                     #             if statefulset_uid:
#                     #                 from_uid_statefulset = statefulset_uid
                        

#                     # source_type, source_obj = alaz_find_source_node(from_uid_pod, from_uid_service, cached_pods, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_services)

#                     # if source_type == 'deployment':
#                     #     from_uid_deployment = str(source_obj.uid)
#                     # elif source_type == 'daemon_set':
#                     #     from_uid_daemonset = str(source_obj.uid)

#                     # to_uid_pod = None
#                     # to_uid_service = None
#                     # to_url_outbound = None
#                     # to_uid_deployment = None
#                     # to_uid_daemonset = None
#                     # to_uid_statefulset = None

#                     # if to_type == "pod":
#                     #     to_uid_pod = to_id
#                     # elif to_type == "service":
#                     #     to_uid_service = to_id
#                     # elif to_type == 'outbound':
#                     #     to_url_outbound = to_id
#                     # else:
#                     #     raise exceptions.InvalidRequestError({'msg': f"Invalid to type: {to_type}"})

#                     # if to_type == 'pod':
#                     #     deployment_uid = redis_client.hget(pods_to_deployments_hset, to_id)
#                     #     if deployment_uid:
#                     #         to_uid_deployment = deployment_uid
#                     #     else:
#                     #         daemonset_uid = redis_client.hget(pods_to_daemonsets_hset, to_id)
#                     #         if daemonset_uid:
#                     #             to_uid_daemonset = daemonset_uid
#                     #         else:
#                     #             statefulset_uid = redis_client.hget(pods_to_statefulsets_hset, to_id)
#                     #             if statefulset_uid:
#                     #                 to_uid_statefulset = statefulset_uid

#                     # dest_type, dest_obj = alaz_find_destination_node(to_uid_pod, to_uid_service, to_url_outbound, cached_pods, cached_services, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds)

#                     # if dest_type == 'deployment':
#                     #     to_uid_deployment = str(dest_obj.uid)
#                     # elif dest_type == 'daemon_set':
#                     #     to_uid_daemonset = str(dest_obj.uid)

#                     # kwargs['from_uid_pod'] = from_uid_pod
#                     # kwargs['from_uid_service'] = from_uid_service
#                     # kwargs['from_uid_deployment'] = from_uid_deployment
#                     # kwargs['from_uid_daemonset'] = from_uid_daemonset
#                     # kwargs['from_uid_statefulset'] = from_uid_statefulset
#                     # kwargs['to_uid_pod'] = to_uid_pod
#                     # kwargs['to_uid_service'] = to_uid_service
#                     # kwargs['to_uid_deployment'] = to_uid_deployment
#                     # kwargs['to_uid_daemonset'] = to_uid_daemonset
#                     # kwargs['to_url_outbound'] = to_url_outbound
#                     # kwargs['to_uid_statefulset'] = to_uid_statefulset
#                     alaz_requests.append(json.dumps(kwargs))

#                 except Exception as exc:
#                     errors = str(exc.args)
#                     logger.error(f"Alaz request is not valid: {request}, {exc}")
#                     response['errors'].append({'request_num': request_num, 'request': request, 'errors': errors})

#                 request_num += 1

#             start_time = datetime.now()
#             try:
#                 if len(alaz_requests) > 0:
#                     redis_client.rpush(settings.TCPDUMP_QUEUE, *alaz_requests)
#             except Exception as exc:
#                 logger.fatal(f"Redis rpush failed: {exc}")
#                 raise exceptions.InvalidRequestError({"msg": "Redis rpush failed"})

#             redis_client.hset("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD, 'true')
#             redis_client.expire("idempotency_key:"+idempotency_key, settings.IDEMPOTENCY_KEYS_EXPIRE_TIME_SECONDS)

#             bytes = len(json.dumps(data))
#             redis_client.hincrby(settings.REQUESTS_SIZE_KEY, str(cluster.uid), bytes)

#             if settings.VERBOSE_LOG and 'errors' in response and len(response['errors']) > 0:
#                 logger.warn(response)

#             return Response(response, status=status.HTTP_200_OK)
          
          
class ClusterListViewHeartbeatThreeDaysAgo(GenericAPIView):
    def get(self, request, *args, **kwargs):
        try:
            three_days_ago = timezone.now() - timedelta(days=3)
            
            # Filter clusters whose last_heartbeat is 3 days ago or more and heartbeat_mail_sent is False
            clusters = Cluster.objects.filter(last_heartbeat__lte=three_days_ago, heartbeat_mail_sent=False)[:settings.NO_HEARTBEAT_MAX_MAIL_LENGTH]
            
            # List to hold the clusters to update
            cluster_ids = [cluster.uid for cluster in clusters]

            # Update heartbeat_mail_sent to True for these clusters
            Cluster.objects.filter(uid__in=cluster_ids).update(heartbeat_mail_sent=True)
            
            # Serialize the data
            serializer = ClusterSerializer(clusters, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as exc:
            logger.error("Error while getting 50 clusters which dont have heartbeat for 3 days: ", exc)
            raise exceptions.InvalidRequestError({'msg': "Error while getting 50 clusters which dont have heartbeat for 3 days"})
            
            
class ClusterHandleHeartbeatThreeDaysAgo(GenericAPIView):
    def post(self, request, *args, **kwargs):
        try:
            data = json.loads(request.body)
            failed_clusters = data.get('failed_clusters', [])
            
            if not isinstance(failed_clusters, list):
                raise ValueError("Invalid data format for failed_clusters")

            # Handle the failed clusters here
            logger.info(f'Received failed clusters: {failed_clusters}')
            
            clusters_to_update = Cluster.objects.filter(uid__in=failed_clusters)
            clusters_to_update.update(heartbeat_mail_sent=False)

            response_data = {'status': 'success', 'message': 'Failed clusters processed successfully'}
            return Response(response_data, status=status.HTTP_200_OK)
        except json.JSONDecodeError:
            logger.error('Invalid JSON received')
            return Response({'status': 'error', 'message': 'Invalid JSON'}, status=status.HTTP_200_OK)
        except ValueError as ve:
            logger.error(f'ValueError: {ve}')
            return Response({'status': 'error', 'message': str(ve)}, status=status.HTTP_200_OK)
        except Exception as e:
            logger.error(f'Unexpected error: {e}', exc_info=True)
            return Response({'status': 'error', 'message': 'Internal server error'}, status=status.HTTP_200_OK)


class APICatalogOptionsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    @silk_wrapper(name='ApiCatalogOptions')
    def get(self, request, monitoring_id):
        total_start = time.time()

        times = {
            'validation': None,
            'request_filter': None,
            'protocol_find': None,
            'status_code_find': None,
            'method_find': None,
            'tls_find': None,
            'namespace_find': None,
            'outbound_find': None,
            'resource_writing': None
        }

        timer = time.time()
        start_time = request.query_params.get('start_time', None)  # in ms
        end_time = request.query_params.get('end_time', None)  # in ms

        start_time, end_time = validate_start_and_end_times(start_time, end_time)
        cluster = validate_monitoring_id(monitoring_id)
        times['validation'] = time.time() - timer

        timer = time.time()
        # Get the distinct values of requests in that interval
        requests = Request.objects.filter(cluster=cluster.uid, start_time__gte=start_time, start_time__lte=end_time)
        times['request_filter'] = time.time() - timer

        logger.info(f'Found {requests.count()} requests for monitoring_id: {monitoring_id}')

        timer = time.time()
        protocols = list(requests.values_list('protocol', flat=True).distinct())
        times['protocol_find'] = time.time() - timer
        
        timer = time.time()
        status_codes = list(requests.values_list('status_code', flat=True).distinct()) 
        times['status_code_find'] = time.time() - timer
        
        timer = time.time()
        methods = list(requests.values_list('method', flat=True).distinct())
        times['method_find'] = time.time() - timer
        
        timer = time.time()
        tls = list(requests.values_list('tls', flat=True).distinct())
        times['tls_find'] = time.time() - timer

        timer = time.time()
        namespaces = get_resources_of_cluster(cluster)
        times['namespace_find'] = time.time() - timer
        
        timer = time.time()
        outbounds = list(requests.exclude(to_url_outbound__isnull=True).values_list('to_url_outbound', flat=True).distinct())
        times['outbound_find'] = time.time() - timer

        timer = time.time()
        from_resources = namespaces
        to_resources = namespaces.copy()
        to_resources['outbounds'] = outbounds
        times['resource_writing'] = time.time() - timer

        result = {
            'protocols': protocols,
            'status_codes': status_codes,
            'methods': methods,
            # 'to_ports': to_ports,
            'tls': tls,
            'from_resources': from_resources,
            'to_resources': to_resources,
            'extra_filters': {
            }
        }

        main_tls_set = set(tls)
        
        kafka_events = KafkaEvent.objects.filter(cluster=cluster.uid, start_time__gte=start_time, start_time__lte=end_time)
        
        if kafka_events.count() > 0:
            topics = list(kafka_events.values_list('topic', flat=True).distinct())
            partitions = list(kafka_events.values_list('partition', flat=True).distinct())
            keys = list(kafka_events.values_list('key', flat=True).distinct())
            values = list(kafka_events.values_list('value', flat=True).distinct())
            types = list(kafka_events.values_list('type', flat=True).distinct())
            tls = list(kafka_events.values_list('tls', flat=True).distinct())
            kafka_tls_set = set(tls)
            result['extra_filters']['KAFKA'] = {
                'topics': topics,
                'partitions': partitions,
                'keys': keys,
                'values': values,
                'types': types,
                # 'tls': tls
            }

            result['tls'] = list(main_tls_set.union(kafka_tls_set))
            result['protocols'].append('KAFKA')
            
        times['total_time'] = time.time() - total_start
        logger.info(f'Options endpoint took: {times}')

        return Response(result, status=status.HTTP_200_OK)


class APICatalogMainView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    @silk_wrapper(name='ApiCatalogMain')
    def post(self, request):
        # Arguments
        # start-end time
        # cluster
        # all other filters from the options endpoint
        total_start = time.time()

        times = {
            'request_filter': None,
            'grouping': None,
            'chart_grouping': None,
            'chart_separating': None,
            'status_code_grouping': None
        }
        
        data = request.data
        monitoring_id = data.get('monitoring_id', None)
        start_time = data.get('start_time', None)
        end_time = data.get('end_time', None)
        
        cluster = validate_monitoring_id(monitoring_id)
        start_time, end_time = validate_start_and_end_times(start_time, end_time)
        
        requests = Request.objects.filter(cluster=cluster.uid, start_time__gte=start_time, start_time__lte=end_time)
        
        protocols = data.get('protocols', [])
        status_codes = data.get('status_codes', [])
        methods = data.get('methods', [])
        # to_ports = data.get('to_ports', [])
        tls = data.get('tls', [])
        page_num = data.get('page_num', 1)
        from_resources = data.get('from_resources', [])
        to_resources = data.get('to_resources', [])
        load_chart = data.get('load_chart', False)

        page_size = settings.API_CATALOG_PAGE_SIZE
        
        request_filters = Q()
        
        request_filters &= Q(protocol__in=protocols)
        request_filters &= Q(status_code__in=status_codes)
        request_filters &= Q(method__in=methods)
        # filters &= Q(to_port__in=to_ports)
        request_filters &= Q(tls__in=tls)

        from_deployments = []
        from_daemonsets = []
        from_statefulsets = []
        from_pods = []
        from_services = []

        resource_filters = Q()
        
        # from_resource_filters = Q()
        for from_resource in from_resources:
            resource_type = from_resource.get('type', None)
            resource_uid = from_resource.get('uid', None)
            # resource_filter = Q()
            if resource_type == 'deployment':
                from_deployments.append(resource_uid)
                # resource_filter &= Q(from_uid_deployment=resource_uid)
            elif resource_type == 'daemon_set':
                from_daemonsets.append(resource_uid)
                # resource_filter &= Q(from_uid_daemonset=resource_uid)
            elif resource_type == 'stateful_set':
                from_statefulsets.append(resource_uid)
                # resource_filter &= Q(from_uid_statefulset=resource_uid)
            elif resource_type == 'pod':
                from_pods.append(resource_uid)
                # resource_filter &= Q(from_uid_pod=resource_uid)
            elif resource_type == 'service':
                from_services.append(resource_uid)
                # resource_filter &= Q(from_uid_service=resource_uid)

            # from_resource_filters |= resource_filter

        resource_filters &= Q(from_uid_deployment__in=from_deployments) | Q(from_uid_daemonset__in=from_daemonsets) | Q(from_uid_statefulset__in=from_statefulsets) | Q(from_uid_pod__in=from_pods) | Q(from_uid_service__in=from_services)

        to_deployments = []
        to_daemonsets = []
        to_statefulsets = []
        to_pods = []
        to_services = []
        to_outbounds = []

        # to_resource_filters = Q()
        for to_resource in to_resources:
            resource_type = to_resource.get('type', None)
            resource_uid = to_resource.get('uid', None)
            # resource_filter = Q()
            if resource_type == 'deployment':
                to_deployments.append(resource_uid)
                # resource_filter &= Q(to_uid_deployment=resource_uid)
            elif resource_type == 'daemon_set':
                to_daemonsets.append(resource_uid)
                # resource_filter &= Q(to_uid_daemonset=resource_uid)
            elif resource_type == 'stateful_set':
                to_statefulsets.append(resource_uid)
                # resource_filter &= Q(to_uid_statefulset=resource_uid)
            elif resource_type == 'pod':
                to_pods.append(resource_uid)
                # resource_filter &= Q(to_uid_pod=resource_uid)
            elif resource_type == 'service':
                to_services.append(resource_uid)
                # resource_filter &= Q(to_uid_service=resource_uid)
            elif resource_type == 'outbound':
                to_outbounds.append(resource_uid)
                # resource_filter &= Q(to_url_outbound=resource_uid)

            # to_resource_filters |= resource_filter
        
        resource_filters &= Q(to_uid_deployment__in=to_deployments) | Q(to_uid_daemonset__in=to_daemonsets) | Q(to_uid_statefulset__in=to_statefulsets) | Q(to_uid_pod__in=to_pods) | Q(to_uid_service__in=to_services) | Q(to_url_outbound__in=to_outbounds)
            

        timer = time.time()
        requests = requests.filter(request_filters)
        requests = requests.filter(resource_filters)
        times['request_filter'] = time.time() - timer

        logger.info(f'Found {requests.count()} requests for monitoring_id: {monitoring_id}')
        
        time_diff = (end_time - start_time).total_seconds() if end_time > start_time else 1

        page_start = (page_num - 1) * page_size
        page_end = page_num * page_size
        # Group requests by their from-to resources


        timer = time.time()
        requests_list = requests.annotate(
            from_uid = Coalesce('from_uid_deployment', 'from_uid_daemonset', 'from_uid_statefulset', 'from_uid_pod', 'from_uid_service'),
            from_type = Case(
                When(from_uid_deployment__isnull=False, then=Value('deployment')),
                When(from_uid_daemonset__isnull=False, then=Value('daemon_set')),
                When(from_uid_statefulset__isnull=False, then=Value('stateful_set')),
                When(from_uid_pod__isnull=False, then=Value('pod')),
                When(from_uid_service__isnull=False, then=Value('service')),
                default=Value('unknown'),
                output_field=CharField()
            ),
            to_uid = Coalesce('to_uid_deployment', 'to_uid_daemonset', 'to_uid_statefulset', 'to_uid_pod', 'to_uid_service'),
            to_type = Case(
                When(to_uid_deployment__isnull=False, then=Value('deployment')),
                When(to_uid_daemonset__isnull=False, then=Value('daemon_set')),
                When(to_uid_statefulset__isnull=False, then=Value('stateful_set')),
                When(to_uid_pod__isnull=False, then=Value('pod')),
                When(to_uid_service__isnull=False, then=Value('service')),
                When(to_url_outbound__isnull=False, then=Value('outbound')),
                default=Value('unknown'),
                output_field=CharField()
            )
        ).values(
            'method',
            'path',
            'status_code',
            'protocol',
            'tls',
            'from_uid',
            'from_type',
            'to_uid',
            'to_type',
            'to_url_outbound'
        ).annotate(
            count=Count('id'),
            rps=Count('id') / time_diff,
            avg_latency=Avg('latency'),
            p50_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.5,)),
            p90_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.9,)),
            p95_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.95,)),
            p99_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.99,))
            )
        
        total_list = list(requests_list)
        times['grouping'] = time.time() - timer
        
        resources = {
            'deployments': {},
            'daemon_sets': {},
            'stateful_sets': {},
            'pods': {},
            'services': {},
        }

        if 'KAFKA' in protocols:
            kafka_filters = Q()
            extra_filters = data.get('extra_filters', {})
            kafka_topics = extra_filters.get('KAFKA', {}).get('topics', [])
            kafka_partitions = extra_filters.get('KAFKA', {}).get('partitions', [])
            kafka_keys = extra_filters.get('KAFKA', {}).get('keys', [])
            kafka_values = extra_filters.get('KAFKA', {}).get('values', [])
            kafka_types = extra_filters.get('KAFKA', {}).get('types', [])
            kafka_filters &= Q(topic__in=kafka_topics)
            kafka_filters &= Q(partition__in=kafka_partitions)
            kafka_filters &= Q(key__in=kafka_keys)
            kafka_filters &= Q(value__in=kafka_values)
            kafka_filters &= Q(type__in=kafka_types)
            kafka_filters &= Q(tls__in=tls)

            kafka_events = KafkaEvent.objects.filter(cluster=cluster.uid, start_time__gte=start_time, start_time__lte=end_time)
            kafka_events = kafka_events.filter(kafka_filters)
            kafka_events = kafka_events.filter(resource_filters)

            kafka_events_list = kafka_events.annotate(
                from_uid = Coalesce('from_uid_deployment', 'from_uid_daemonset', 'from_uid_statefulset', 'from_uid_pod', 'from_uid_service'),
                from_type = Case(
                    When(from_uid_deployment__isnull=False, then=Value('deployment')),
                    When(from_uid_daemonset__isnull=False, then=Value('daemon_set')),
                    When(from_uid_statefulset__isnull=False, then=Value('stateful_set')),
                    When(from_uid_pod__isnull=False, then=Value('pod')),
                    When(from_uid_service__isnull=False, then=Value('service')),
                    default=Value('unknown'),
                    output_field=CharField()
                ),
                to_uid = Coalesce('to_uid_deployment', 'to_uid_daemonset', 'to_uid_statefulset', 'to_uid_pod', 'to_uid_service'),
                to_type = Case(
                    When(to_uid_deployment__isnull=False, then=Value('deployment')),
                    When(to_uid_daemonset__isnull=False, then=Value('daemon_set')),
                    When(to_uid_statefulset__isnull=False, then=Value('stateful_set')),
                    When(to_uid_pod__isnull=False, then=Value('pod')),
                    When(to_uid_service__isnull=False, then=Value('service')),
                    When(to_url_outbound__isnull=False, then=Value('outbound')),
                    default=Value('unknown'),
                    output_field=CharField(),
                ),
                protocol=Value('KAFKA')
            ).values(
                'topic',
                'partition',
                'key',
                'value',
                'type',
                'protocol',
                'tls',
                'from_uid',
                'from_type',
                'to_uid',
                'to_type',
                'to_url_outbound'
            ).annotate(
                count=Count('id'),
                rps=Count('id') / time_diff,
                avg_latency=Avg('latency'),
                p50_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.5,)),
                p90_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.9,)),
                p95_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.95,)),
                p99_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.99,))
                )

            total_list.extend(list(kafka_events_list))

        total_list = total_list[page_start:page_end]


        for elem in total_list:
            if elem['to_type'] == 'outbound':
                elem['to_uid'] = elem['to_url_outbound']
            del elem['to_url_outbound']                

        count_chart = []
        error_chart = []
        latency_chart = []
        status_codes = {}

        if load_chart:
            grouping_interval = get_grouping_interval(start_time, end_time)
            grouping_expr = get_grouping_expr(grouping_interval, 'start_time')
            # Group them by the grouping interval, get request count, error count, avg latency

            timer = time.time()
            groups = requests.annotate(
                start_time_truncated =TruncSecond('start_time')
            ).annotate(
                interval=grouping_expr
            ).values('interval'
            ).annotate(
                count=Count('id'),
                avg_latency=Avg('latency'),
                p50_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.5,)),
                p80_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.8,)),
                p90_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.9,)),
                p95_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.95,)),
                p98_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.98,)),
                p99_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.99,))
            ).order_by('interval')
            
            groups = list(groups)

            groups_status_codes = requests.annotate(
                start_time_truncated =TruncSecond('start_time')
            ).annotate(
                interval=grouping_expr
            ).values('interval',
                     'status_code'
            ).annotate(
                count=Count('id'),
            ).order_by('interval')
            
            groups_status_codes = list(groups_status_codes)
            times['chart_grouping'] = time.time() - timer
            
            if 'KAFKA' in protocols:
                kafka_groups = kafka_events.annotate(
                    start_time_truncated =TruncSecond('start_time')
                ).annotate(
                    interval=grouping_expr
                ).values('interval'
                ).annotate(
                    count=Count('id'),
                    # error_count = Value(0),
                    avg_latency=Avg('latency'),
                    p50_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.5,)),
                    p80_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.8,)),
                    p90_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.9,)),
                    p95_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.95,)),
                    p98_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.98,)),
                    p99_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.99,))                    
                ).order_by('interval')
                
                # Merge duplicate timestamps. Get the new avg latency and count
                for kafka_group in kafka_groups:
                    groups_status_codes.append(
                        {
                            "interval": kafka_group['interval'],
                            "status_code": 0,
                            "count": kafka_group['count']
                        }
                    )
                    
                if groups:
                    for kafka_group in kafka_groups:
                        found = False
                        for group in groups:
                            if group['interval'] == kafka_group['interval']:
                                new_sum = group['count'] * group['avg_latency'] + kafka_group['count'] * kafka_group['avg_latency']
                                group['avg_latency'] = new_sum / (group['count'] + kafka_group['count'])
                                group['count'] += kafka_group['count']
                                found = True
                                break
                        if not found:
                            groups.append(kafka_group)
                            
                else:
                    groups = kafka_groups
                    
                        
            # Separate groups 
            timer = time.time()
            for group in groups:
                timestamp = int(group['interval'] * 1000)
                
                count_chart.append(
                    {
                        'timestamp': timestamp,
                        'all_requests': group['count']
                    }
                )
                
                latency_chart.append(
                    {
                        'timestamp': timestamp,
                        'avg_latency': group['avg_latency'],
                        'p50_latency': group['p50_latency'],
                        'p80_latency': group['p80_latency'],
                        'p90_latency': group['p90_latency'],
                        'p95_latency': group['p95_latency'],
                        'p98_latency': group['p98_latency'],
                        'p99_latency': group['p99_latency']
                    }
                )
            times['chart_separating'] = time.time() - timer

            timer = time.time()
            for group in groups_status_codes:
                timestamp = int(group['interval'] * 1000)
                if timestamp not in status_codes:
                    status_codes[timestamp] = {
                        'timestamp': timestamp,
                    }
                status_code = group['status_code']
                if status_code not in status_codes[timestamp]:
                    status_codes[timestamp][status_code] = 0
                status_codes[timestamp][status_code] += group['count']

                error_chart.append(
                    {
                        'timestamp': timestamp,
                        'status_codes': group
                    }
                )
            times['status_code_grouping'] = time.time() - timer
            
        else:
            groups = []
            

        result = {
            'list': total_list,
            'count_chart': count_chart,
            'error_chart': status_codes.values(),
            'latency_chart': latency_chart,
        }
        
        times['total_time'] = time.time() - total_start
        logger.info(f'Main endpoint took: {times}')
        
        return Response(result, status=status.HTTP_200_OK)

        
class APICatalogDetailsView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    @silk_wrapper(name='ApiCatalogDetails')
    def post(self, request):
        total_start = time.time()
        data = request.data
        times = {
            'filtering+grouping': None,
            'chart_grouping': None,
            'chart_separating': None,
            'status_code_grouping': None
        }
        
        monitoring_id = data.get('monitoring_id', None)
        start_time = data.get('start_time', None)
        end_time = data.get('end_time', None)
        page_num = data.get('page_num', 1)
        load_chart = data.get('load_chart', False)
        from_type = data.get('from_type', None)
        from_uid = data.get('from_uid', None)
        to_type = data.get('to_type', None)
        to_uid = data.get('to_uid', None)
        protocol = data.get('protocol', None)
        tls = data.get('tls', None)
        if not protocol:
            raise exceptions.InvalidRequestError({'msg': 'Protocol is required.'})
        
        page_size = settings.API_CATALOG_PAGE_SIZE
        page_start = (page_num - 1) * page_size
        page_end = page_num * page_size
        
        cluster = validate_monitoring_id(monitoring_id)
        start_time, end_time = validate_start_and_end_times(start_time, end_time)
        
        result_list = []

        fields_to_include = [
            'start_time',
            'latency',
            'from_ip',
            'to_ip',
            'from_port',
            'to_port',
            'from_uid',
            'from_type',
            'to_uid',
            'to_type',
            'protocol',
            'to_url_outbound'
        ]

        result = prepare_api_catalog_selected_filters(data)
        
        # requests = Request.objects.filter(cluster=cluster.uid, start_time__gte=start_time, start_time__lte=end_time, method=method, path=path, status_code=status_code, protocol=protocol, to_port=to_port, tls=tls)
        if protocol == 'KAFKA':
            partition = data.get('partition', None)
            if partition is None:
                raise exceptions.InvalidRequestError({'msg': 'Partition is required for Kafka events'})
            key = data.get('key', None)
            if key is None:
                raise exceptions.InvalidRequestError({'msg': 'Key is required for Kafka events'})
            value = data.get('value', None)
            if value is None:
                raise exceptions.InvalidRequestError({'msg': 'Value is required for Kafka events'})
            topic = data.get('topic', None)
            if topic is None:
                raise exceptions.InvalidRequestError({'msg': 'Topic is required for Kafka events'})
            
            fields_to_include.extend(
                [
                    'topic',
                    'partition',
                    'key',
                    'value',
                    'type'
                ]
            )

            kafka_events = KafkaEvent.objects.filter(
                cluster=cluster.uid, 
                start_time__gte=start_time, 
                start_time__lte=end_time, 
                tls=tls,
                partition=partition,
                key=key,
                value=value,
                topic=topic).annotate(
                    from_uid=Coalesce('from_uid_deployment', 'from_uid_daemonset', 'from_uid_statefulset', 'from_uid_pod', 'from_uid_service'),
                    from_type=Case(
                        When(from_uid_deployment__isnull=False, then=Value('deployment')),
                        When(from_uid_daemonset__isnull=False, then=Value('daemon_set')),
                        When(from_uid_statefulset__isnull=False, then=Value('stateful_set')),
                        When(from_uid_pod__isnull=False, then=Value('pod')),
                        When(from_uid_service__isnull=False, then=Value('service')),
                        default=Value('unknown'),
                        output_field=CharField()
                    ),
                    to_uid=Coalesce('to_uid_deployment', 'to_uid_daemonset', 'to_uid_statefulset', 'to_uid_pod', 'to_uid_service'),
                    to_type=Case(
                        When(to_uid_deployment__isnull=False, then=Value('deployment')),
                        When(to_uid_daemonset__isnull=False, then=Value('daemon_set')),
                        When(to_uid_statefulset__isnull=False, then=Value('stateful_set')),
                        When(to_uid_pod__isnull=False, then=Value('pod')),
                        When(to_uid_service__isnull=False, then=Value('service')),
                        When(to_url_outbound__isnull=False, then=Value('outbound')),
                        default=Value('unknown'),
                        output_field=CharField(),
                    ),
                    protocol=Value('KAFKA')
                ).filter(
                    from_type=from_type,
                    from_uid=from_uid,
                )
                
            if to_type != 'outbound':
                kafka_events = kafka_events.filter(
                    to_type=to_type, 
                    to_uid=to_uid)
            else:
                kafka_events = kafka_events.filter(
                    to_url_outbound=to_uid)
                
            kafka_events = kafka_events.order_by('-start_time')
                
            logger.info(f'Found {kafka_events.count()} kafka events for monitoring_id: {monitoring_id}')

            result_list = list(kafka_events[page_start: page_end].values(*fields_to_include))

            # Group them by the grouping interval, get request count, error count, avg latency
            if load_chart:
                grouping_interval = get_grouping_interval(start_time, end_time)
                grouping_expr = get_grouping_expr(grouping_interval, 'start_time')

                groups = kafka_events.annotate(
                    start_time_truncated =TruncSecond('start_time')
                ).annotate(
                    interval=grouping_expr
                ).values('interval'
                ).annotate(
                    count=Count('id'),
                    # error_count = Value(0),
                    avg_latency=Avg('latency'),
                    p50_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.5,)),
                    p80_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.8,)),
                    p90_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.9,)),
                    p95_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.95,)),
                    p98_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.98,)),
                    p99_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.99,))                    
                ).order_by('interval')

                error_chart = {}
                for group in groups:
                    timestamp = int(group['interval'] * 1000)
                    error_chart[timestamp] = {
                        'timestamp': timestamp,
                        0: group['count']
                    }
                
                error_chart = list(error_chart.values())
            else:
                groups = []
            
        else:
            fields_to_include.extend(
                [
                    'status_code',
                    'fail_reason',
                    'method',
                    'path',
                    'non_simplified_path'
                ]
            )
            
            method = data.get('method', None)
            if method is None:
                raise exceptions.InvalidRequestError({'msg': 'Method is required for HTTP requests'})
            path = data.get('path', None)
            if path is None: 
                raise exceptions.InvalidRequestError({'msg': 'Path is required for HTTP requests'})
            status_code = data.get('status_code', None)
            if status_code is None:
                raise exceptions.InvalidRequestError({'msg': 'Status code is required for HTTP requests'})
            tls = data.get('tls', None)
            if tls is None:
                raise exceptions.InvalidRequestError({'msg': 'TLS is required for HTTP requests'})
            
            timer = time.time()
            requests = Request.objects.filter(
                cluster=cluster.uid, 
                start_time__gte=start_time, 
                start_time__lte=end_time, 
                method=method, 
                path=path, 
                status_code=status_code, 
                protocol=protocol, 
                tls=tls).annotate(
                    from_uid=Coalesce('from_uid_deployment', 'from_uid_daemonset', 'from_uid_statefulset', 'from_uid_pod', 'from_uid_service'),
                    from_type=Case(
                        When(from_uid_deployment__isnull=False, then=Value('deployment')),
                        When(from_uid_daemonset__isnull=False, then=Value('daemon_set')),
                        When(from_uid_statefulset__isnull=False, then=Value('stateful_set')),
                        When(from_uid_pod__isnull=False, then=Value('pod')),
                        When(from_uid_service__isnull=False, then=Value('service')),
                        default=Value('unknown'),
                        output_field=CharField()
                    ),
                    to_uid=Coalesce('to_uid_deployment', 'to_uid_daemonset', 'to_uid_statefulset', 'to_uid_pod', 'to_uid_service'),
                    to_type=Case(
                        When(to_uid_deployment__isnull=False, then=Value('deployment')),
                        When(to_uid_daemonset__isnull=False, then=Value('daemon_set')),
                        When(to_uid_statefulset__isnull=False, then=Value('stateful_set')),
                        When(to_uid_pod__isnull=False, then=Value('pod')),
                        When(to_uid_service__isnull=False, then=Value('service')),
                        When(to_url_outbound__isnull=False, then=Value('outbound')),
                        default=Value('unknown'),
                        output_field=CharField()
                    )
            ).filter(
                from_type=from_type,
                from_uid=from_uid,
            )

            if to_type != 'outbound':
                requests = requests.filter(
                    to_type=to_type, 
                    to_uid=to_uid)
            else:
                requests = requests.filter(
                    to_url_outbound=to_uid)
                
            requests = requests.order_by('-start_time')
                
            logger.info(f'Found {requests.count()} requests for monitoring_id: {monitoring_id}')
            
            result_list = list(requests[page_start: page_end].values(*fields_to_include))
            
            times['filtering+grouping'] = time.time() - timer
        
            # Group them by the grouping interval, get request count, error count, avg latency
            if load_chart:
                grouping_interval = get_grouping_interval(start_time, end_time)
                grouping_expr = get_grouping_expr(grouping_interval, 'start_time')

                timer = time.time()
                groups = requests.annotate(
                    start_time_truncated =TruncSecond('start_time')
                ).annotate(
                    interval=grouping_expr
                ).values('interval'
                ).annotate(
                    count=Count('id'),
                    # error_count = Count(
                    #     Case(
                    #         When(Q(protocol='HTTP') | Q(protocol='HTTPS'), status_code__gte=400, then=1),
                    #         When(~Q(protocol='HTTP') & ~Q(protocol='HTTPS'), status_code=3, then=1),
                    #         output_field=IntegerField()
                    #     )
                    # ),
                    avg_latency=Avg('latency'),
                    p50_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.5,)),
                    p80_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.8,)),
                    p90_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.9,)),
                    p95_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.95,)),
                    p98_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.98,)),
                    p99_latency=RawAnnotation('percentile_disc(%s) WITHIN GROUP (ORDER BY latency)', (0.99,))                    
                ).order_by('interval')
                
                times['chart_grouping'] = time.time() - timer

                timer = time.time()
                status_codes = requests.annotate(
                    start_time_truncated =TruncSecond('start_time')
                ).annotate(
                    interval=grouping_expr
                ).values('interval',
                         'status_code'
                ).annotate(
                    count=Count('id'),
                ).order_by('interval')

                error_chart = {}
                
                for group in status_codes:
                    timestamp = int(group['interval'] * 1000)
                    if timestamp not in error_chart:
                        error_chart[timestamp] = {
                            'timestamp': timestamp
                        }
                    status_code = group['status_code']
                    if status_code not in error_chart[timestamp]:
                        error_chart[timestamp][status_code] = 0
                    error_chart[timestamp][status_code] += group['count']

                error_chart = error_chart.values()
                
                times['status_code_grouping'] = time.time() - timer
            else:
                groups = []

                
        for elem in result_list:
            if elem['to_type'] == 'outbound':
                elem['to_uid'] = elem['to_url_outbound']
            del elem['to_url_outbound'] 
           
        count_chart = []
        latency_chart = []
        timer = time.time()
        for group in groups:
            timestamp = int(group['interval'] * 1000)
            
            count_chart.append(
                {
                    'timestamp': timestamp,
                    'all_requests': group['count']
                }
            )
            
            # error_chart.append(
            #     {
            #         'timestamp': timestamp,
            #         'error_count': group['error_count']
            #     }
            # )
            
            latency_chart.append(
                {
                    'timestamp': timestamp,
                    'avg_latency': group['avg_latency'],
                    'p50_latency': group['p50_latency'],
                    'p80_latency': group['p80_latency'],
                    'p90_latency': group['p90_latency'],
                    'p95_latency': group['p95_latency'],
                    'p98_latency': group['p98_latency'],
                    'p99_latency': group['p99_latency']
                }
            )
        times['chart_separating'] = time.time() - timer
           
        # result = {
        #     'list': result_list,
        #     'count_chart': count_chart,
        #     'error_chart': error_chart,
        #     'latency_chart': latency_chart,
        # } 

        result['list'] = result_list
        result['count_chart'] = count_chart
        result['error_chart'] = error_chart
        result['latency_chart'] = latency_chart


        times['total_time'] = time.time() - total_start
        logger.info(f'Details endpoint took: {times}')
        
        return Response(result, status=status.HTTP_200_OK)
        

class K8sEventsView(GenericAPIView):
    throttle_classes = (throttling.ConcurrencyAlazDefaultThrottleApiKey,)

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    # @log_endpoint
    # @silk_wrapper(name='Requests')
    def post(self, request):
        cluster_uid, idempotency_key, alaz_version, instance = lightweight_validate_alaz_post(request, endpoint_type='events')
        data = request.data

        batch_size = settings.BULK_BATCH_SIZE

        if redis_client.hget("idempotency_key:"+idempotency_key, settings.REDIS_IDEMPOTENCY_FIELD):
            return Response({"msg": "K8s events are already processed"}, status=status.HTTP_200_OK)

        events = data.get('events', [])
        event_objects = {}
        event_names = []
        for event in events:

            event_name = event.get('EventName', None)
            kind = event.get('Kind', None)
            namespace = event.get('Namespace', None)
            name = event.get('Name', None)
            uid = event.get('Uid', None)
            reason = event.get('Reason', None)
            message = event.get('Message', None)
            count = event.get('Count', None)
            first_timestamp = event.get('FirstTimestamp', None) # ms
            last_timestamp = event.get('LastTimestamp', None) # ms

            try:
                first_timestamp = int(first_timestamp)
                last_timestamp = int(last_timestamp)
                if first_timestamp < 0 or last_timestamp < 0:
                    logger.warn(f'Negative timestamp for K8s Event: {first_timestamp} or {last_timestamp}')
                    continue
            except Exception as e:
                logger.warn(f'Error while parsing timestamp for K8s Event: {first_timestamp} or {last_timestamp}')
                continue
            
            event_objects[event_name] = {
                'processed': False,
                'object': K8sEvent(
                cluster=cluster_uid,
                event_name=event_name,
                kind=kind,
                namespace=namespace,
                name=name,
                uid=uid,
                reason=reason,
                message=message,
                count=count,
                first_timestamp=datetime.fromtimestamp(first_timestamp / 1000, UTC),
                last_timestamp=datetime.fromtimestamp(last_timestamp / 1000, UTC)
            )} 
            
            event_names.append(event_name)
            
        existing_ones = K8sEvent.objects.filter(cluster=cluster_uid, event_name__in=event_names)

        events_to_update = []
        
        for event in existing_ones:
            event_name = event.event_name
            if event_name in event_objects:
                event_objects[event_name]['processed'] = True
                event.count = event_objects[event_name]['object'].count
                event.last_timestamp = event_objects[event_name]['object'].last_timestamp
                events_to_update.append(event)
                
        events_to_create = [event_objects[event_name]['object'] for event_name in event_objects if not event_objects[event_name]['processed']]
                
        try:
            K8sEvent.objects.bulk_update(events_to_update, ['count', 'last_timestamp'], batch_size=batch_size)
        except Exception as e:
            for event in events_to_update:
                try:
                    event.save()
                except Exception as e:
                    logger.error(f'Error while updating K8s event: {event}. {e}')
                    continue
        
        try:
            K8sEvent.objects.bulk_create(events_to_create, batch_size=batch_size)
        except Exception as e:
            for event in events_to_create:
                try:
                    event.save()
                except Exception as e:
                    logger.error(f'Error while creating K8s event: {event}. {e}')
                    continue

        return Response({'msg': 'K8s events are processed'}, status=status.HTTP_200_OK)


class K8sEventsFiltersView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def get(self, request, monitoring_id):

        start_time = request.query_params.get('start_time', None)  # in ms
        end_time = request.query_params.get('end_time', None)  # in ms

        start_time, end_time = validate_start_and_end_times(start_time, end_time)
        cluster = validate_monitoring_id(monitoring_id)
        
        events = K8sEvent.objects.filter(cluster=cluster.uid, first_timestamp__gte=start_time, last_timestamp__lte=end_time)

        kinds = list(events.values_list('kind', flat=True).distinct())
        namespaces = list(events.values_list('namespace', flat=True).distinct())
        # Get all (name, uid) combinations
        resources = list(events.values_list('name', 'uid').distinct())
        processed_resources = []
        for resource in resources:
            processed_resources.append({
                'name': resource[0],
                'uid': resource[1]
            })
        reasons = list(events.values_list('reason', flat=True).distinct())

        result = {
            'kinds': kinds,
            'namespaces': namespaces,
            'resources': processed_resources,
            'reasons': reasons
        }

        return Response(result, status=status.HTTP_200_OK)


class K8sEventsListView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    permission_classes = [BasicAuth]

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def post(self, request):
        data = request.data
        
        monitoring_id = data.get('monitoring_id', None)
        start_time = data.get('start_time', None)
        end_time = data.get('end_time', None)
        page_num = data.get('page_num', 1)
        load_chart = data.get('load_chart', False)
        kinds = data.get('kinds', [])
        namespaces = data.get('namespaces', [])
        resources = data.get('resources', [])
        reasons = data.get('reasons', [])
        
        page_size = settings.K8S_EVENTS_PAGE_SIZE
        page_start = (page_num - 1) * page_size
        page_end = page_num * page_size
        
        cluster = validate_monitoring_id(monitoring_id)
        start_time, end_time = validate_start_and_end_times(start_time, end_time)
        
        result_list = []

        fields_to_include = [
            'event_name',
            'kind',
            'namespace',
            'name',
            'uid',
            'reason',
            'message',
            'count',
            'first_timestamp',
            'last_timestamp'
        ]

        result = prepare_k8s_events_selected_filters(data)
        
        events = K8sEvent.objects.filter(
            cluster=cluster.uid, 
            first_timestamp__gte=start_time, 
            last_timestamp__lte=end_time,
            kind__in=kinds,
            namespace__in=namespaces,
            name__in=[resource['name'] for resource in resources],
            uid__in=[resource['uid'] for resource in resources],
            reason__in=reasons).order_by('-last_timestamp')
        

        result_list = list(events[page_start: page_end].values(*fields_to_include))
        
        # Group them by the grouping interval, get request count, error count, avg latency
        if load_chart:
            grouping_interval = get_grouping_interval(start_time, end_time)
            grouping_expr = get_grouping_expr(grouping_interval, 'first_timestamp')

            groups = events.annotate(
                first_timestamp_truncated =TruncSecond('first_timestamp')
            ).annotate(
                timestamp=grouping_expr
            ).values('timestamp'
            ).annotate(
                count=Count('id')
            ).order_by('timestamp')
            
            for group in groups:
                group['timestamp'] = int(group['timestamp'] * 1000)
            
        else:
            groups = []

        result['list'] = result_list
        result['chart'] = list(groups)

        return Response(result, status=status.HTTP_200_OK)
      