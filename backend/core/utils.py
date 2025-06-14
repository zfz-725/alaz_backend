
from django.db.models import F, ExpressionWrapper, Func, FloatField
import functools
import time
import traceback
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
import datetime
import logging
import re
import uuid
from django.db.models.expressions import RawSQL


import redis

from accounts.models import User
from accounts.utils import find_team_owner_if_exists
# from accounts.utils import find_team_owner_if_exists, get_max_ips_for_onprem
# from accounts.serializers import UserDetailSerializer
from core.clickhouse_utils import get_last_2_container_ids
from core import exceptions
from core.models import Cluster, Container, DaemonSet, Deployment, Endpoint, ReplicaSet, Service, Pod, Setting, StatefulSet
from core.requester import PrometheusRequester, SlackRequester
from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import connection, transaction
from contextlib import contextmanager
from contextlib import contextmanager

logger = logging.getLogger(__name__)
redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0, charset="utf-8", decode_responses=True)

def verbose_log(log):
    if settings.VERBOSE_LOG:
        logger.info(log)

clusters = {}

def alaz_find_pod_parent(pod, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_stateful_sets):
    # If parent does not exist, return the pod itself
    if pod.replicaset_owner:
        # Look for deployment
        replica_set = cached_replica_sets.get(pod.replicaset_owner, None)
        if replica_set is None:
            # print(f"For pod: {pod.uid}, replicaset with id {pod.replicaset_owner} could not be found")
            return None, None
        deployment = cached_deployments.get(replica_set.owner, None)
        if deployment is None:
            # print(
                # f"For pod: {pod.uid}, and replicaset: {replica_set.uid}, deployment with id {replica_set.owner} could not be found")
            return None, None
        return "deployment", deployment
    elif pod.daemonset_owner:
        # Look for daemonset
        daemon_set = cached_daemon_sets.get(pod.daemonset_owner, None)
        if daemon_set is None:
            # print(f"For pod: {pod.uid}, daemonset with id {pod.daemonset_owner} could not be found")
            return None, None
        return "daemon_set", daemon_set
    elif pod.statefulset_owner:
        # Look for statefulset
        stateful_set = cached_stateful_sets.get(pod.statefulset_owner, None)
        if stateful_set is None:
            # print(f"For pod: {pod.uid}, statefulset with id {pod.statefulset_owner} could not be found")
            return None, None
        return "stateful_set", stateful_set
    else:
        # No parent
        return "pod", pod

def alaz_find_source_node(from_uid_pod, from_uid_service, cached_pods, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_services):
    # Assuming that pod type is given
    # Try excepts are added because of namespace filtering
    # If a request contains multiple namespaces and only some of them are given as filtering, it raises an exception
    # So we catch them and return None, None
    try:
        if from_uid_pod is not None:
            pod_uid = from_uid_pod
            if type(pod_uid) == str:
                pod_uid = uuid.UUID(pod_uid)
            pod = cached_pods.get(pod_uid, None)
            if pod is None:
                # print(f"For request: {request.id}, source pod with id {pod_uid} could not be found")
                return None, None
            source_type, source_object = alaz_find_pod_parent(
                pod, cached_replica_sets, cached_deployments, cached_daemon_sets)
        elif from_uid_service is not None:
            service_uid = from_uid_service
            if type(service_uid) == str:
                service_uid = uuid.UUID(service_uid)
            service = cached_services.get(service_uid, None)
            if service is None:
                # print(f"For request: {request.id}, source service with id {service_uid} could not be found")
                return None, None
            source_type, source_object = "service", service
        else:
            # print(f'For request: {request.id}, source resource type could not be found')
            return None, None
        return source_type, source_object
    except Exception as e:
        # print(f'For request: {request.id}, source resource could not be found')
        return None, None

def alaz_request_find_source_node(group, cached_pods, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_services, cached_stateful_sets):
    # Assuming that pod type is given
    # Try excepts are added because of namespace filtering
    # If a request contains multiple namespaces and only some of them are given as filtering, it raises an exception
    # So we catch them and return None, None
    # TODO: Add deployment + daemonset support to prevent unnecessary checks
    try:
        # if group['from_uid_pod']:
        #     return 'pod', cached_pods.get(group['from_uid_pod'], None)
        # elif group['from_uid_service']:
        #     return 'service', cached_services.get(group['from_uid_service'], None)
        # elif group['from_uid_deployment']:
        #     return 'deployment', cached_deployments.get(group['from_uid_deployment'], None)
        # elif group['from_uid_daemon_set']:
        #     return 'daemon_set', cached_daemon_sets.get(group['from_uid_daemon_set'], None)
        # elif group['from_uid_stateful_set']:
        #     return 'stateful_set', cached_stateful_sets.get(group['from_uid_stateful_set'], None)
        # else:
        #     # print(f'For request: {request.id}, source resource type could not be found')
        #     return None, None

        if group['from_uid_pod'] is not None:
            pod_uid = group['from_uid_pod']
            pod = cached_pods.get(pod_uid, None)
            if pod is None:
                # print(f"For request: {request.id}, source pod with id {pod_uid} could not be found")
                return None, None
            source_type, source_object = alaz_find_pod_parent(
                pod, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_stateful_sets)
        elif group['from_uid_service'] is not None:
            service_uid = group['from_uid_service']
            service = cached_services.get(service_uid, None)
            if service is None:
                # print(f"For request: {request.id}, source service with id {service_uid} could not be found")
                return None, None
            source_type, source_object = "service", service
        else:
            # print(f'For request: {request.id}, source resource type could not be found')
            return None, None
        return source_type, source_object
    except Exception as e:
        # print(f'For request: {request.id}, source resource could not be found')
        return None, None

def get_pods_of_statefulset(statefulset, context):
    if 'cached_stateful_sets' in context and statefulset.uid in context['cached_stateful_sets']:
        return context['cached_stateful_sets'][statefulset.uid].pods.values()
    else:
        return Pod.objects.filter(statefulset_owner=statefulset.uid, cluster=statefulset.cluster, namespace=statefulset.namespace)


def alaz_add_used_port_to_resource(resource_type, resource, cached_pods, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets, port_info):
    uid = resource.uid
    try:
        if resource_type == "pod":
            if not hasattr(cached_pods[uid], 'ports'):
                cached_pods[uid].ports = []
            if port_info not in cached_pods[uid].ports:
                cached_pods[uid].ports.append(port_info)
            resource = cached_pods[uid]
        elif resource_type == "deployment":
            if not hasattr(cached_deployments[uid], 'ports'):
                cached_deployments[uid].ports = []
            if port_info not in cached_deployments[uid].ports:
                cached_deployments[uid].ports.append(port_info)
            resource = cached_deployments[uid]
        elif resource_type == "daemon_set":
            if not hasattr(cached_daemon_sets[uid], 'ports'):
                cached_daemon_sets[uid].ports = []
            if port_info not in cached_daemon_sets[uid].ports:
                cached_daemon_sets[uid].ports.append(port_info)
            resource = cached_daemon_sets[uid]
        elif resource_type == 'stateful_set':
            if not hasattr(cached_stateful_sets[uid], 'ports'):
                cached_stateful_sets[uid].ports = []
            if port_info not in cached_stateful_sets[uid].ports:
                cached_stateful_sets[uid].ports.append(port_info)
            resource = cached_stateful_sets[uid]
        elif resource_type == "outbound":
            if not hasattr(cached_outbounds[uid], 'ports'):
                cached_outbounds[uid].ports = []
            if port_info not in cached_outbounds[uid].ports:
                cached_outbounds[uid].ports.append(port_info)
            resource = cached_outbounds[uid]
        elif resource_type == 'service':
            if not hasattr(cached_services[uid], 'used_ports'):
                cached_services[uid].used_ports = []
            if port_info not in cached_services[uid].used_ports:
                cached_services[uid].used_ports.append(port_info)
    except Exception as e:
        uid = uuid.UUID(uid)
        if resource_type == "pod":
            if not hasattr(cached_pods[uid], 'ports'):
                cached_pods[uid].ports = []
            if port_info not in cached_pods[uid].ports:
                cached_pods[uid].ports.append(port_info)
            resource = cached_pods[uid]
        elif resource_type == "deployment":
            if not hasattr(cached_deployments[uid], 'ports'):
                cached_deployments[uid].ports = []
            if port_info not in cached_deployments[uid].ports:
                cached_deployments[uid].ports.append(port_info)
            resource = cached_deployments[uid]
        elif resource_type == "daemon_set":
            if not hasattr(cached_daemon_sets[uid], 'ports'):
                cached_daemon_sets[uid].ports = []
            if port_info not in cached_daemon_sets[uid].ports:
                cached_daemon_sets[uid].ports.append(port_info)
            resource = cached_daemon_sets[uid]
        elif resource_type == 'stateful_set':
            if not hasattr(cached_stateful_sets[uid], 'ports'):
                cached_stateful_sets[uid].ports = []
            if port_info not in cached_stateful_sets[uid].ports:
                cached_stateful_sets[uid].ports.append(port_info)
            resource = cached_stateful_sets[uid]
        elif resource_type == "outbound":
            if not hasattr(cached_outbounds[uid], 'ports'):
                cached_outbounds[uid].ports = []
            if port_info not in cached_outbounds[uid].ports:
                cached_outbounds[uid].ports.append(port_info)
            resource = cached_outbounds[uid]
        elif resource_type == 'service':
            if not hasattr(cached_services[uid], 'used_ports'):
                cached_services[uid].used_ports = []
            if port_info not in cached_services[uid].used_ports:
                cached_services[uid].used_ports.append(port_info)
    return resource


# def alaz_add_used_port_to_service(service, cached_services, port_info, to_port, protocol):
#     uid = 
#     if port_info not in 
#     found = False
#     uid = service.uid
#     if type(uid) == str:
#         uid = uuid.UUID(uid)
#     for port in cached_services[uid].ports:
#         if port['src'] == to_port and port['protocol'] == protocol:
#             found = True
#             break
#     if not found:
#         cached_services[service.uid].ports.append(port_info)
#         service = cached_services[service.uid]
#     return service

class Outbound():
    def __init__(self, url):
        self.uid = url
        self.connected_resources = {'incoming': [], 'outgoing': []}
        self.ports = []

def alaz_find_destination_node(to_uid_pod, to_uid_service, to_url_outbound, cached_pods, cached_services, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds):
    # Try excepts are added because of namespace filtering
    # If a request contains multiple namespaces and only some of them are given as filtering, it raises an exception
    # So we catch them and return None, None
    try:
        if to_uid_pod is not None:
            if type(to_uid_pod) == str:
                to_uid_pod = uuid.UUID(to_uid_pod)
            to_pod = cached_pods[to_uid_pod]
            dest_type, dest_object = alaz_find_pod_parent(
                to_pod, cached_replica_sets, cached_deployments, cached_daemon_sets)
        elif to_uid_service is not None:
            dest_type = "service"
            if type(to_uid_service) == str:
                to_uid_service = uuid.UUID(to_uid_service)
            dest_object = cached_services[to_uid_service]
        elif to_url_outbound is not None:
            dest_type = 'outbound'
            if to_url_outbound not in cached_outbounds:
                dest_object = Outbound(to_url_outbound)
                cached_outbounds[to_url_outbound] = dest_object
            else:
                dest_object = cached_outbounds[to_url_outbound]
        else:
            # print(f"Given request {request.id} does not have any destination")
            return None, None
        return dest_type, dest_object
    except Exception as exc:
        # print(f'For request: {request.id}, destination resource could not be found')
        return None, None

def alaz_request_find_destination_node(group, cached_pods, cached_services, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_stateful_sets, add_port=True):
    # Try excepts are added because of namespace filtering
    # If a request contains multiple namespaces and only some of them are given as filtering, it raises an exception
    # So we catch them and return None, None
    # TODO: Add deployment + daemonset support to prevent unnecessary checks
    port_info = {'port': group['to_port'], 'protocol': 'TCP'}
    try:
        # res_type = None
        # res = None
        # if group['to_uid_pod']:
        #     res_type = 'pod'
        #     res = cached_pods.get(group['to_uid_pod'], None)
        # elif group['to_uid_service']:
        #     res_type = 'service'
        #     res = cached_services.get(group['to_uid_service'], None)
        # elif group['to_uid_deployment']:
        #     res_type = 'deployment'
        #     res = cached_deployments.get(group['to_uid_deployment'], None)
        # elif group['to_uid_daemon_set']:
        #     res_type = 'daemon_set'
        #     res = cached_daemon_sets.get(group['to_uid_daemon_set'], None)
        # elif group['to_uid_stateful_set']:
        #     res_type = 'stateful_set'
        #     res = cached_stateful_sets.get(group['to_uid_stateful_set'], None)
        # elif group['to_url_outbound']:
        #     res_type = 'outbound'
        #     if group['to_url_outbound'] not in cached_outbounds:
        #         res = Outbound(group['to_url_outbound'])
        #         cached_outbounds[group['to_url_outbound']] = res
        #     else:
        #         res = cached_outbounds[group['to_url_outbound']]

        # if add_port:
        #     res = alaz_add_used_port_to_resource(
        #         res_type, res, cached_pods, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets, port_info)

        # return res_type, res
            
        if group['to_uid_pod'] is not None:
            to_pod = cached_pods[group['to_uid_pod']]
            dest_type, dest_object = alaz_find_pod_parent(
                to_pod, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_stateful_sets)
        elif group['to_uid_service'] is not None:
            dest_type = "service"
            dest_object = cached_services[group['to_uid_service']]
        elif group['to_url_outbound'] is not None:
            dest_type = 'outbound'
            if group['to_url_outbound'] not in cached_outbounds:
                dest_object = Outbound(group['to_url_outbound'])
                cached_outbounds[group['to_url_outbound']] = dest_object
            else:
                dest_object = cached_outbounds[group['to_url_outbound']]
        else:
            # print(f"Given request {request.id} does not have any destination")
            return None, None
        if add_port:
            verbose_log(f'Adding port {port_info} to the destination object {dest_object.uid}-{dest_type}')
            dest_object = alaz_add_used_port_to_resource(
                dest_type, dest_object, cached_pods, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets, port_info)
            verbose_log(f'Port {port_info} added to the destination object {dest_object.uid}-{dest_type}')
        return dest_type, dest_object
    except Exception as exc:
        # verbose_log(f'For request: {group}, destination resource could not be found due to {exc}. cached_services: {cached_services}')
        # print(f'For request: {request.id}, destination resource could not be found')
        return None, None


def alaz_find_target_port_of_service(to_port, service):
    # Try excepts are added because of namespace filtering
    # If a request contains multiple namespaces and only some of them are given as filtering, it raises an exception
    # So we catch them and return None, None
    try:
        pointed_port = None
        pointed_port_protocol = None
        for port in service.ports:
            if port['src'] == to_port:
                pointed_port = port['dest']
                pointed_port_protocol = port['protocol']
                break
        if pointed_port is None:
            print(
                f"The given service {service.uid} does not have a corresponding port mapping for the given port {to_port}")
            return None, None
        return pointed_port, pointed_port_protocol
    except Exception as exc:
        print(f'Target port for the port {to_port} of the service {service.uid} could not be found')
        return None, None


# TODO: Cache this
def alaz_find_pointed_destinations_of_service(service, to_port, pods, endpoints, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets, found_resources=None, add_used_port=True):
    # Try excepts are added because of namespace filtering
    # If a request contains multiple namespaces and only some of them are given as filtering, it raises an exception
    # So we catch them and return None, None
    target_port, target_port_protocol = alaz_find_target_port_of_service(to_port, service)
    if target_port is None or target_port_protocol is None:
        return found_resources, None
    service_uid = service.uid
    try:
        service_uid = uuid.UUID(service.uid)
    except:
        pass
    filtered_endpoints = endpoints.get(service_uid, None)
    if filtered_endpoints is None:
        if settings.VERBOSE_LOG:
            print(f' Endpoints could not be found for service {service_uid}')
        return found_resources, None
    pointed = dict()
    for endpoint in filtered_endpoints:
        for address in endpoint.addresses:
            for ip in address['ips']:
                try:
                    # TODO: Rename this to pod in the future
                    if ip['type'] == 'Pod':
                        pod = pods[uuid.UUID(ip['id'])]
                        destination_type, destination_object = alaz_find_pod_parent(
                            pod, cached_replica_sets, cached_deployments, cached_daemon_sets, cached_stateful_sets)
                        port_info = {'port': target_port, 'protocol': target_port_protocol}
                        if add_used_port:
                            destination_object = alaz_add_used_port_to_resource(
                                destination_type, destination_object, pods, cached_deployments, cached_daemon_sets, cached_outbounds, cached_services, cached_stateful_sets, port_info)
                        if found_resources:
                            found_resources[destination_type + 's'][str(destination_object.uid)] = destination_object
                        if str(destination_object.uid) not in pointed:
                            # This may be problematic in the future as different types may share the same uid
                            pointed[str(destination_object.uid)] = {
                                "type": destination_type,
                                "pointed_port": target_port}
                    # else:
                    #     # external ip
                    #     if ip['ip'] not in pointed:
                    #         pointed[ip['ip']] = {
                    #             "type": "external",
                    #             "port": target_port}
                except Exception as e:
                    print(f'One of the pointed destinations for the service {service.uid} could not be found. {e}')

    return found_resources, pointed

def remove_values_from_postgres_query(query):
    # Define a regular expression pattern to match various data types
    pattern = r"'([^']+)'(?:::([^\s,]+))?"  # Matches 'some_val' and 'some_val'::some_type

    # Replace the matched patterns with %s
    query = re.sub(pattern, lambda m: '%s', query)

    # Replace integers with %s
    query = re.sub(r'\b\d+\b', '%s', query)

    # Replace 'true' and 'false' with %s
    query = query.replace('true', '%s').replace('false', '%s')

    # Replace "VALUES (...)" and "VALUES(..." with %s
    query = re.sub(r'VALUES\s*\([^)]*\)?', '%s', query)

    # Replace NULL with %s
    query = query.replace('NULL', '%s')

    return query

def simplify_postgres_query(query):
    # If there exists a part in the format "val1"."val2", replace it with "val2"
    query = re.sub(r'"([^"]+)"\."([^"]+)"', r'\2', query)

    # Remove quotation marks from words
    query = re.sub(r'"([^"]+)"', r'\1', query)

    # Remove the trailing part if it started with a quote but it is not finished
    # Comment it for now, as it could delete okay trailing parts starting with a matched quote. For example -> description = 'asd' WHERE ...
    # query = re.sub(r"'[^']*?$", '', query)
    return query

def simplify_http_endpoint(endpoint):
    # Turn all query parameters to %s
    endpoint = re.sub(r'(\?|&)([^=]+)=([^&]+)', r'\1\2=%s', endpoint)

    # Aside from query parameters, turn all uids to %s
    endpoint = re.sub(r'(\/[a-f0-9-]{8}-[a-f0-9-]{4}-[a-f0-9-]{4}-[a-f0-9-]{4}-[a-f0-9-]{12}\/)', '/%s/', endpoint)
    
    # Aside from query parameters, turn all ints to %s
    endpoint = re.sub(r'\/(\d+)\/', '/%s/', endpoint)

    # Aside from query parameters, turn all jwt tokens to %s
    endpoint = re.sub(r'\/([a-zA-Z0-9-_=]+\.[a-zA-Z0-9-_=]+\.[a-zA-Z0-9-_=]+)\/', '/%s/', endpoint)

    return endpoint
    

def alaz_calculate_rps_latency(min_time, max_time, latencies):
    time_diff = (max_time - min_time).total_seconds() if max_time > min_time else 1
    for identify_key in latencies:
        edge_count = 0
        edge_sum_latency = 0
        for protocol in latencies[identify_key]['protocols']:
            protocol_count = 0
            protocol_sum_latency = 0
            for endpoint in latencies[identify_key]['protocols'][protocol]['paths']:
                endpoint_count = 0
                endpoint_sum_latency = 0
                for request_method in latencies[identify_key]['protocols'][protocol]['paths'][endpoint]['request_methods']:
                    if request_method == 'example_query':
                        continue
                    method_count = 0
                    method_sum_latency = 0
                    for status_code in latencies[identify_key]['protocols'][protocol]['paths'][endpoint]['request_methods'][request_method]['status_codes']:
                        count = latencies[identify_key]['protocols'][protocol]['paths'][endpoint]['request_methods'][request_method]['status_codes'][status_code]['count']
                        sum_latency = latencies[identify_key]['protocols'][protocol]['paths'][endpoint]['request_methods'][request_method]['status_codes'][status_code]['sum_latency']
                        rps = count / time_diff
                        avg_latency = sum_latency / count
                        method_count += count
                        method_sum_latency += sum_latency
                        latencies[identify_key]['protocols'][protocol]['paths'][endpoint]['request_methods'][request_method]['status_codes'][status_code]['rps'] = rps
                        latencies[identify_key]['protocols'][protocol]['paths'][endpoint]['request_methods'][request_method]['status_codes'][status_code]['avg_latency'] = avg_latency
                        del latencies[identify_key]['protocols'][protocol]['paths'][endpoint]['request_methods'][request_method]['status_codes'][status_code]['count']
                        del latencies[identify_key]['protocols'][protocol]['paths'][endpoint]['request_methods'][request_method]['status_codes'][status_code]['sum_latency']

                    method_rps = method_count / time_diff
                    method_avg_latency = method_sum_latency / method_count
                    endpoint_count += method_count
                    endpoint_sum_latency += method_sum_latency
                    latencies[identify_key]['protocols'][protocol]['paths'][endpoint]['request_methods'][request_method]['rps'] = method_rps
                    latencies[identify_key]['protocols'][protocol]['paths'][endpoint]['request_methods'][request_method]['avg_latency'] = method_avg_latency

                endpoint_rps = endpoint_count / time_diff
                endpoint_avg_latency = endpoint_sum_latency / endpoint_count
                protocol_count += endpoint_count
                protocol_sum_latency += endpoint_sum_latency
                latencies[identify_key]['protocols'][protocol]['paths'][endpoint]['rps'] = endpoint_rps
                latencies[identify_key]['protocols'][protocol]['paths'][endpoint]['avg_latency'] = endpoint_avg_latency

            protocol_rps = protocol_count / time_diff
            protocol_avg_latency = protocol_sum_latency / protocol_count
            edge_count += protocol_count
            edge_sum_latency += protocol_sum_latency
            latencies[identify_key]['protocols'][protocol]['rps'] = protocol_rps
            latencies[identify_key]['protocols'][protocol]['avg_latency'] = protocol_avg_latency

        edge_rps = edge_count / time_diff
        edge_avg_latency = edge_sum_latency / edge_count
        latencies[identify_key]['rps'] = edge_rps
        latencies[identify_key]['avg_latency'] = edge_avg_latency

    return latencies

def alaz_find_services_pointing_to_pod(pod, cached_services, cached_endpoints, endpoint_to_service):
    services = {}
    for _, endpoints in cached_endpoints.items():
        for endpoint in endpoints:
            for address in endpoint.addresses:
                for ip in address['ips']:
                    if ip['type'] == 'Pod' and uuid.UUID(ip['id']) == pod.uid:
                        service = endpoint_to_service.get(endpoint.uid, None)
                        if service is None:
                            if settings.VERBOSE_LOG:
                                print(f'The endpoint with uid {endpoint.uid} does not have a matching service')
                                continue
                        service.uid = str(service.uid)
                        services[service.uid] = service

    return list(services.values())

def alaz_process_node_cpu_seconds_total_data(data):

    if 'data' in data and 'result' in data['data'] and type(data['data']['result']) is list and len(data['data']['result']) > 0 and 'values' in data['data']['result'][0]:
        return data['data']['result'][0]['values']

    return []

def prometheus_validate_single_value(metric, metric_data, fields):
    result = {}
    if 'data' in metric_data and 'result' in metric_data['data'] and type(metric_data['data']['result']) is list and len(metric_data['data']['result']) > 0:
        for individual_metric in metric_data['data']['result']:
            if 'value' not in individual_metric or 'metric' not in individual_metric:
                return False, None
            metric_info = individual_metric['metric']
            value = individual_metric['value']
            field_vals = [metric]
            for field in fields:
                if field in metric_info:
                    field_vals.append(metric_info[field])
            result[f'{"_".join(field_vals)}'] = value
        return True, result
    return False, None

def alaz_process_prometheus_data(all_data, valid_until=None):

    def validate_single_value(metric, metric_data, fields):
        result = {}
        if 'data' in metric_data and 'result' in metric_data['data'] and type(metric_data['data']['result']) is list and len(metric_data['data']['result']) > 0:
            for individual_metric in metric_data['data']['result']:
                if 'value' not in individual_metric or 'metric' not in individual_metric:
                    return False, None
                metric_info = individual_metric['metric']
                value = individual_metric['value']
                field_vals = [metric]
                for field in fields:
                    if field in metric_info:
                        field_vals.append(metric_info[field])
                result[f'{"_".join(field_vals)}'] = value
            return True, result
        return False, None

    def validate_multiple_values(metric, metric_data, fields):
        result = {}
        if 'data' in metric_data and 'result' in metric_data['data'] and type(metric_data['data']['result']) is list and len(metric_data['data']['result']) > 0:
            for individual_metric in metric_data['data']['result']:
                if 'metric' not in individual_metric or 'values' not in individual_metric:
                    return False, None
                metric_info = individual_metric['metric']
                values = individual_metric['values']
                field_vals = []
                for field in fields:
                    if field in metric_info:
                        field_vals.append(metric_info[field])
                merged_key = f'{"_".join(field_vals)}'
                if merged_key == '':
                    # This should not happen normally, but if it happens, carry on normally
                    result[metric] = values
                else:
                    result[merged_key] = {}
                    result[merged_key][metric] = values
            return True, result
        return False, None

    def process_value(metric_value):
        timestamp = float(metric_value[0])
        timestamp = int(timestamp * 1000)  # Convert to ms
        if metric_value[1] != 'NaN':
            value = float(metric_value[1])
        else:
            value = metric_value[1]
        return timestamp, value

    result = {}
    for data_group, data in all_data.items():
        nested = False
        if 'data' not in data:
            # Returned as list of objects
            data_group_formatted_result = {}
            for metric, metric_data in data.items():
                fields, data = metric_data['fields'], metric_data['data']
                is_single_value, single_value = validate_single_value(metric, data, fields)
                is_multiple_values, multiple_values = validate_multiple_values(metric, data, fields)
                if is_single_value:
                    # This shouldn't happen
                    print(f'For {data_group}-{metric} expected multiple metric data contains single metric: {single_value}')
                elif is_multiple_values:
                    for key, values in multiple_values.items():
                        if type(values) is list:
                            nested = False
                            for value in values:
                                timestamp, value = process_value(value)
                                timestamp_date = datetime.datetime.fromtimestamp(timestamp / 1000, datetime.UTC)
                                if timestamp not in data_group_formatted_result:
                                    data_group_formatted_result[timestamp] = {}
                                if valid_until and timestamp_date > valid_until:
                                    data_group_formatted_result[timestamp][key] = 0
                                else:
                                    data_group_formatted_result[timestamp][key] = value
                        elif type(values) is dict:
                            nested = True
                            for metric, values in values.items():
                                for value in values:
                                    timestamp, value = process_value(value)
                                    timestamp_date = datetime.datetime.fromtimestamp(timestamp / 1000, datetime.UTC)
                                    if timestamp not in data_group_formatted_result:
                                        data_group_formatted_result[timestamp] = {}
                                    if key not in data_group_formatted_result[timestamp]:
                                        data_group_formatted_result[timestamp][key] = {}
                                        
                                    if valid_until and timestamp_date > valid_until:
                                        data_group_formatted_result[timestamp][key][metric] = 0
                                    else:
                                        data_group_formatted_result[timestamp][key][metric] = value
                        else:
                            pass
                            # print(f'Could not process metric data due to misformatting in the helper: {metric_data}')
                else:
                    pass
                    # print(f'Could not process metric data: {metric_data}')
            for timestamp, metric_data in data_group_formatted_result.items():
                if nested:
                    for tab, data in metric_data.items():
                        data['timestamp'] = timestamp
                        if data_group not in result:
                            result[data_group] = {}
                        if tab not in result[data_group]:
                            result[data_group][tab] = []
                        result[data_group][tab].append(data)
                else:
                    metric_data['timestamp'] = timestamp
                    if data_group not in result:
                        result[data_group] = []
                    result[data_group].append(metric_data)
        else:
            fields, data = data['fields'], data['data']
            # Returned as a single object
            result[data_group] = {}
            is_single_value, single_value = validate_single_value(data_group, data, fields)
            if is_single_value:
                # Normally, there should be a single item in this dict
                # If not, log it
                if len(single_value) > 1:
                    print(f'For {data_group} singular metric data contains multiple metrics: {single_value}')
                for _, value in single_value.items():
                    timestamp, value = process_value(value)
                    timestamp_date = datetime.datetime.fromtimestamp(timestamp / 1000, datetime.UTC)
                    if valid_until and timestamp_date > valid_until:
                        result[data_group] = 0
                    else:
                        result[data_group][timestamp] = value
            else:
                pass
                # print(f'Could not process metric data: {data}')

    return result

def get_instances_of_cluster(cluster, start_time=None):
    instances = []
    if not start_time:
        # Get the active instances
        for instance in cluster.last_data_ts['active']:
            instances.append(instance)
    else:
        # Get the instances with the given time interval
        if not cluster.instances:
            return instances

        for instance, time in cluster.instances.items():
            if time < int(start_time):  # in ms
                continue
            instances.append(instance)

    return instances

def get_total_instances_of_user(user):
    user = find_team_owner_if_exists(user)
    active_instances = []
    passive_instances = []
    clusters = get_clusters_and_cluster_count(user)
    for cluster in clusters:
        active_instances.extend(cluster.last_data_ts['active'].keys())
        passive_instances.extend(cluster.last_data_ts['passive'].keys())
    return active_instances, passive_instances

def check_user_instance_count(cluster, instance, type):
    user = cluster.user
    owner = find_team_owner_if_exists(user)
    clusters = get_clusters_and_cluster_count(owner, primary=True)

    with transaction.atomic():
        count = 0
        instance_is_new = False
        for cluster_to_check in clusters:
            instances = get_instances_of_cluster(cluster_to_check)
            if cluster_to_check.uid == cluster.uid and instance not in instances:
                instance_is_new = True
                count += 1
            count += len(instances)

        max_count = get_instance_limit_of_user(owner)
        if instance_is_new and count > int(max_count):
            if type == 'healthcheck':
                now = int(datetime.datetime.now(datetime.UTC).timestamp() * 1000)
                cluster.last_data_ts['passive'][instance] = {type: now}
                save_cluster(cluster)
            return False
        else:
            # TODO: Add instance activation here
            return True


def validate_service_map_get_method(request):

    monitoring_id = request.query_params.get('monitoring_id', None)
    if monitoring_id is None:
        raise exceptions.InvalidRequestError({'msg': "Monitoring id is required"})

    cluster_qs = Cluster.objects.filter(monitoring_id=monitoring_id)
    if not cluster_qs.exists():
        raise exceptions.InvalidRequestError({'msg': "Cluster for the monitoring id not found"})

    cluster = cluster_qs.first()

    start_time = request.query_params.get('start_time', None)  # in ms
    end_time = request.query_params.get('end_time', None)  # in ms
    if start_time is None or end_time is None:
        raise exceptions.InvalidRequestError({'msg': "Start time and end time are required"})

    start_time = int(start_time)
    end_time = int(end_time)

    if end_time < start_time:
        raise exceptions.InvalidRequestError({'msg': "End time cannot be less than start time"})

    if end_time - start_time > 7200000:
        raise exceptions.InvalidRequestError({'msg': "Time difference cannot be greater than 2 hours"})

    return cluster, start_time, end_time

    
def validate_metrics_get_method(request):

    monitoring_id = request.query_params.get('monitoring_id', None)
    if monitoring_id is None:
        raise exceptions.InvalidRequestError({'msg': "Monitoring id is required"})

    cluster_qs = Cluster.objects.filter(monitoring_id=monitoring_id)
    if not cluster_qs.exists():
        raise exceptions.InvalidRequestError({'msg': "Cluster for the monitoring id not found"})

    cluster = cluster_qs.first()

    start_time = request.query_params.get('start_time', None)  # in ms
    end_time = request.query_params.get('end_time', None)  # in ms
    if start_time is None or end_time is None:
        raise exceptions.InvalidRequestError({'msg': "Start time and end time are required"})

    start_time = int(start_time)
    end_time = int(end_time)

    if end_time < start_time:
        raise exceptions.InvalidRequestError({'msg': "End time cannot be less than start time"})

    if end_time - start_time > 24 * 3600000:
        raise exceptions.InvalidRequestError({'msg': "Time difference cannot be greater than 24 hours"})

    return cluster, start_time, end_time

def cache_alaz_resources(cluster, namespaces):

    start_time = datetime.datetime.now()

    if namespaces is None:
        pods_qs = Pod.objects.filter(cluster=cluster.uid, deleted=False)
        replica_sets_qs = ReplicaSet.objects.filter(cluster=cluster.uid, deleted=False)
        deployments_qs = Deployment.objects.filter(cluster=cluster.uid, deleted=False)
        endpoints_qs = Endpoint.objects.filter(cluster=cluster.uid, deleted=False)
        services_qs = Service.objects.filter(cluster=cluster.uid, deleted=False)
        daemon_sets_qs = DaemonSet.objects.filter(cluster=cluster.uid, deleted=False)
        # containers_qs = Container.objects.filter(cluster=cluster.uid)
        stateful_sets_qs = StatefulSet.objects.filter(cluster=cluster.uid, deleted=False)
    else:
        pods_qs = Pod.objects.filter(cluster=cluster.uid, namespace__in=namespaces, deleted=False)
        replica_sets_qs = ReplicaSet.objects.filter(cluster=cluster.uid, namespace__in=namespaces, deleted=False)
        deployments_qs = Deployment.objects.filter(cluster=cluster.uid, namespace__in=namespaces, deleted=False)
        endpoints_qs = Endpoint.objects.filter(cluster=cluster.uid, namespace__in=namespaces, deleted=False)
        services_qs = Service.objects.filter(cluster=cluster.uid, namespace__in=namespaces, deleted=False)
        daemon_sets_qs = DaemonSet.objects.filter(cluster=cluster.uid, namespace__in=namespaces, deleted=False)
        # containers_qs = Container.objects.filter(cluster=cluster.uid, namespace__in=namespaces)
        stateful_sets_qs = StatefulSet.objects.filter(cluster=cluster.uid, namespace__in=namespaces, deleted=False)
        
    verbose_log(f'Cache queryset preparation time: {datetime.datetime.now() - start_time}')

    cached_replica_sets = dict()
    cached_deployments = dict()
    cached_pods = dict()
    cached_endpoints = dict()
    cached_services = dict()
    cached_daemon_sets = dict()
    cached_stateful_sets = dict()
    endpoint_to_service = dict()
    services_with_name_namespace = dict()

    start_time = datetime.datetime.now()
    
    for service in services_qs.iterator():
        service.connected_resources = {'incoming': [], 'outgoing': []}
        service.used_ports = []
        cached_services[service.uid] = service
        services_with_name_namespace[(service.name, service.namespace)] = service
    
    verbose_log(f'Cache services time: {datetime.datetime.now() - start_time}')
    start_time = datetime.datetime.now()

    for deployment in deployments_qs.iterator():
        deployment.connected_resources = {'incoming': [], 'outgoing': []}
        deployment.replica_sets = {}
        cached_deployments[deployment.uid] = deployment
        
    verbose_log(f'Cache deployments time: {datetime.datetime.now() - start_time}')
    start_time = datetime.datetime.now()

    for daemon_set in daemon_sets_qs.iterator():
        daemon_set.connected_resources = {'incoming': [], 'outgoing': []}
        daemon_set.pods = {}
        cached_daemon_sets[daemon_set.uid] = daemon_set
        
    verbose_log(f'Cache daemon sets time: {datetime.datetime.now() - start_time}')
    start_time = datetime.datetime.now()

    for stateful_set in stateful_sets_qs.iterator():
        stateful_set.connected_resources = {'incoming': [], 'outgoing': []}
        stateful_set.pods = {}
        cached_stateful_sets[stateful_set.uid] = stateful_set

    for replica_set in replica_sets_qs.iterator():
        replica_set.pods = {}
        if replica_set.owner and replica_set.owner in cached_deployments:
            cached_deployments[replica_set.owner].replica_sets[replica_set.uid] = replica_set
        cached_replica_sets[replica_set.uid] = replica_set
        
    verbose_log(f'Cache replica sets time: {datetime.datetime.now() - start_time}')
    start_time = datetime.datetime.now()

    # TODO: Store the owner as the other resources in the future
    for endpoint in endpoints_qs:
        endpoint_name = endpoint.name
        endpoint_namespace = endpoint.namespace
        service = services_with_name_namespace.get((endpoint_name, endpoint_namespace), None)
        if service is None:
            if settings.VERBOSE_LOG:
                print(f'Endpoint {endpoint.uid} does not have a corresponding service')
            continue
        if service.uid not in cached_endpoints:
            cached_endpoints[service.uid] = []
        cached_endpoints[service.uid].append(endpoint)
        endpoint_to_service[endpoint.uid] = service
        
    verbose_log(f'Cache endpoints time: {datetime.datetime.now() - start_time}')
    start_time = datetime.datetime.now()

    for pod in pods_qs.iterator():
        pod.connected_resources = {'incoming': [], 'outgoing': []}
        pod.containers = {}
        if pod.replicaset_owner and pod.replicaset_owner in cached_replica_sets:
            cached_replica_sets[pod.replicaset_owner].pods[pod.uid] = pod
        if pod.daemonset_owner and pod.daemonset_owner in cached_daemon_sets:
            cached_daemon_sets[pod.daemonset_owner].pods[pod.uid] = pod
        if pod.statefulset_owner and pod.statefulset_owner in cached_stateful_sets:
            cached_stateful_sets[pod.statefulset_owner].pods[pod.uid] = pod
        cached_pods[pod.uid] = pod

    verbose_log(f'Cached pods: {cached_pods}')
        
    verbose_log(f'Cache pods time: {datetime.datetime.now() - start_time}')
    start_time = datetime.datetime.now()

    cached_outbounds = dict()

    # for container in containers_qs.iterator():
    #     if container.pod and container.pod in cached_pods:
    #         cached_pods[container.pod].containers[container.uid] = container
            
    verbose_log(f'Cache containers time: {datetime.datetime.now() - start_time}')
    
    return cached_replica_sets, cached_deployments, cached_pods, cached_endpoints, cached_services, cached_daemon_sets, cached_outbounds, endpoint_to_service, cached_stateful_sets

def get_pods_of_daemonset(daemonset, context):
    if 'cached_daemon_sets' in context and daemonset.uid in context['cached_daemon_sets']:
        return context['cached_daemon_sets'][daemonset.uid].pods.values()
    else:
        return Pod.objects.filter(daemonset_owner=daemonset.uid, cluster=daemonset.cluster, namespace=daemonset.namespace)

def get_containers_of_pod(pod, context):
    # return []
    if 'cached_pods' in context and pod.uid in context['cached_pods']:
        return context['cached_pods'][pod.uid].containers.values()
    else:
        return Container.objects.filter(pod=pod.uid, namespace=pod.namespace, cluster=pod.cluster)

# TODO: Do this as the other ones in the future
def get_endpoints_of_service(service, context):
    if 'cached_endpoints' in context and service.uid in context['cached_endpoints']:
        return context['cached_endpoints'][service.uid]
    else:
        return Endpoint.objects.filter(name=service.name, namespace=service.namespace, cluster=service.cluster, deleted=False).order_by('-date_created').first()

def get_replicasets_of_deployment(deployment, context):
    if 'cached_deployments' in context and deployment.uid in context['cached_deployments']:
        return context['cached_deployments'][deployment.uid].replica_sets.values()
    else:
        return ReplicaSet.objects.filter(owner=deployment.uid, namespace=deployment.namespace, cluster=deployment.cluster)

def get_pods_of_replicaset(replicaset, context):
    if 'cached_replica_sets' in context and replicaset.uid in context['cached_replica_sets']:
        return context['cached_replica_sets'][replicaset.uid].pods.values()
    else:
        return Pod.objects.filter(replicaset_owner=replicaset.uid, namespace=replicaset.namespace, cluster=replicaset.cluster)

def validate_alaz_post_method(request, method_type):
    # TODO: Separate this for healthcheck, do the cluster check only for healthcheck
    data = request.data
    if "metadata" not in data:
        raise exceptions.InvalidRequestError({'msg': "Metadata is required"})

    if "monitoring_id" not in data["metadata"]:
        raise exceptions.InvalidRequestError({'msg': "Monitoring id in the metadata is required"})

    monitoring_id = data["metadata"]["monitoring_id"]
    # Directly connect to the master db to prevent writing-reading lag
    try:
        # TODO: Check cluster existence from memory. If not found, check from redis
        # TODO: Remove if cluster is deleted
        cluster = Cluster.objects.using('default').prefetch_related('user').get(monitoring_id=monitoring_id)
    except Exception as e:
        logger.warn(f"Cluster for the monitoring id is not found: {monitoring_id} - {e}")
        raise exceptions.ValidationError({"msg": f"Cluster for the monitoring id is not found: {monitoring_id}"})

    # TODO: Do not set last data ts
    if len(cluster.last_data_ts['active']) > 0:
        instance = list(cluster.last_data_ts['active'].keys())[0]
    elif len(cluster.instances) > 0:
        instance = list(cluster.instances.keys())[0]
    else:
        instance = 'default_instance'

    if "idempotency_key" not in data["metadata"]:
        raise exceptions.InvalidRequestError({'msg': "Idempotency key in the metadata is required"})
    idempotency_key = data["metadata"]["idempotency_key"]

    # if 'alaz_version' not in data['metadata']:
        # raise exceptions.InvalidRequestError({'msg': "Alaz version in the metadata is required"})
    alaz_version = data['metadata'].get('alaz_version', None)
    
    # if 'node_id' not in data['metadata']:
        # raise exceptions.InvalidRequestError({'msg': "Node id in the metadata is required"})
    if 'node_id' in data['metadata']:
        instance = data['metadata']['node_id']
    
    if method_type == 'events':
        if 'events' not in data or type(data['events']) != list:
            raise exceptions.InvalidRequestError({'msg': "Events are required"})
    elif method_type == 'requests':
        if 'requests' not in data or type(data['requests']) != list:
            raise exceptions.InvalidRequestError({'msg': "Requests are required"})
    elif method_type == 'traffic':
        if 'traffic' not in data or type(data['traffic']) != list:
            raise exceptions.InvalidRequestError({'msg': "Traffic is required"})
    elif method_type == 'kafka_events':
        if 'kafka_events' not in data or type(data['kafka_events']) != list:
            raise exceptions.InvalidRequestError({'msg': "Kafka events are required"})
    elif method_type == 'connections':
        if 'connections' not in data or type(data['connections']) != list:
            raise exceptions.InvalidRequestError({'msg': "Connections are required"})
    elif method_type == 'logs':
        if 'logs' not in data or type(data['logs']) != list:
            raise exceptions.InvalidRequestError({'msg': "Logs are required"})

    return cluster, idempotency_key, alaz_version, instance


def lightweight_validate_alaz_post(request, endpoint_type='requests'):
    # TODO: Separate this for healthcheck, do the cluster check only for healthcheck
    data = request.data
    if "metadata" not in data:
        raise exceptions.InvalidRequestError({'msg': "Metadata is required"})

    if "monitoring_id" not in data["metadata"]:
        raise exceptions.InvalidRequestError({'msg': "Monitoring id in the metadata is required"})

    monitoring_id = data["metadata"]["monitoring_id"]
    if monitoring_id in clusters:
        cluster_uid = clusters[monitoring_id]
    else:
        try:
            cluster = Cluster.objects.using('default').prefetch_related('user').get(monitoring_id=monitoring_id)
            cluster_uid = str(cluster.uid)
            clusters[monitoring_id] = cluster_uid
        except Exception as e:
            logger.warn(f"Cluster for the monitoring id is not found: {monitoring_id} - {e}")
            raise exceptions.ValidationError({"msg": f"Cluster for the monitoring id is not found: {monitoring_id}"})

    # TODO: Do not set last data ts
    # if len(cluster.last_data_ts['active']) > 0:
        # instance = list(cluster.last_data_ts['active'].keys())[0]
    # elif len(cluster.instances) > 0:
        # instance = list(cluster.instances.keys())[0]
    # else:
        # instance = 'default_instance'

    if "idempotency_key" not in data["metadata"]:
        raise exceptions.InvalidRequestError({'msg': "Idempotency key in the metadata is required"})
    idempotency_key = data["metadata"]["idempotency_key"]

    # if 'alaz_version' not in data['metadata']:
        # raise exceptions.InvalidRequestError({'msg': "Alaz version in the metadata is required"})
    alaz_version = data['metadata'].get('alaz_version', None)
    
    # if 'node_id' not in data['metadata']:
        # raise exceptions.InvalidRequestError({'msg': "Node id in the metadata is required"})
    if 'node_id' in data['metadata']:
        instance = data['metadata']['node_id']
    
    if endpoint_type not in data or type(data[endpoint_type]) != list:
        raise exceptions.InvalidRequestError({'msg': f"{endpoint_type} are required"})

    return cluster_uid, idempotency_key, alaz_version, instance



def log_endpoint(view):

    def wrapper(*args, **kwargs):
        try:
            # print(f'Processing event {args[1].data}')
            result = view(*args, **kwargs)
            # print(f'Processed event {args[1].data}')
            return result
        except Exception as e:
            print(f'Failed to process event {args[1].data}, \nexception = {e}')
            raise
    return wrapper


def alaz_find_all_namespaces(cluster):
    namespaces = set()
    pod_namespaces = Pod.objects.using('default').filter(cluster=cluster.uid).values_list('namespace', flat=True).distinct()
    deployment_namespaces = Deployment.objects.using('default').filter(cluster=cluster.uid).values_list('namespace', flat=True).distinct()
    daemonset_namespaces = DaemonSet.objects.using('default').filter(cluster=cluster.uid).values_list('namespace', flat=True).distinct()
    replicaset_namespaces = ReplicaSet.objects.using('default').filter(cluster=cluster.uid).values_list('namespace', flat=True).distinct()
    service_namespaces = Service.objects.using('default').filter(cluster=cluster.uid).values_list('namespace', flat=True).distinct()
    endpoint_namespaces = Endpoint.objects.using('default').filter(cluster=cluster.uid).values_list('namespace', flat=True).distinct()
    stateful_set_namespaces = StatefulSet.objects.using('default').filter(cluster=cluster.uid).values_list('namespace', flat=True).distinct()

    namespaces.update(pod_namespaces)
    namespaces.update(deployment_namespaces)
    namespaces.update(daemonset_namespaces)
    namespaces.update(replicaset_namespaces)
    namespaces.update(service_namespaces)
    namespaces.update(endpoint_namespaces)
    namespaces.update(stateful_set_namespaces)

    return list(namespaces)


def alaz_apply_namespace_filter_to_request(namespaces, from_object, to_object, pointed_destinations):
    if namespaces is None:
        return True
    if from_object.namespace not in namespaces and to_object.namespace not in namespaces:
        found = False
        if pointed_destinations is None:
            return False
        for pointed_destination_uid, pointed_destination in pointed_destinations.items():
            if pointed_destination['type'] == 'pod':
                pod = Pod.objects.get(uid=pointed_destination_uid)
                if pod.namespace in namespaces:
                    found = True
                    break
            elif pointed_destination['type'] == 'deployment':
                deployment = Deployment.objects.get(uid=pointed_destination_uid)
                if deployment.namespace in namespaces:
                    found = True
                    break
        if not found:
            return False
    return True


def alaz_add_connected_resources_if_any(resources, edges):
    for edge in edges.values():
        from_type = edge['from_type']
        from_uid = edge['from_id']
        to_type = edge['to_type']
        to_uid = edge['to_id']
        pointed = edge.get('pointed', None)

        # Add incoming for to
        incoming_resource = {
            "type": from_type,
            "uid": from_uid,
        }
        try:
            if incoming_resource not in resources[to_type + 's'][to_uid].connected_resources['incoming']:
                resources[to_type + 's'][to_uid].connected_resources['incoming'].append(incoming_resource)
        except Exception as e:
            if settings.VERBOSE_LOG:
                logger.warn(f'Could not add incoming resource {incoming_resource["type"]} {incoming_resource["uid"]} for {to_type} {to_uid} because {e}')

        # Add outgoing for from
        outgoing_resource = {
            "type": to_type,
            "uid": to_uid,
        }
        try:
            if outgoing_resource not in resources[from_type + 's'][from_uid].connected_resources['outgoing']:
                resources[from_type + 's'][from_uid].connected_resources['outgoing'].append(outgoing_resource)
        except Exception as e:
            if settings.VERBOSE_LOG:
                logger.warn(f'Could not add outgoing resource for {outgoing_resource["type"]} {outgoing_resource["uid"]} {from_type} {from_uid} because {e}')

        # Add incoming and outgoing for pointed, if it exists
        if not pointed:
            continue

        for pointed_resource_uid, pointed_resource in pointed.items():
            type = pointed_resource['type']
            if type == 'external':
                # TODO: Implement this in the future
                continue
            outgoing_pointed_payload = {
                "type": type,
                "uid": pointed_resource_uid,
            }
            # It's not possible for to_type to be outbound here
            try:
                if outgoing_pointed_payload not in resources[to_type + 's'][to_uid].connected_resources['outgoing']:
                    resources[to_type + 's'][to_uid].connected_resources['outgoing'].append(outgoing_pointed_payload)
            except Exception as e:
                if settings.VERBOSE_LOG:
                    logger.warn(f'Could not add outgoing pointed resource {outgoing_pointed_payload["type"]} {outgoing_pointed_payload["uid"]} for {to_type} {to_uid} because {e}')

            incoming_pointed_payload = {
                "type": to_type,
                "uid": to_uid,
            }
            try:
                if incoming_pointed_payload not in resources[type + 's'][pointed_resource_uid].connected_resources['incoming']:
                    resources[type +
                              's'][pointed_resource_uid].connected_resources['incoming'].append(incoming_pointed_payload)
            except Exception as e:
                if settings.VERBOSE_LOG:
                    logger.warn(f'Could not add incoming pointed resource {incoming_pointed_payload["type"]} {incoming_pointed_payload["uid"]} for {type} {pointed_resource_uid} because {e} could not be found')

    return resources

def get_last_data_ts_of_user(user):
    if settings.ANTEON_ENV == 'onprem':
        clusters = Cluster.objects.all()
    else:
        if user.team:
            clusters = Cluster.objects.filter(team=user.team)
            owners_clusters = Cluster.objects.filter(user=user.team.owner)
            clusters.union(owners_clusters)
        else:
            clusters = Cluster.objects.filter(user=user, team=None)

    last_data_ts = None
    last_data_ts_cluster = None
    for cluster in clusters:
        last_data_ts_of_cluster = None
        for _, node_details in cluster.last_data_ts['active'].items():
            for _, mode_timestamp in node_details.items():
                if last_data_ts_of_cluster is None or last_data_ts_of_cluster < mode_timestamp:
                    last_data_ts_of_cluster = mode_timestamp
        if last_data_ts_of_cluster is None:
            continue
        if last_data_ts is None or last_data_ts < last_data_ts_of_cluster:
            last_data_ts = last_data_ts_of_cluster
            last_data_ts_cluster = cluster

    last_data_ts_cluster_monitoring_id = last_data_ts_cluster.monitoring_id if last_data_ts_cluster else None
    return last_data_ts, last_data_ts_cluster_monitoring_id
    

def alaz_set_instance_as_active(cluster, instance, type):
    now = int(datetime.datetime.now(datetime.UTC).timestamp() * 1000)
    if instance not in cluster.last_data_ts['active']:
        if instance in cluster.last_data_ts['passive']:
            data = cluster.last_data_ts['passive'][instance]
            del cluster.last_data_ts['passive'][instance]
        else:
            data = {}
        cluster.last_data_ts['active'][instance] = data
    cluster.last_data_ts['active'][instance][type] = now
    return cluster

def alaz_keep_count_of_protocol(src_type, src_obj, dest_type, dest_obj, protocol, counts, pointed, count):
    src_uid = str(src_obj.uid)
    dest_uid = str(dest_obj.uid)

    # TODO: Implement this later
    # if protocol.lower() == 'amqp' and method.lower() == 'deliver':
    #     if src_uid not in counts[src_type + 's']:
    #         counts[src_type + 's'][src_uid] = {}
    #     counts[src_type + 's'][src_uid][protocol] = count

        # if protocol not in counts[src_type + 's'][src_uid]:
            # counts[src_type + 's'][src_uid][protocol] = 0
        # counts[src_type + 's'][src_uid][protocol] += 1

    # else:

    if dest_uid not in counts[dest_type + 's']:
        counts[dest_type + 's'][dest_uid] = {}
    counts[dest_type + 's'][dest_uid][protocol] = count
    # if protocol not in counts[dest_type + 's'][dest_uid]:
        # counts[dest_type + 's'][dest_uid][protocol] = 0
    # counts[dest_type + 's'][dest_uid][protocol] += 1

    if pointed is None:
        return counts

    for pointed_uid, pointed_obj in pointed.items():
        pointed_type = pointed_obj['type']
        if pointed_type == 'external':
            continue
        if pointed_uid not in counts[pointed_type + 's']:
            counts[pointed_type + 's'][pointed_uid] = {}
        counts[pointed_type + 's'][pointed_uid][protocol] = count
        # if protocol not in counts[pointed_type + 's'][pointed_uid]:
            # counts[pointed_type + 's'][pointed_uid][protocol] = 0
        # counts[pointed_type + 's'][pointed_uid][protocol] += 1

    return counts

def alaz_assign_most_frequent_protocol_to_resources(resources, counts):
    for type in resources:
        for uid, resource in resources[type].items():
            if uid not in counts[type] or len(counts[type][uid]) == 0:
                resource.protocol = ''
                continue
            max_count = 0
            max_protocol = ''
            for protocol, count in counts[type][uid].items():
                if count > max_count:
                    max_count = count
                    max_protocol = protocol
            resource.protocol = max_protocol
    
    return resources

def alaz_mark_zombie_resources(resources):
    for _, type_resources in resources.items():
        for _, resource in type_resources.items():
            if len(resource.connected_resources['incoming']) == 0 and len(resource.connected_resources['outgoing']) == 0:
                resource.is_zombie = True
            else:
                resource.is_zombie = False

    return resources

def get_clusters_and_cluster_count(user, primary=False):
    if settings.ANTEON_ENV == 'onprem':
        clusters = Cluster.objects.all().order_by('-date_created')
    else:
        if user.is_admin:
            if primary:
                clusters = Cluster.objects.using('default').all().order_by('-date_created')
            else:
                clusters = Cluster.objects.all().order_by('-date_created')
        else:
            if user.team:
                if primary:
                    clusters = Cluster.objects.using('default').filter(team=user.team).order_by('-date_created')
                    owners_clusters = Cluster.objects.using('default').filter(user=user.team.owner).order_by('-date_created')
                    clusters = clusters.union(owners_clusters)
                else:
                    clusters = Cluster.objects.filter(team=user.team).order_by('-date_created')
                    owners_clusters = Cluster.objects.filter(user=user.team.owner).order_by('-date_created')
                    clusters = clusters.union(owners_clusters)
            else:
                if primary:
                    clusters = Cluster.objects.using('default').filter(user=user, team=None).order_by('-date_created')
                else:
                    clusters = Cluster.objects.filter(user=user, team=None).order_by('-date_created')

    return clusters


def check_monitoring_read_permission(user, cluster):
    if settings.ANTEON_ENV == 'onprem':
        # Assuming that the user is already authenticated (if enterprise)
        pass
    else:
        if user.is_admin:
            return
        if user.team and not cluster.team and user.team.owner != cluster.user:
            raise exceptions.PermissionDenied({'msg': "You do not have access to this monitoring resource"})
        if not user.team and cluster.team and user != cluster.user:
            raise exceptions.PermissionDenied({'msg': "You do not have access to this monitoring resource"})
        # From now on, either both the user and the cluster have teams or they both dont have any
        if user.team and cluster.team and user.team != cluster.team:
            raise exceptions.PermissionDenied({'msg': "You do not have access to this monitoring resource"})
        if not user.team and not cluster.team and user != cluster.user:
            raise exceptions.PermissionDenied({'msg': "You do not have access to this monitoring resource"})


def check_monitoring_write_permission(user, cluster=None):
    if settings.ANTEON_ENV == 'onprem':
        root_user = get_user_model().objects.get(email=settings.ROOT_EMAIL)
        if root_user.self_hosted_is_enterprise_unlocked and user != root_user:
            raise exceptions.PermissionDenied({'msg': "Only the root user can manage monitoring on enterprise selfhosted"})
    else:
        if user.team and user.team.owner != user:
            raise exceptions.PermissionDenied({'msg': "Only the team owner can manage monitoring"})
        if cluster:
            # Check for an update
            # if cluster = None, check for a creation
            if user.team and not cluster.team:
                raise exceptions.PermissionDenied({'msg': "You do not have access to this monitoring resource"})
            if not user.team and cluster.team:
                raise exceptions.PermissionDenied({'msg': "You do not have access to this monitoring resource"})
            # From now on, either both the user and the cluster have teams or they both dont have any
            if user.team and cluster.team and user.team != cluster.team:
                raise exceptions.PermissionDenied({'msg': "You do not have access to this monitoring resource"})
            if not user.team and not cluster.team and user != cluster.user:
                raise exceptions.PermissionDenied({'msg': "You do not have access to this monitoring resource"})

def silk_wrapper(name):
    def _decorator(view_function):
        def _wrapper(request, *args, **kwargs):
            if settings.SILK_ENABLED:
                # Apply the decorator to the view function
                from silk.profiling.profiler import silk_profile
                decorated_view = silk_profile(name)(view_function)
                return decorated_view(request, *args, **kwargs)
            else:
                # If the condition is not met, use the original view function
                return view_function(request, *args, **kwargs)

        return _wrapper

    return _decorator

def limit_active_instances_of_user(user):
    if user.team and user.team.owner != user:
        return 

    clusters = get_clusters_and_cluster_count(user, primary=True)
    limit = get_instance_limit_of_user(user)

    instance_count = 0
    for cluster in clusters:
        for instance in cluster.last_data_ts['active']:
            instance_count += 1

    drop_count = instance_count - limit
    if drop_count <= 0:
        return

    finished = False    
    for cluster in clusters:
        to_delete = []
        for instance in cluster.last_data_ts['active']:
            if drop_count <= 0:
                finished = True
                break

            to_delete.append((instance, cluster.last_data_ts['active'][instance]))
            drop_count -= 1
        
        for instance, data in to_delete:
            del cluster.last_data_ts['active'][instance]
            cluster.last_data_ts['passive'][instance] = data
        
        save_cluster(cluster)
            
        if finished:
            break


def prometheus_get_singular_metric(metric):
    if 'data' in metric and 'result' in metric['data'] and type(metric['data']['result']) is list and len(metric['data']['result']) > 0:
        # Get all values of metrics
        result = []
        for individual_metric in metric['data']['result']:
            if 'value' not in individual_metric:
                continue
            value = individual_metric['value']
            result.append(value)
        return result

    return None


def get_instance_week_metrics(cluster, instance):
    requester = PrometheusRequester()
    metrics = requester.get_current_summary_metrics(instance, '7d', cluster.monitoring_id, 'alaz')
    result = {}
    for metric_key, metric_val in metrics.items():
        datas = prometheus_get_singular_metric(metric_val)
        # Normally there should only be one element. But if not, it is safer to take the average
        if not datas:
            continue
        avg = 0
        count = 0
        for data in datas:
            avg += float(data[1])
            count += 1
        if count > 0:
            avg /= count
        result[metric_key] = avg
    
    return result


def string_is_ascii(arg):
    for char in arg:
        if ord(char) > 127:
            return False

    return True


def get_instance_limit_of_user(user):
    if settings.ANTEON_ENV == 'onprem':
        root_user = User.objects.get(email=settings.ROOT_EMAIL)
        is_enterprise = root_user.selfhosted_enterprise
        if is_enterprise:
            max_count = settings.ENTERPRISE_INSTANCES_LIMIT
        else:
            max_count = settings.COMMUNITY_INSTANCES_LIMIT
    else:
        owner = find_team_owner_if_exists(user)
        max_count = owner.plan['monitoring_instances']

    return max_count

def revert_group_if_required(group, heuristics=False):
    from_port = group['from_port']
    to_port = group['to_port']
    from_uid_pod = group['from_uid_pod']
    from_uid_service = group['from_uid_service']
    from_uid_deployment = group['from_uid_deployment']
    from_uid_daemonset = group['from_uid_daemonset']
    to_uid_pod = group['to_uid_pod']
    to_uid_service = group['to_uid_service']
    to_uid_deployment = group['to_uid_deployment']
    to_uid_daemonset = group['to_uid_daemonset']
    to_url_outbound = group['to_url_outbound']

    def in_exit_range(port):
        exit_port_range = [49152, 65535]
        return port >= exit_port_range[0] and port <= exit_port_range[1]
    
    def in_enter_range(port):
        enter_port_range = [1, 49151]
        return port >= enter_port_range[0] and port <= enter_port_range[1]

    def digit_number(port):
        # Return the digit number of the given port
        return len(str(port))

    def revert(group):
        group['from_port'] = to_port
        group['to_port'] = from_port
        group['from_uid_pod'] = to_uid_pod
        group['from_uid_service'] = to_uid_service
        group['from_uid_deployment'] = to_uid_deployment
        group['from_uid_daemonset'] = to_uid_daemonset
        group['to_uid_pod'] = from_uid_pod
        group['to_uid_service'] = from_uid_service
        group['to_uid_deployment'] = from_uid_deployment
        group['to_uid_daemonset'] = from_uid_daemonset
        return group

    # verbose_log(f'Checking if the group should be reverted: {group}')

    if to_url_outbound:
        # Do not revert
        return group
    
    if in_exit_range(from_port) and in_enter_range(to_port):
        # verbose_log(f'In exit range and in enter range: {group}')
        # Do not revert
        return group

    if in_enter_range(from_port) and in_exit_range(to_port):
        # verbose_log(f'Not in enter range and in exit range, will revert: {group}.')
        # Revert
        group['reverted'] = True
        return revert(group)
    
    if digit_number(from_port) < digit_number(to_port):
        # verbose_log(f'Digit number of from port {digit_number(from_port)} is less than digit number of to port {digit_number(to_port)}, will revert: {group}.')
        # Revert
        group['reverted'] = True
        return revert(group)
    
    if digit_number(to_port) < digit_number(from_port):
        # verbose_log(f'Digit number of to port {digit_number(to_port)} is less than digit number of from port {digit_number(from_port)}, will revert: {group}.')
        # Do not revert
        return group

    if from_port < to_port:
        if heuristics:
            # verbose_log(f'From port {from_port} is less than to port {to_port}. Heuristics = True so will revert: {group}.')
            # Revert
            group['reverted'] = True
            return revert(group)
        else:
            # verbose_log(f'From port {from_port} is less than to port {to_port}. Heuristics = False so will return None')
            return None

    # verbose_log(f'No condition is satisfied. So will return: {group}')
    return group

def get_log_sources(cluster, namespace):
    # Get pods
    # Get their containers

    # pods_q_start = time.time()
    pods = Pod.objects.filter(cluster=cluster.uid, namespace=namespace)
    # pod_uids = pods.values_list('uid', flat=True)

    # logger.info(f'Getting pods took {time.time() - pods_q_start} seconds.')
    # mapped_pods = {str(pod.uid): pod for pod in pods}

    # containers_q_start = time.time()
    containers = Container.objects.filter(cluster=cluster.uid, namespace=namespace)
    # logger.info(f'Getting containers took {time.time() - containers_q_start} seconds.')
    resources = {}


    # First write all the pods and containers from the db
    # prepare_pods_start = time.time()
    for pod in pods:
        pod_uid = str(pod.uid)
        if pod_uid not in resources:
            resources[pod_uid] = {
                'pod_name': pod.name,
                'pod_uid': pod_uid,
                'deleted': pod.deleted,
                'containers': {}
            }
            
    # logger.info(f'Preparing pods took {time.time() - prepare_pods_start} seconds.')
            

    # containers_prep_start = time.time()
    for container in containers:
        pod_uid = str(container.pod)
        container_name = container.name
        # container_nums = container.container_nums
        # len_container_nums = len(container_nums)

        # if len_container_nums > 2:
        #     current_container_num = str(max(container_nums))
        #     container_nums.remove(int(current_container_num))
        #     # Get the second max
        #     previous_container_num = str(max(container_nums))            

        # elif len_container_nums > 1:
        #     current_container_num = str(container_nums[0])
        #     previous_container_num = None

        # else:
        #     current_container_num = "0"
        #     previous_container_num = None
            
        if pod_uid not in resources:
            logger.warn(f'Owner of container {container.name}-{container.namespace}-{container.pod}-{container.cluster} does not exist in the db.')
            continue
        
        resources[pod_uid]['containers'][container_name] = {
            'container_name': container_name,
            # 'current_num': None,
            'current_num': None,
            'previous_num': None,
        }
        
    # logger.info(f'Preparing containers took {time.time() - containers_prep_start} seconds.')

    # last_2_cont_id_start = time.time()
    last_2_container_ids = get_last_2_container_ids(str(cluster.monitoring_id), namespace)
    # logger.info(f'Getting last 2 container ids took {time.time() - last_2_cont_id_start} seconds.')
    # logger.info(f'Last 2 container ids: {last_2_container_ids}')
    
    # process_last_2_container_start = time.time()
    for pod_uid, container_name, container_ids in last_2_container_ids:
        if pod_uid not in resources:
            logger.warn(f'Owner of container {container_name}-{namespace}-{pod_uid}-{cluster.monitoring_id} does not exist in the db.')
            continue
        
        if container_name not in resources[pod_uid]['containers']:
            logger.warn(f'Container {container_name} does not exist in the db.')
        else:
            if len(container_ids) == 1:
                resources[pod_uid]['containers'][container_name]['current_num'] = container_ids[0]
            if len(container_ids) == 2:
                resources[pod_uid]['containers'][container_name]['previous_num'] = container_ids[1]

    # logger.info(f'Processing last 2 container ids took {time.time() - process_last_2_container_start} seconds.')

    return resources


class Locked(Exception):
    pass


def with_redis_lock(redis_client, lock_key, timeout):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                pipe = redis_client.pipeline()
                pipe.watch(lock_key)
                locked = pipe.get(lock_key)
                if locked == 'true':
                    raise Locked()
                pipe.multi()
                pipe.set(lock_key, 'true', ex=timeout)
                pipe.execute()

                try:
                    start_time = datetime.datetime.now(datetime.UTC)
                    result = func(*args, **kwargs)
                except Exception as e2:
                    raise e2
                finally:
                    if datetime.datetime.now(datetime.UTC) - start_time > datetime.timedelta(seconds=timeout):
                        logger.warn(f'The function {func.__name__} took longer than {timeout} seconds to execute.')
                    else:
                        redis_client.delete(lock_key)
                    
                return result

            except Locked:
                logger.warn(f'Could not acquire lock {lock_key}.')
                
            except Exception as e:
                logger.error(f'Failed to execute the function {func.__name__} due to {traceback.format_exc()}.')
                
        return wrapper
    return decorator


def format_log_date(date):
    return date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + 'Z'

# @log_redis_lock(redis_client, 10)
def broadcast_logs(group_name, logs):
    channel_layer = get_channel_layer()
    # TODO: Add a lock here
    # group_name = pod_uid + '_' + container_name + '_' + container_num
    # print(f'Will broadcast logs to group {group_name}')
    for log in logs:
        # print(f'Will send log {log} to pod {pod_uid}')
        async_to_sync(channel_layer.group_send)(
            group_name,
            # message
            {
                "type": "send.log.message",  # Matches the method in LogConsumer
                "timestamp": log['timestamp'],
                "message": log['log'],
            }
        )

def mute_gpu_metrics_after(gpu_metrics, globals, valid_until):
    now = datetime.datetime.now(datetime.UTC)
    for gpu_uid, gpu_metric in gpu_metrics.items():
        if valid_until < now:
            print(f'Will mute gpu metric {gpu_uid} because valid_until {valid_until} is less than now {now}.')
            for key, val in gpu_metric['gauge'].items():
                gpu_metric['gauge'][key] = '0'
        else:
            print(f'Will not mute gpu metric {gpu_uid} because valid_until {valid_until} is greater than now {now}.')
        for metric_key, metrics in gpu_metric['time_series'].items():
            for metric in metrics:
                timestamp = metric['timestamp']
                timestamp_date = datetime.datetime.fromtimestamp(timestamp / 1000, datetime.UTC)
                if valid_until < timestamp_date:
                    print(f'Will mute gpu metric {gpu_uid} because valid_until {valid_until} is less than timestamp {timestamp_date}.')
                    for key, val in metric.items():
                        if key == 'timestamp':
                            continue
                        metric[key] = '0'
                else:
                    print(f'Will not mute gpu metric {gpu_uid} because valid_until {valid_until} is greater than timestamp {timestamp_date}.')

    return gpu_metrics, globals

def get_monitoring_id(monitoring_id):
    try:
        cluster = Cluster.objects.using('default').get(monitoring_id=monitoring_id)
        return cluster
    except Exception as e:
        raise exceptions.NotFoundError({'msg': f'Cluster with monitoring id {monitoring_id} is not found'})


def get_default_setting():
    exception = None
    for _ in range(5):
        try:
            settings = Setting.objects.get(name='default')
            return settings
        except Exception as e:
            exception = e
            time.sleep(0.2)

    if exception:
        logger.warn(f'Failed to get default settings with exception {exception}')
    return None

def get_cluster(uid):
    try:
        cluster = Cluster.objects.using('default').get(uid=uid)
        return cluster
    except Exception as e:
        raise exceptions.NotFoundError({'msg': f'Cluster {uid} is not found'})

def save_cluster(cluster):
    # logger.info(f'Saving cluster: {cluster.uid}')
    if settings.LOCK_CLUSTER_UPDATE:
        with transaction.atomic():
            try:
                cluster_to_save = Cluster.objects.select_for_update().get(uid=cluster.uid)
                cluster_to_save.name = cluster.name
                cluster_to_save.fresh = cluster.fresh
                cluster_to_save.team = cluster.team
                cluster_to_save.last_data_ts = cluster.last_data_ts
                cluster_to_save.is_alive = cluster.is_alive
                cluster_to_save.last_heartbeat = cluster.last_heartbeat
                cluster_to_save.alaz_info = cluster.alaz_info
                cluster_to_save.alaz_version = cluster.alaz_version
                cluster_to_save.instances = cluster.instances
                cluster_to_save.save()
                # logger.info(f'Saved cluster: {cluster.uid}')
            except Cluster.DoesNotExist:
                logger.warn(f'Cluster {cluster.uid} is not found. It could be okay if the cluster is deleted at this moment')
                raise exceptions.NotFoundError({'msg': f'Cluster {cluster.uid} is not found'})
            except Exception as e:
                logger.error(f'Failed to save cluster {cluster.uid} with exception {e}')
                raise exceptions.ServerError({'msg': f'Failed to save cluster {cluster.uid} with exception {e}'})
    else:
        cluster.save()

def parse_resp(data):
    
    def recursive_parse(data):
        type_byte = data[0:1]
        content = data[1:data.index('\r\n')]
        next_index = data.index('\r\n') + 2  # Position after the current element
        success = True

        if type_byte == '+':  # Simple strings
            return content, next_index, success

        elif type_byte == '-':  # Simple errors
            return content, next_index, success

        elif type_byte == ':':  # Integers
            return int(content), next_index, success

        elif type_byte == '$':  # Bulk strings
            length = int(content)
            if length == -1:
                success = False
                return None, next_index, success
            start = data.index('\r\n') + 2
            try:
                end = data[start:].index('\r\n') + start
            except:
                end = start + length
            return data[start:end], end + 2, success

        elif type_byte == '*':  # Arrays
            count = int(content)
            if count == -1:
                return None, next_index, success
            elements = []
            start = data.index('\r\n') + 2
            for _ in range(count):
                sub_data = data[start:]
                try:
                    parsed_element, next_index, recursive_success = recursive_parse(sub_data)
                except Exception as e:
                    success = False
                    return elements, next_index, success
                if not recursive_success:
                    success = False
                    return elements, next_index, success
                elements.append(parsed_element)
                start += next_index
            return elements, start, success

        elif type_byte == '_':  # Nulls
            return None, next_index, success

        elif type_byte == '#':  # Booleans
            return content == 't', next_index, success

        elif type_byte == ',':  # Doubles
            return float(content), next_index, success

        elif type_byte == '(':  # Big numbers
            return int(content), next_index, success  # This assumes the number fits into Python's int

        elif type_byte == '!':  # Bulk errors
            length = int(content)
            start = data.index('\r\n') + 2
            end = start + length
            return data[start:end], end + 2, success

        elif type_byte == '=':  # Verbatim strings
            length = int(content)
            start = data.index('\r\n') + 2
            end = start + length
            return data[start:end], end + 2, success

        elif type_byte == '%':  # Maps
            count = int(content) * 2  # Key-value pairs, so double the count
            if count == -2:
                return None, next_index, success
            elements = []
            for _ in range(count):
                try:
                    parsed_element, parsed_length, recursive_success = recursive_parse(data[next_index:])
                except Exception as e:
                    success = False
                    return elements, next_index, success
                if not recursive_success:
                    success = False
                    return elements, next_index, success
                elements.append(parsed_element)
                next_index += parsed_length
            return dict(zip(elements[0::2], elements[1::2])), next_index, success  # Create dictionary from list

        elif type_byte == '~':  # Sets
            count = int(content)
            if count == -1:
                return None, next_index, success
            elements = []
            for _ in range(count):
                try:
                    parsed_element, parsed_length, recursive_success = recursive_parse(data[next_index:])
                except Exception as e:
                    success = False
                    return elements, next_index, success
                if not recursive_success:
                    success = False
                    return elements, next_index, success
                elements.append(parsed_element)
                next_index += parsed_length
            return set(elements), next_index, success  # Create set from list

        elif type_byte == '>':  # Pushes
            # Pushes are similar to Arrays but for different protocol use, treat like Array
            count = int(content)
            if count == -1:
                return None, next_index, success
            elements = []
            for _ in range(count):
                try:
                    parsed_element, parsed_length, recursive_success = recursive_parse(data[next_index:])
                except Exception as e:
                    success = False
                    return elements, next_index, success
                if not recursive_success:
                    success = False
                    return elements, next_index, success
                elements.append(parsed_element)
                next_index += parsed_length
            return elements, next_index, success  # Return the list

        else:
            return None, None, False

    try:
        result = recursive_parse(data)[0]
    except Exception as e:
        # TODO: Change this to logger.error
        logger.warn(f'Failed to parse resp message {data.encode()} with exception {e}')
        result = 'Unknown'

    formatted_result = None
    if type(result) == list:
        string_elems = list(map(str, result))
        formatted_result = ' '.join(string_elems)
    else:
        formatted_result = str(result)
        
    return formatted_result

def remove_values_from_redis_query(data):
    # Turn all words aside from the first one to %s as they are the values
    data = data.split(' ')
    for i in range(1, len(data)):
        data[i] = '%s'
    data = ' '.join(data)

    return data
    

def convert_bytes_to_megabytes(bytes_size):
    return bytes_size / (1024 * 1024)


def notify_bloat(type, **kwargs):
    if settings.ANTEON_ENV == 'onprem':
        return

    if not settings.BLOAT_NOTIFICATION_ENABLED:
        return

    if type not in ['redis', 'postgres', 'rabbitmq', 'clickhouse']:
        logger.error(f'Invalid bloat type: {type}')

    def format_bloat_notification(blocks, **kwargs):
        blocks[0]['text']['text'] = blocks[0]['text']['text'].format(env=settings.ANTEON_ENV, **kwargs)

        return blocks

    slack_requester = SlackRequester()
    access_token = settings.SLACK_BLOAT_NOTIFIER_TOKEN
    try:
        setting = Setting.objects.get(name='default')
    except Setting.DoesNotExist:
        logger.error(f"Default setting not found. Could not notify bloat for type: {type}")
        return

    channel = setting.bloat_notifier_channel
    if not channel:
        logger.error(f"Bloat notifier channel not set. Could not notify bloat for type: {type}")
        return

    blocks = setting.alert_messages['slack']['bloat'][type]
    blocks = format_bloat_notification(blocks, **kwargs)

    slack_requester.send_message(access_token, channel, message=None, blocks=blocks)


def insert_requests(requests, cursor, connection):
    def get_request_values(request):
        return (request["cluster"], request['start_time'], request['latency'], request['from_ip'], request['to_ip'], request['from_port'], request['to_port'], request['from_uid_pod'], request['from_uid_service'], request['from_uid_deployment'], request['from_uid_daemonset'], request['from_uid_statefulset'], request['to_uid_pod'], request['to_uid_service'], request['to_url_outbound'], request['to_uid_deployment'], request['to_uid_daemonset'], request['to_uid_statefulset'], request['protocol'], request['status_code'], request['fail_reason'], request['method'], request['path'].replace('\r', '\\r'), request['non_simplified_path'].replace('\r', '\\r'), request['tls'])
    
    if len(requests) == 0:
        return
    data = [get_request_values(request) for request in requests]

    with cursor.copy("COPY core_request (cluster, start_time, latency, from_ip, to_ip, from_port, to_port, from_uid_pod, from_uid_service, from_uid_deployment, from_uid_daemonset, from_uid_statefulset, to_uid_pod, to_uid_service, to_url_outbound, to_uid_deployment, to_uid_daemonset, to_uid_statefulset, protocol, status_code, fail_reason, method, path, non_simplified_path, tls) FROM STDIN") as copy:
        for record in data:
            copy.write_row(record)

    connection.commit()


def generate_insert_query_of_requests(requests):
    def get_request_values(request):
        # ('7701a634-5598-4717-95fb-c63fc4528da6'::uuid, '2023-12-15T13:51:47+00:00'::timestamptz, 211981522, '192.168.94.28', '10.100.106.48', 8000, 8001, 'd920a7cc-8a82-40e3-aef5-e40c2d6d1dd9'::uuid, NULL, '0ef3d8cf-71bf-44b1-a4f6-d0a3bbf58b24'::uuid, NULL, NULL, 'd920a7cc-8a82-40e3-aef5-e40c2d6d1dd9'::uuid, NULL, NULL, '0ef3d8cf-71bf-44b1-a4f6-d0a3bbf58b24'::uuid, NULL, NULL, 'POSTGRES', 403, '', 'GET', 'SELECT id, name, task, interval_id, crontab_id, solar_id, clocked_id, args, kwargs, queue, exchange, routing_key, headers, priority, expires, expire_seconds, one_off, start_time, enabled, last_run_at, total_run_count, django_celery_beat_periodictask."date_chang', 'SELECT id, name, task, interval_id, crontab_id, solar_id, clocked_id, args, kwargs, queue, exchange, routing_key, headers, priority, expires, expire_seconds, one_off, start_time, enabled, last_run_at, total_run_count, django_celery_beat_periodictask."date_chang', true, 1, 'asd', 1, false)
    
        cluster_val = f"'{request["cluster"]}'::uuid"
        start_time_val = f"'{request['start_time']}'::timestamptz"
        latency_val = request['latency']
        from_ip_val = request['from_ip']
        to_ip_val = request['to_ip']
        from_port_val = request['from_port']
        to_port_val = request['to_port']
        from_uid_pod_val = f"'{request['from_uid_pod']}'::uuid" if request['from_uid_pod'] else 'NULL'
        from_uid_service_val = f"'{request['from_uid_service']}'::uuid" if request['from_uid_service'] else 'NULL'
        from_uid_deployment_val = f"'{request['from_uid_deployment']}'::uuid" if request['from_uid_deployment'] else 'NULL'
        from_uid_daemonset_val = f"'{request['from_uid_daemonset']}'::uuid" if request['from_uid_daemonset'] else 'NULL'
        from_uid_statefulset_val = f"'{request['from_uid_statefulset']}'::uuid" if request['from_uid_statefulset'] else 'NULL'
        to_uid_pod_val = f"'{request['to_uid_pod']}'::uuid" if request['to_uid_pod'] else 'NULL'
        to_uid_service_val = f"'{request['to_uid_service']}'::uuid" if request['to_uid_service'] else 'NULL'
        to_url_outbound_val = request['to_url_outbound']
        to_uid_deployment_val = f"'{request['to_uid_deployment']}'::uuid" if request['to_uid_deployment'] else 'NULL'
        to_uid_daemonset_val = f"'{request['to_uid_daemonset']}'::uuid" if request['to_uid_daemonset'] else 'NULL'
        to_uid_statefulset_val = f"'{request['to_uid_statefulset']}'::uuid" if request['to_uid_statefulset'] else 'NULL'
        protocol_val = request['protocol']
        status_code_val = request['status_code']
        fail_reason_val = request['fail_reason']
        method_val = request['method']
        path_val = request['path'].replace("'", "''")
        non_simplified_path_val = request['non_simplified_path'].replace("'", "''")
        tls_val = request['tls']
        tcp_seq_num_val = request['tcp_seq_num']
        node_id_val = f"'{request['node_id']}'"
        thread_id_val = request['thread_id']
        span_exists_val = request['span_exists']
        
        return f"({cluster_val}, {start_time_val}, {latency_val}, '{from_ip_val}', '{to_ip_val}', {from_port_val}, {to_port_val}, {from_uid_pod_val}, {from_uid_service_val}, {from_uid_deployment_val}, {from_uid_daemonset_val}, {from_uid_statefulset_val}, {to_uid_pod_val}, {to_uid_service_val}, '{to_url_outbound_val}', {to_uid_deployment_val}, {to_uid_daemonset_val}, {to_uid_statefulset_val}, '{protocol_val}', {status_code_val}, '{fail_reason_val}', '{method_val}', '{path_val}', '{non_simplified_path_val}', {tls_val}, {tcp_seq_num_val}, {node_id_val}, {thread_id_val}, {span_exists_val})"

    values = [get_request_values(request) for request in requests]
    query = f'INSERT INTO "core_request" ("cluster", "start_time", "latency", "from_ip", "to_ip", "from_port", "to_port", "from_uid_pod", "from_uid_service", "from_uid_deployment", "from_uid_daemonset", "from_uid_statefulset", "to_uid_pod", "to_uid_service", "to_url_outbound", "to_uid_deployment", "to_uid_daemonset", "to_uid_statefulset", "protocol", "status_code", "fail_reason", "method", "path", "non_simplified_path", "tls", "tcp_seq_num", "node_id", "thread_id", "span_exists") VALUES {', '.join(values)}'
    return query


def validate_start_and_end_times(start_time, end_time):
    # Times are expected in ms
    if not start_time:
        raise exceptions.ValidationError({'msg': 'Start time is required'})
    
    if not end_time:
        raise exceptions.ValidationError({'msg': 'End time is required'})
    
    try:
        start_time = int(start_time)
        end_time = int(end_time)
    except ValueError:
        raise exceptions.ValidationError({'msg': "Invalid start or end time"})

    if start_time > end_time:
        raise exceptions.ValidationError({'msg': "Start time is greater than end time"})

    start_time = datetime.datetime.fromtimestamp(start_time / 1000, datetime.UTC)
    end_time = datetime.datetime.fromtimestamp(end_time / 1000, datetime.UTC)

    logger.info(f'Returning start time {start_time} and end time {end_time}')
    return start_time, end_time


def validate_monitoring_id(monitoring_id):
    if monitoring_id is None:
        raise exceptions.InvalidRequestError({'msg': "Monitoring id is required"})

    cluster_qs = Cluster.objects.filter(monitoring_id=monitoring_id)
    if not cluster_qs.exists():
        raise exceptions.InvalidRequestError({'msg': "Cluster for the monitoring id not found"})

    cluster = cluster_qs.first()

    logger.info(f'Returning cluster {cluster.uid} for monitoring id {monitoring_id}')
    return cluster


def get_grouping_interval(start_time, end_time):
    # Choose an appropriate grouping interval from 1s, 5s, 15s, 30s, 60s, 5m, 15m, 30m, 1h, 6h, 12h, 1d
    
    interval = end_time - start_time
    if interval <= datetime.timedelta(seconds=60):
        return 1  # '1s'
    elif interval <= datetime.timedelta(seconds=300):
        return 5  # '5s'
    elif interval <= datetime.timedelta(seconds=900):
        return 15  # '15s'
    elif interval <= datetime.timedelta(seconds=1800):
        return 30  # '30s'
    elif interval <= datetime.timedelta(seconds=3600):
        return 60  # '60s'
    elif interval <= datetime.timedelta(seconds=300 * 60):
        return 300  # '5m'
    elif interval <= datetime.timedelta(seconds=900 * 60):
        return 900  # '15m'
    elif interval <= datetime.timedelta(seconds=1800 * 60):
        return 1800  # '30m'
    elif interval <= datetime.timedelta(seconds=3600 * 60):
        return 3600  # '1h'
    elif interval <= datetime.timedelta(seconds=3600 * 6):
        return 21_600  # '6h'
    elif interval <= datetime.timedelta(seconds=3600 * 12):
        return 43_200  # '12h'
    else:
        return 86_400  # '1d'


def get_grouping_expr(grouping_interval, column_name):
    class RoundSecond(Func):
        function = 'FLOOR'
        template = "%(function)s(EXTRACT(EPOCH FROM %(expressions)s) / %(interval)s) * %(interval)s"

    grouping_expr = RoundSecond(
    ExpressionWrapper(F(column_name), output_field=FloatField()), 
    interval=grouping_interval
    )

    return grouping_expr


def get_resources_of_cluster(cluster):
    deployments = Deployment.objects.filter(cluster=cluster.uid, deleted=False)
    daemon_sets = DaemonSet.objects.filter(cluster=cluster.uid, deleted=False)
    stateful_sets = StatefulSet.objects.filter(cluster=cluster.uid, deleted=False)
    pods = Pod.objects.filter(cluster=cluster.uid, deleted=False, replicaset_owner=None, daemonset_owner=None, statefulset_owner=None)
    services = Service.objects.filter(cluster=cluster.uid, deleted=False)
    
    template = {
        'deployments': [],
        'daemon_sets': [],
        'stateful_sets': [],
        'pods': [],
        'services': [],
    }
    
    namespaces = {}
    for deployment in deployments:
        namespace = deployment.namespace
        if namespace not in namespaces:
            namespaces[namespace] = template.copy()
        namespaces[namespace]['deployments'].append({
            'name': deployment.name,
            'uid': deployment.uid,
        })
        
    for daemon_set in daemon_sets:
        namespace = daemon_set.namespace
        if namespace not in namespaces:
            namespaces[namespace] = template.copy()
        namespaces[namespace]['daemon_sets'].append({
            'name': daemon_set.name,
            'uid': daemon_set.uid,
        })
        
    for stateful_set in stateful_sets:
        namespace = stateful_set.namespace
        if namespace not in namespaces:
            namespaces[namespace] = template.copy()
        namespaces[namespace]['stateful_sets'].append({
            'name': stateful_set.name,
            'uid': stateful_set.uid,
        })
        
    for pod in pods:
        namespace = pod.namespace
        if namespace not in namespaces:
            namespaces[namespace] = template.copy()

        namespaces[namespace]['pods'] = {
            'uid': pod.uid,
            'name': pod.name,
        }
        
    for service in services:
        namespace = service.namespace
        if namespace not in namespaces:
            namespaces[namespace] = template.copy()
        namespaces[namespace]['services'].append({
            'name': service.name,
            'uid': service.uid,
        })
    
    return namespaces


def get_onprem_retention_days():
    root_user = User.objects.get(email=settings.ROOT_EMAIL)
    is_enterprise = root_user.selfhosted_enterprise
    if is_enterprise:
        days = settings.ENTERPRISE_RETENTION_DAYS
    else:
        days = settings.COMMUNITY_RETENTION_DAYS

    return days


class RawAnnotation(RawSQL):
    """
    RawSQL also aggregates the SQL to the `group by` clause which defeats the purpose of adding it to an Annotation.
    """
    def get_group_by_cols(self):
        return []


def prepare_api_catalog_selected_filters(data):
    protocol = data['protocol']
    response = {
        # 'monitoring_id': data['monitoring_id'],
        # 'start_time': data['start_time'],
        # 'end_time': data['end_time'],
        'protocols': [],
        'status_codes': [],
        'methods': [],
        'tls': [],
        'from_resources': [],
        'to_resources': [],
        'extra_filters': {
            'KAFKA': {
                'topics': [],
                'partitions': [],
                'keys': [],
                'values': [],
                'types': []
            }
        }
    }

    response['from_resources'].append({
        'type': data['from_type'],
        'uid': data['from_uid']
    })

    response['to_resources'].append({
        'type': data['to_type'],
        'uid': data['to_uid']
    })
    response['tls'].append(data['tls'])
    response['protocols'].append(protocol)
    
    if protocol == 'KAFKA':
        response['extra_filters']['KAFKA']['topics'].append(data['topic'])
        response['extra_filters']['KAFKA']['partitions'].append(data['partition'])
        response['extra_filters']['KAFKA']['keys'].append(data['key'])
        response['extra_filters']['KAFKA']['values'].append(data['value'])
    else:
        response['status_codes'].append(data['status_code'])
        response['methods'].append(data['method'])

    return response

def prepare_k8s_events_selected_filters(data):
    response = {
        'kinds': [],
        'namespaces': [],
        'resources': [],
        'reasons': [],
    }
    
    response['kinds'] = data['kinds']
    response['namespaces'] = data['namespaces']
    response['resources'] = data['resources']
    response['reasons'] = data['reasons']
    
    return response