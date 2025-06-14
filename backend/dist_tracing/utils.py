# import logging
# from rest_framework.pagination import PageNumberPagination
# from math import ceil
# from rest_framework.response import Response
# from dist_tracing.models import Trace
# from core.utils import check_monitoring_read_permission
# from core import exceptions
# from dist_tracing.serializers import TraceWriteSerializer
# from core.models import Cluster, DaemonSet, Deployment, Pod, Service
# from django.db.models import Q
# from datetime import UTC, datetime

# logger = logging.getLogger(__name__)
# class PaginationWithPageNumber(PageNumberPagination):
#     def get_paginated_response(self, data):
#         return Response({
#             'links': {
#                'next': self.get_next_link(),
#                'previous': self.get_previous_link()
#             },
#             'count': self.page.paginator.count,
#             'page_size': self.page_size,
#             'total_pages': ceil(self.page.paginator.count / self.page_size),
#             'results': data
#         })


# def link_span_to_parent(child, parent, trace_cache):
#     trace, parent = get_or_create_trace_of_span(parent, trace_cache)
#     trace, child = add_span_to_trace(trace, child)
#     child.parent_id = parent.id
#     trace_cache[str(trace.id)] = trace
#     return trace, child, parent


# def get_or_create_trace_of_span(span, trace_cache):
#     trace = None
#     if span.trace_id is not None:
#         if str(span.trace_id) in trace_cache:
#             trace = trace_cache[str(span.trace_id)]
#         else:
#             try:
#                 trace = Trace.objects.get(id=span.trace_id)
#             except Trace.DoesNotExist:
#                 pass

#     if trace is None or span.trace_id is None:
#         payload = {
#             'cluster': span.cluster,
#             'name': span.name,
#             'start_time': span.start_time,
#             'end_time': span.end_time,
#             'attributes': {},
#             # 'spans': [span.id],
#             'span_count': 1,
#         }

#         for key in span.attributes:
#             if key not in payload['attributes']:
#                 payload['attributes'][key] = []
#             payload['attributes'][key].append(span.attributes[key])

#         serializer = TraceWriteSerializer(data=payload)
#         serializer.is_valid(raise_exception=True)
#         trace = serializer.save()
        
#     span.trace_id = trace.id

#     return trace, span


# def add_span_to_trace(trace, span):
#     for key, data in span.attributes.items():
#         if key not in trace.attributes:
#             trace.attributes[key] = []
#         if data not in trace.attributes[key]:
#             trace.attributes[key].append(data)

#     if span.start_time < trace.start_time:
#         trace.start_time = span.start_time
#     if span.end_time > trace.end_time:
#         trace.end_time = span.end_time
#     trace.span_count += 1
#     span.trace_id = trace.id
#     return trace, span


# def get_trace_filters(user, request):
#     body = request.data
#     errors = {}
#     cluster_uid = None

#     monitoring_id = body.get('monitoring_id', None)
#     if monitoring_id:
#         try:
#             cluster_qs = Cluster.objects.filter(monitoring_id=monitoring_id)
#         except Exception:
#             errors['monitoring_id'] = ['Monitoring id is not valid']

#         if cluster_qs.exists():
#             cluster = cluster_qs.first()
#             cluster_uid = cluster.uid
#             try:
#                 check_monitoring_read_permission(user, cluster)
#             except Exception as exc:
#                 errors['monitoring_id'] = ['You do not have permission to access this cluster']
#     else:
#         errors['monitoring_id'] = ['Monitoring id is required']

#     operations = body.get('operations', None)
#     if operations:
#         if type(operations) != list:
#             errors['operations'] = ['Operations are not valid']
#         for elem in operations:
#             if type(elem) != str:
#                 errors['operations'] = ['Operations are not valid']
#                 break
    
#     attributes = body.get('attributes', None)
#     if attributes:
#         try:
#             attributes = attributes.strip().split(' ')
#             # Remove empty strings
#             attributes = list(filter(None, attributes))
#             attributes = [attr.strip().split('=') for attr in attributes]
#             # Turn it into a dictionary
#             attributes = {'attributes__' + attr[0] + '__contains': attr[1] for attr in attributes}
#         except Exception:
#             errors['attributes'] = ['Attributes are not valid']
        
#     start_time = body.get('start_time', None)
#     if start_time:
#         try:
#             start_time = datetime.fromtimestamp(int(start_time) / 1000, UTC)
#         except Exception:
#             errors['start_time'] = ['Start time is not valid']
#     else:
#         errors['start_time'] = ['Start time is required']
    
#     end_time = body.get('end_time', None)
#     if end_time:
#         try:
#             end_time = datetime.fromtimestamp(int(end_time) / 1000, UTC)
#         except Exception:
#             errors['end_time'] = ['End time is not valid']
#     else:
#         errors['end_time'] = ['End time is required']

#     if start_time and end_time:
#         try:
#             # Start time can't be later than end time
#             if start_time > end_time:
#                 errors['start_time'] = ['Start time cannot be later than end time']
#             # Time difference can't be more than 24 hours
#             if (end_time - start_time).total_seconds() > 24 * 60 * 60:
#                 errors['start_time'] = ['Start time and end time cannot be more than 24 hours apart']
#         except Exception:
#             pass

#     min_duration_ms = body.get('min_duration_ms', None)
#     if min_duration_ms:
#         try:
#             min_duration_ms = float(min_duration_ms)
#         except Exception:
#             errors['min_duration_ms'] = ['Min duration is not valid']
        
#     max_duration_ms = body.get('max_duration_ms', None)
#     if max_duration_ms:
#         try:
#             max_duration_ms = float(max_duration_ms)
#         except Exception:
#             errors['max_duration_ms'] = ['Max duration is not valid']

#     if min_duration_ms and max_duration_ms and min_duration_ms > max_duration_ms:
#         errors['min_duration_ms'] = ['Min duration cannot be larger than max duration']

#     min_span_count = body.get('min_span_count', None)
#     if min_span_count:
#         try:
#             min_span_count = int(min_span_count)
#         except Exception:
#             errors['min_span_count'] = ['Min span count is not valid']

#     max_span_count = body.get('max_span_count', None)
#     if max_span_count:
#         try:
#             max_span_count = int(max_span_count)
#         except Exception:
#             errors['max_span_count'] = ['Max span count is not valid']

#     if min_span_count and max_span_count:
#         try:
#             if min_span_count > max_span_count:
#                 errors['min_duration_ms'] = ['Min span count cannot be larger than max span count']
#         except Exception:
#             pass

#     if errors != {}:
#         raise exceptions.ValidationError({'msg': errors})

#     # Filter traces
#     filters = Q()
#     if cluster_uid:
#         filters &= Q(cluster=cluster_uid)
#     else:
#         logger.error('Cluster uid is not found in trace filtering. This should not happen.')

#     if operations:
#         filters &= Q(name__in=operations)

#     if attributes:
#         filters &= Q(**attributes)

#     if start_time:
#         filters &= Q(start_time__gte=start_time)

#     if end_time:
#         filters &= Q(end_time__lte=end_time)

#     if min_duration_ms:
#         filters &= Q(duration_ms__gte=min_duration_ms)

#     if max_duration_ms:
#         filters &= Q(duration_ms__lte=max_duration_ms)

#     if min_span_count:
#         filters &= Q(span_count__gte=min_span_count)

#     if max_span_count:
#         filters &= Q(span_count__lte=max_span_count)

#     return filters 


# def cache_all_alaz_resources():

#     pods = Pod.objects.filter(deleted=False)
#     services = Service.objects.filter(deleted=False)
#     deployments = Deployment.objects.filter(deleted=False)
#     daemonsets = DaemonSet.objects.filter(deleted=False)

#     result = {}

#     for pod in pods:
#         if 'pods' not in result:
#             result['pods'] = {}
#         result['pods'][str(pod.uid)] = pod

#     for service in services:
#         if 'services' not in result:
#             result['services'] = {}
#         result['services'][str(service.uid)] = service

#     for deployment in deployments:
#         if 'deployments' not in result:
#             result['deployments'] = {}
#         result['deployments'][str(deployment.uid)] = deployment

#     for daemonset in daemonsets:
#         if 'daemonsets' not in result:
#             result['daemonsets'] = {}
#         result['daemonsets'][str(daemonset.uid)] = daemonset

#     return result