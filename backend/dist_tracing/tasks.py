# from datetime import timedelta, datetime, UTC
# import json
# import logging
# import threading
# import traceback
# from celery import shared_task
# from django.db import connection
# import jsonpickle
# import redis
# from accounts.utils import find_team_owner_if_exists
# from core.utils import with_redis_lock
# from core.models import Cluster, DaemonSet, Deployment, KafkaEvent, Pod, Request, Service
# from core.tasks import sample_mem_wrapper
# from core.models import Cluster, DaemonSet, Deployment, Pod, Request, Service
# from dist_tracing.serializers import SpanCreateSerializer
# from dist_tracing.utils import get_or_create_trace_of_span, link_span_to_parent
# from django.conf import settings
# from celery.utils.debug import sample_mem, memdump
# from django.core.cache import cache


# from dist_tracing.models import Span, Trace, Traffic

# logger = logging.getLogger(__name__)
# redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0, charset="utf-8", decode_responses=True)
# delete_batch_size = settings.REQUESTS_DELETE_BULK_BATCH_SIZE

# def verbose_log(log):
#     if settings.CELERY_VERBOSE_LOG:
#         logger.info(log)

# @shared_task
# @with_redis_lock(redis_client, settings.SPAN_GENERATION_LOCK, settings.SPAN_GENERATION_TIMEOUT_SECONDS)
# def task_generate_spans_with_requests():
#     # TODO: Make this in batches
#     resources = {}
#     def fetch_resource(type, uid):
#         if uid in resources:
#             return resources[uid]

#         resource = None
#         try:
#             if type == 'pod':
#                 resource = Pod.objects.using("dist_tracing_tasks").get(uid=uid)
#             elif type == 'service':
#                 resource = Service.objects.using("dist_tracing_tasks").get(uid=uid)
#             elif type == 'deployment':
#                 resource = Deployment.objects.using("dist_tracing_tasks").get(uid=uid)
#             elif type == 'daemonset':
#                 resource = DaemonSet.objects.using("dist_tracing_tasks").get(uid=uid)
#         except Exception as e:
#             logger.warn(f"Error while fetching {type} {uid}: {e}")
        
#         resources[uid] = resource
#         return resource

#     verbose_log(f"Generating spans with requests")
#     # start_time = datetime.now(UTC)
#     time_interval_sec = settings.SPAN_GENERATION_INTERVAL_SEC * 3
#     match_limit_sec = settings.DIST_TRACING_MATCH_LIMIT_SEC
#     request_extra_interval_sec = settings.DIST_TRACING_REQUEST_INTERVAL_EXTRA_SECONDS

#     # TODO: Add 1 day limit for traffic matching
#     sql = '''
# -- Step 1: Filter Traffic and AlazRequest
# WITH FilteredTraffic AS (
#     SELECT *
#     FROM dist_tracing_traffic
#     WHERE dist_tracing_traffic.span_exists = FALSE
#       AND date_created >= (CURRENT_TIMESTAMP - INTERVAL '%s seconds')
#       AND tcp_seq_num != 0
# ),
# FilteredAlazRequest AS (
#     SELECT *
#     FROM core_request
#     WHERE core_request.span_exists = FALSE
#       AND start_time >= (CURRENT_TIMESTAMP - INTERVAL '%s seconds')
#       AND tcp_seq_num != 0
# )

# -- Step 2: Create CTEs for Egress and Ingress
# , Egress AS (
#     SELECT *
#     FROM FilteredTraffic
#     WHERE ingress = FALSE
# ),
# Ingress AS (
#     SELECT *
#     FROM FilteredTraffic
#     WHERE ingress = TRUE
# )

# -- Step 3: Join and Match Logic
# SELECT AR.id, E.id, I.id
# FROM FilteredAlazRequest AR
#     JOIN Egress E ON AR.node_id = E.node_id AND AR.thread_id = E.thread_id AND AR.tcp_seq_num = E.tcp_seq_num AND AR.cluster = E.cluster_id
#     JOIN Ingress I ON AR.tcp_seq_num = I.tcp_seq_num AND AR.cluster = I.cluster_id
# WHERE
#     AR.to_url_outbound IS NULL
#     AND
#     CASE
#         WHEN E.timestamp > AR.start_time THEN
#             EXTRACT(EPOCH FROM (E.timestamp - AR.start_time)) <= %s + AR.latency / 1000000
#         ELSE
#             EXTRACT(EPOCH FROM (AR.start_time - E.timestamp)) <= %s + AR.latency / 1000000
#     END
# UNION

# SELECT AR.id, E.id, NULL
# FROM FilteredAlazRequest AR
#     JOIN Egress E ON AR.node_id = E.node_id AND AR.thread_id = E.thread_id AND AR.tcp_seq_num = E.tcp_seq_num AND AR.cluster = E.cluster_id
# WHERE
#     CASE
#         WHEN E.timestamp > AR.start_time THEN
#             EXTRACT(EPOCH FROM (E.timestamp - AR.start_time)) <= %s + AR.latency / 1000000
#         ELSE
#             EXTRACT(EPOCH FROM (AR.start_time - E.timestamp)) <= %s + AR.latency / 1000000
#     END
#     AND
#     AR.to_url_outbound IS NOT NULL;

# '''
#     cursor = connection.cursor()
#     cursor.execute(sql, [time_interval_sec, time_interval_sec + request_extra_interval_sec, match_limit_sec, match_limit_sec, match_limit_sec, match_limit_sec])
#     rows = cursor.fetchall()
#     cursor.close()

#     matched_requests = {}
#     matched_egress = {}
#     matched_ingress = {}

#     spans_to_create = []
#     traffic_to_update = []
#     requests_to_update = []
#     verbose_log(f'Rows count: {len(rows)}')
#     for row in rows:
#         request_id = row[0]
#         egress_id = row[1]
#         ingress_id = row[2]

#         if request_id in matched_requests:
#             continue

#         if egress_id in matched_egress:
#             continue

#         if ingress_id in matched_ingress:
#             continue

#         request = Request.objects.using("dist_tracing_tasks").get(id=request_id)
#         egress = Traffic.objects.using("dist_tracing_tasks").get(id=egress_id)
#         ingress = Traffic.objects.using("dist_tracing_tasks").get(id=ingress_id) if ingress_id else None

#         # Turn nanoseconds to seconds
#         latency = request.latency / 1000000000

#         name = request.path
#         if request.protocol.lower() == 'postgres':
#             # verbose_log(f"Postgres span name before: {name}")
#             # Get the first word
#             name = name.split(' ')[0]
#             # verbose_log(f"Postgres span name after 1: {name}")
                
#         if not name:
#             name = request.method
#         # verbose_log(f"Postgres span name after 2: {name}")

#         start_time = egress.timestamp
#         end_time = egress.timestamp + timedelta(seconds=latency)

#         data = {
#             'name': name,
#             'start_time': egress.timestamp,
#             'end_time': egress.timestamp + timedelta(seconds=latency),
#             'cluster': str(egress.cluster_id),
#             'egress_tcp_num': egress.tcp_seq_num,
#             'egress_thread_id': egress.thread_id,
#             'ingress_tcp_num': ingress.tcp_seq_num if ingress else None,
#             'ingress_thread_id': ingress.thread_id if ingress else None,
#             'node_id': egress.node_id,
#             'attributes': {
#                 'from_ip': request.from_ip,
#                 'to_ip': request.to_ip,
#                 'from_port': request.from_port,
#                 'to_port': request.to_port,
#                 'protocol': request.protocol,
#                 'status_code': request.status_code,
#                 'method': request.method,
#                 'path': request.path,
#             },
#         }

#         from_type = None
#         from_uid = None

#         to_type = None
#         to_uid = None

#         if request.from_uid_deployment:
#             from_type = 'deployment'
#             from_uid = request.from_uid_deployment
#         elif request.from_uid_daemonset:
#             from_type = 'daemonset'
#             from_uid = request.from_uid_daemonset
#         elif request.from_uid_pod:
#             from_type = 'pod'
#             from_uid = request.from_uid_pod
#         elif request.from_uid_service:
#             from_type = 'service'
#             from_uid = request.from_uid_service

#         if request.to_uid_deployment:
#             to_type = 'deployment'
#             to_uid = request.to_uid_deployment
#         elif request.to_uid_daemonset:
#             to_type = 'daemonset'
#             to_uid = request.to_uid_daemonset
#         elif request.to_uid_pod:
#             to_type = 'pod'
#             to_uid = request.to_uid_pod
#         elif request.to_uid_service:
#             to_type = 'service'
#             to_uid = request.to_uid_service
#         elif request.to_url_outbound:
#             to_type = 'outbound'
#             to_uid = request.to_url_outbound

#         from_uid = str(from_uid)
#         to_uid = str(to_uid)

#         data['attributes']['from_type'] = from_type
#         data['attributes']['from_uid'] = from_uid
#         from_resource = fetch_resource(from_type, from_uid)
#         if from_resource is None:
#             logger.warn(f"task_generate_spans_with_requests: from resource {from_uid} ({from_type}) is None. Request: {request}, egress: {egress.id}, ingress: {ingress.id if ingress else None}")
#             continue
            
#         data['attributes']['from_name'] = from_resource.name

#         data['attributes']['to_type'] = to_type
#         data['attributes']['to_uid'] = to_uid 
#         if to_type != 'outbound':
#             to_resource = fetch_resource(to_type, to_uid)
#             if to_resource is None:
#                 logger.warn(f"task_generate_spans_with_requests: to resource {to_uid} ({to_type}) is None. Request: {request}, egress: {egress.id}, ingress: {ingress.id if ingress else None}")
#                 continue

#             data['attributes']['to_name'] = to_resource.name
#         else:
#             data['attributes']['to_name'] = to_uid

#         spans_to_create.append(data)
#         # serializer = SpanCreateSerializer(data=data)
#         # if not serializer.is_valid():
#         #     continue

#         try:
#             # serializer.save()
#             egress.span_exists = True
#             traffic_to_update.append(egress)
#             # egress.save()
#             request.span_exists = True
#             requests_to_update.append(request)
#             # request.save()
#             # verbose_log(f"Created span: {span}")
#             if ingress:
#                 ingress.span_exists = True
#                 traffic_to_update.append(ingress)
#                 # ingress.save()
#                 matched_ingress[ingress.id] = True
#             matched_egress[egress.id] = True
#             matched_requests[request.id] = True
#         except Exception as e:
#             logger.error(f"Error {e} while creating span: {data}")
#             continue

#     # end_time = datetime.now(UTC)
#     # verbose_log(f"Spans are generated in {end_time - start_time} seconds")
#     batch_size = settings.BULK_BATCH_SIZE
#     verbose_log(f"Spans to create: {len(spans_to_create)}")

#     for i in range(0, len(spans_to_create), batch_size):
#         batch = spans_to_create[i:i+batch_size]
#         try:
#             Span.objects.using("dist_tracing_tasks").bulk_create([Span(**data) for data in batch])
#         except Exception as e:
#             # logger.warn(f"Error while bulk creating spans: {e}. Will create one by one")
#             for data in batch:
#                 try:
#                     Span.objects.using("dist_tracing_tasks").create(**data)
#                 except Exception as e:
#                     logger.error(f"Error while creating span: {e}. Failed to create: {data}")
#                     continue
                
#     verbose_log(f'{len(spans_to_create)} spans are generated')
#     verbose_log(f'Traffic to update: {len(traffic_to_update)}')
                
#     for i in range(0, len(traffic_to_update), batch_size):
#         batch = traffic_to_update[i:i+batch_size]
#         try:
#             Traffic.objects.using("dist_tracing_tasks").bulk_update(batch, ['span_exists'])
#         except Exception as e:
#             # logger.warn(f"Error while bulk updating traffic: {e}. Will update one by one")
#             for obj in batch:
#                 try:
#                     obj.save()
#                 except Exception as e:
#                     logger.error(f"Error while updating traffic: {e}. Failed to update: {obj}")
#                     continue
                
#     verbose_log(f'{len(traffic_to_update)} traffic are updated')
#     verbose_log(f'Requests to update: {len(requests_to_update)}')
        
#     for i in range(0, len(requests_to_update), batch_size):
#         batch = requests_to_update[i:i+batch_size]
#         try:
#             Request.objects.using("dist_tracing_tasks").bulk_update(batch, ['span_exists'])
#         except Exception as e:
#             # logger.warn(f"Error while bulk updating requests: {e}. Will update one by one")
#             for obj in batch:
#                 try:
#                     obj.save()
#                 except Exception as e:
#                     logger.error(f"Error while updating request: {e}. Failed to update: {obj}")
#                     continue
    
#     verbose_log(f'{len(requests_to_update)} requests are updated')


# @shared_task
# @with_redis_lock(redis_client, settings.KAFKA_SPAN_GENERATION_LOCK, settings.KAFKA_SPAN_GENERATION_TIMEOUT_SECONDS)
# def task_generate_spans_with_kafka_events():
#     # TODO: Make this in batches
#     resources = {}
#     def fetch_resource(type, uid):
#         if uid in resources:
#             return resources[uid]

#         resource = None
#         try:
#             if type == 'pod':
#                 resource = Pod.objects.using("dist_tracing_tasks").get(uid=uid)
#             elif type == 'service':
#                 resource = Service.objects.using("dist_tracing_tasks").get(uid=uid)
#             elif type == 'deployment':
#                 resource = Deployment.objects.using("dist_tracing_tasks").get(uid=uid)
#             elif type == 'daemonset':
#                 resource = DaemonSet.objects.using("dist_tracing_tasks").get(uid=uid)
#         except Exception as e:
#             logger.warn(f"Error while fetching {type} {uid}: {e}")
        
#         resources[uid] = resource
#         return resource

#     verbose_log(f"Generating spans with kafka events")
#     # start_time = datetime.now(UTC)
#     time_interval_sec = settings.SPAN_GENERATION_INTERVAL_SEC * 3
#     match_limit_sec = settings.DIST_TRACING_MATCH_LIMIT_SEC
#     request_extra_interval_sec = settings.DIST_TRACING_REQUEST_INTERVAL_EXTRA_SECONDS

#     # TODO: Add 1 day limit for traffic matching
#     sql = '''
# -- Step 1: Filter Traffic and Kafka Event
# WITH FilteredTraffic AS (
#     SELECT *
#     FROM dist_tracing_traffic
#     WHERE dist_tracing_traffic.span_exists = FALSE
#       AND date_created >= (CURRENT_TIMESTAMP - INTERVAL '%s seconds')
#       AND tcp_seq_num != 0
# ),
# FilteredKafkaEvent AS (
#     SELECT *
#     FROM core_kafkaevent
#     WHERE core_kafkaevent.span_exists = FALSE
#       AND start_time >= (CURRENT_TIMESTAMP - INTERVAL '%s seconds')
#       AND tcp_seq_num != 0
# )

# -- Step 2: Create CTEs for Egress and Ingress
# , Egress AS (
#     SELECT *
#     FROM FilteredTraffic
#     WHERE ingress = FALSE
# ),
# Ingress AS (
#     SELECT *
#     FROM FilteredTraffic
#     WHERE ingress = TRUE
# )

# -- Step 3: Join and Match Logic
# SELECT AR.id, E.id, I.id
# FROM FilteredKafkaEvent AR
#     JOIN Egress E ON AR.node_id = E.node_id AND AR.thread_id = E.thread_id AND AR.tcp_seq_num = E.tcp_seq_num AND AR.cluster = E.cluster_id
#     JOIN Ingress I ON AR.tcp_seq_num = I.tcp_seq_num AND AR.cluster = I.cluster_id
# WHERE
#     AR.to_url_outbound IS NULL
#     AND
#     CASE
#         WHEN E.timestamp > AR.start_time THEN
#             EXTRACT(EPOCH FROM (E.timestamp - AR.start_time)) <= %s + AR.latency / 1000000
#         ELSE
#             EXTRACT(EPOCH FROM (AR.start_time - E.timestamp)) <= %s + AR.latency / 1000000
#     END
# UNION

# SELECT AR.id, E.id, NULL
# FROM FilteredKafkaEvent AR
#     JOIN Egress E ON AR.node_id = E.node_id AND AR.thread_id = E.thread_id AND AR.tcp_seq_num = E.tcp_seq_num AND AR.cluster = E.cluster_id
# WHERE
#     CASE
#         WHEN E.timestamp > AR.start_time THEN
#             EXTRACT(EPOCH FROM (E.timestamp - AR.start_time)) <= %s + AR.latency / 1000000
#         ELSE
#             EXTRACT(EPOCH FROM (AR.start_time - E.timestamp)) <= %s + AR.latency / 1000000
#     END
#     AND
#     AR.to_url_outbound IS NOT NULL;

# '''
#     cursor = connection.cursor()
#     cursor.execute(sql, [time_interval_sec, time_interval_sec + request_extra_interval_sec, match_limit_sec, match_limit_sec, match_limit_sec, match_limit_sec])
#     rows = cursor.fetchall()
#     cursor.close()

#     matched_kafka_events = {}
#     matched_egress = {}
#     matched_ingress = {}

#     spans_to_create = []
#     traffic_to_update = []
#     kafka_events_to_update = []
#     verbose_log(f'Rows count: {len(rows)}')
#     for row in rows:
#         kafka_event_id = row[0]
#         egress_id = row[1]
#         ingress_id = row[2]

#         if kafka_event_id in matched_kafka_events:
#             continue

#         if egress_id in matched_egress:
#             continue

#         if ingress_id in matched_ingress:
#             continue

#         kafka_event = KafkaEvent.objects.using("dist_tracing_tasks").get(id=kafka_event_id)
#         egress = Traffic.objects.using("dist_tracing_tasks").get(id=egress_id)
#         ingress = Traffic.objects.using("dist_tracing_tasks").get(id=ingress_id) if ingress_id else None

#         # Turn nanoseconds to seconds
#         latency = kafka_event.latency / 1000000000

#         name = kafka_event.topic

#         start_time = egress.timestamp
#         end_time = egress.timestamp + timedelta(seconds=latency)

#         data = {
#             'name': name,
#             'start_time': egress.timestamp,
#             'end_time': egress.timestamp + timedelta(seconds=latency),
#             'cluster': str(egress.cluster_id),
#             'egress_tcp_num': egress.tcp_seq_num,
#             'egress_thread_id': egress.thread_id,
#             'ingress_tcp_num': ingress.tcp_seq_num if ingress else None,
#             'ingress_thread_id': ingress.thread_id if ingress else None,
#             'node_id': egress.node_id,
#             'attributes': {
#                 'from_ip': kafka_event.from_ip,
#                 'to_ip': kafka_event.to_ip,
#                 'from_port': kafka_event.from_port,
#                 'to_port': kafka_event.to_port,
#                 'topic': kafka_event.topic,
#                 'key': kafka_event.key,
#                 'partition': kafka_event.partition,
#                 'value': kafka_event.value,
#                 'event_type': kafka_event.type,
#                 # 'protocol': kafka_event.protocol,
#                 # 'status_code': kafka_event.status_code,
#                 # 'method': kafka_event.method,
#                 # 'path': kafka_event.path,
#             },
#         }

#         from_type = None
#         from_uid = None

#         to_type = None
#         to_uid = None

#         if kafka_event.from_uid_deployment:
#             from_type = 'deployment'
#             from_uid = kafka_event.from_uid_deployment
#         elif kafka_event.from_uid_daemonset:
#             from_type = 'daemonset'
#             from_uid = kafka_event.from_uid_daemonset
#         elif kafka_event.from_uid_pod:
#             from_type = 'pod'
#             from_uid = kafka_event.from_uid_pod
#         elif kafka_event.from_uid_service:
#             from_type = 'service'
#             from_uid = kafka_event.from_uid_service

#         if kafka_event.to_uid_deployment:
#             to_type = 'deployment'
#             to_uid = kafka_event.to_uid_deployment
#         elif kafka_event.to_uid_daemonset:
#             to_type = 'daemonset'
#             to_uid = kafka_event.to_uid_daemonset
#         elif kafka_event.to_uid_pod:
#             to_type = 'pod'
#             to_uid = kafka_event.to_uid_pod
#         elif kafka_event.to_uid_service:
#             to_type = 'service'
#             to_uid = kafka_event.to_uid_service
#         elif kafka_event.to_url_outbound:
#             to_type = 'outbound'
#             to_uid = kafka_event.to_url_outbound

#         from_uid = str(from_uid)
#         to_uid = str(to_uid)

#         data['attributes']['from_type'] = from_type
#         data['attributes']['from_uid'] = from_uid
#         from_resource = fetch_resource(from_type, from_uid)
#         if from_resource is None:
#             logger.warn(f"task_generate_spans_with_requests: from resource {from_uid} ({from_type}) is None. Request: {kafka_event}, egress: {egress.id}, ingress: {ingress.id if ingress else None}")
#             continue
            
#         data['attributes']['from_name'] = from_resource.name

#         data['attributes']['to_type'] = to_type
#         data['attributes']['to_uid'] = to_uid 
#         if to_type != 'outbound':
#             to_resource = fetch_resource(to_type, to_uid)
#             if to_resource is None:
#                 logger.warn(f"task_generate_spans_with_requests: to resource {to_uid} ({to_type}) is None. Request: {kafka_event}, egress: {egress.id}, ingress: {ingress.id if ingress else None}")
#                 continue

#             data['attributes']['to_name'] = to_resource.name
#         else:
#             data['attributes']['to_name'] = to_uid

#         spans_to_create.append(data)
#         # serializer = SpanCreateSerializer(data=data)
#         # if not serializer.is_valid():
#         #     continue

#         try:
#             # serializer.save()
#             egress.span_exists = True
#             traffic_to_update.append(egress)
#             # egress.save()
#             kafka_event.span_exists = True
#             kafka_events_to_update.append(kafka_event)
#             # request.save()
#             # verbose_log(f"Created span: {span}")
#             if ingress:
#                 ingress.span_exists = True
#                 traffic_to_update.append(ingress)
#                 # ingress.save()
#                 matched_ingress[ingress.id] = True
#             matched_egress[egress.id] = True
#             matched_kafka_events[kafka_event.id] = True
#         except Exception as e:
#             logger.error(f"Error {e} while creating span: {data}")
#             continue

#     # end_time = datetime.now(UTC)
#     # verbose_log(f"Spans are generated in {end_time - start_time} seconds")
#     batch_size = settings.BULK_BATCH_SIZE
#     verbose_log(f"Spans to create: {len(spans_to_create)}")

#     for i in range(0, len(spans_to_create), batch_size):
#         batch = spans_to_create[i:i+batch_size]
#         try:
#             Span.objects.using("dist_tracing_tasks").bulk_create([Span(**data) for data in batch])
#         except Exception as e:
#             # logger.warn(f"Error while bulk creating spans: {e}. Will create one by one")
#             for data in batch:
#                 try:
#                     Span.objects.using("dist_tracing_tasks").create(**data)
#                 except Exception as e:
#                     logger.error(f"Error while creating span: {e}. Failed to create: {data}")
#                     continue
                
#     verbose_log(f'{len(spans_to_create)} spans are generated')
#     verbose_log(f'Traffic to update: {len(traffic_to_update)}')
                
#     for i in range(0, len(traffic_to_update), batch_size):
#         batch = traffic_to_update[i:i+batch_size]
#         try:
#             Traffic.objects.using("dist_tracing_tasks").bulk_update(batch, ['span_exists'])
#         except Exception as e:
#             # logger.warn(f"Error while bulk updating traffic: {e}. Will update one by one")
#             for obj in batch:
#                 try:
#                     obj.save()
#                 except Exception as e:
#                     logger.error(f"Error while updating traffic: {e}. Failed to update: {obj}")
#                     continue
                
#     verbose_log(f'{len(traffic_to_update)} traffic are updated')
#     verbose_log(f'Kafka events to update: {len(kafka_events_to_update)}')
        
#     for i in range(0, len(kafka_events_to_update), batch_size):
#         batch = kafka_events_to_update[i:i+batch_size]
#         try:
#             KafkaEvent.objects.using("dist_tracing_tasks").bulk_update(batch, ['span_exists'])
#         except Exception as e:
#             # logger.warn(f"Error while bulk updating requests: {e}. Will update one by one")
#             for obj in batch:
#                 try:
#                     obj.save()
#                 except Exception as e:
#                     logger.error(f"Error while updating kafka event: {e}. Failed to update: {obj}")
#                     continue
    
#     verbose_log(f'{len(kafka_events_to_update)} kafka events are updated')


# @shared_task
# @with_redis_lock(redis_client, settings.TRACE_GENERATION_LOCK, settings.TRACE_GENERATION_TIMEOUT_SECONDS)
# def task_generate_traces():
#     # TODO: Make this in batches
#     verbose_log(f'Generating traces')
#     start_time = datetime.now(UTC)
#     time_interval_sec = settings.TRACE_GENERATION_INTERVAL_SEC * 3
#     lag_sec = settings.SPAN_GENERATION_INTERVAL_SEC * 2
#     match_limit_sec = settings.DIST_TRACING_MATCH_LIMIT_SEC
#     now = datetime.now(UTC)
#     # TODO: If the parent span in this queryset is not present in the last 60 seconds, we may miss that span and thus trace
#     spans = Span.objects.using("dist_tracing_tasks").filter(date_created__gte=now - timedelta(seconds=time_interval_sec + lag_sec), date_created__lte=now - timedelta(seconds=lag_sec)).order_by('start_time')

#     # verbose_log(f"Spans count: {spans.count()}")

#     # Organize spans by the thread id and pids of their egress traffic
#     # This is because we will iterate through spans
#     # We will try to match the ingress thread_id and pid of the span with the egress thread_id and pid of the other spans
#     egress_thread_ids = {}

#     total_sizes = {}
#     # traces_bulk_update = []
#     # spans_bulk_update = []
#     # Assuming traffic will be forwarded from the same thread id and pid of the receiving service
#     for span in spans:
#         key = span.egress_thread_id
#         if key not in egress_thread_ids:
#             egress_thread_ids[key] = []
#         egress_thread_ids[key].append(span)
    
#     trace_cache = {}
#     for parent_span in spans:
#         if parent_span.ingress_thread_id is None:
#             continue
#         same_thread_ids = egress_thread_ids.get(parent_span.ingress_thread_id, None)
#         if same_thread_ids is None:
#             # logger.warning(f"Span {span} has no matching thread id and pid")
#             continue
#         for child_span in same_thread_ids:
#             # verbose_log(f"Checking {span} and {other_span}")
#             # Ignore if the next span is already a part of a trace
#             if child_span.trace_id is not None:
#                 continue

#             # Ignore if they don't belong to the same cluster
#             if parent_span.cluster != child_span.cluster:
#                 continue

#             if parent_span.end_time < child_span.start_time:
#                 continue

#             if child_span.id == parent_span.id:
#                 continue
#             # Other span should be later than the current span
#             if child_span.start_time <= parent_span.start_time:
#                 continue
#             # There shouldn't be a gap of more than 5 seconds (dynamic on env) between the current span and other span
#             if child_span.start_time - parent_span.start_time > timedelta(seconds=match_limit_sec):
#                 # verbose_log(f'{span} and {other_span} has a time difference of more than {match_limit_sec} seconds')
#                 continue

#             # verbose_log(f'Will merge {parent_span} and {child_span}. Source span has trace: {parent_span.trace_id is not None}')

#             try:
#                 _, child_span, parent_span = link_span_to_parent(child_span, parent_span, trace_cache)
#                 child_span.save()
#                 parent_span.save()
#                 # spans_bulk_update.append(child_span)
#                 # spans_bulk_update.append(parent_span)
#                 # traces_bulk_update.append(trace)
#                 cluster_id = str(parent_span.cluster)
#                 if cluster_id not in total_sizes:
#                     total_sizes[cluster_id] = 0
#                 total_sizes[cluster_id] += len(jsonpickle.encode(child_span))
#                 total_sizes[cluster_id] += len(jsonpickle.encode(parent_span))
#             except Exception as e:
#                 logger.error(f"Error while merging {parent_span} and {child_span}: {traceback.format_exc()}")
#                 continue

#     traces_bulk_update = list(trace_cache.values())
#     try:
#         Trace.objects.using("dist_tracing_tasks").bulk_update(traces_bulk_update, ['span_count', 'attributes', 'start_time', 'end_time'])
#     except Exception as e:
#         for trace in traces_bulk_update:
#             try:
#                 trace.save()
#             except Exception as e:
#                 logger.error(f"Error while creating trace: {e}. Failed to create: {trace}")
#                 continue
        
#     # Assign traces to left-out spans
#     for parent_span in spans:
#         if parent_span.trace_id is not None:
#             continue

#         try:
#             trace, parent_span = get_or_create_trace_of_span(parent_span, trace_cache)
#             parent_span.save()
#             # spans_bulk_update.append(parent_span)
#             cluster_id = str(parent_span.cluster)
#             if cluster_id not in total_sizes:
#                 total_sizes[cluster_id] = 0
#             total_sizes[cluster_id] += len(jsonpickle.encode(parent_span))
#         except Exception as e:
#             logger.error(f"Error while assigning trace to span {parent_span}: {e}")
#             continue
        
#     for cluster_id, size in total_sizes.items():
#         redis_client.hincrby(settings.TRACES_SIZE_KEY, cluster_id, size)


#     # try:
#     #     Span.objects.using("dist_tracing_tasks").bulk_update(spans_bulk_update, ['parent_id', 'trace_id'])
#     # except Exception as e:
#     #     for span in spans_bulk_update:
#     #         try:
#     #             span.save()
#     #         except Exception as e:
#     #             logger.error(f"Error while updating span: {e}. Failed to update: {span}")
#     #             continue

#     # end_time = datetime.now(UTC)
#     # verbose_log(f"Traces are generated in {end_time - start_time} seconds")


# @shared_task
# @with_redis_lock(redis_client, settings.DIST_TRACING_CLEAR_TASK_LOCK, settings.DIST_TRACING_CLEAR_TASK_TIMEOUT_SECONDS)
# def task_clear_old_dist_tracing_resources():
#     verbose_log('Deleting old dist_tracing resources')

#     time_interval_minutes = settings.DIST_TRACING_TRAFFIC_RETENTION_MINUTES
#     delete_batch_size = settings.DIST_TRACING_DELETE_BATCH_SIZE
#     now_time = datetime.now(UTC)
#     start_time = datetime.now(UTC)
#     cluster_uids = set()
#     for cluster in Cluster.objects.using("dist_tracing_tasks").all().iterator():
#         cluster_uids.add(cluster.uid)
#         cluster_owner = find_team_owner_if_exists(cluster.user)
#         time_interval_days = cluster_owner.plan.get('data_retention_days', 14)
#         verbose_log(f'Deleting old spans for cluster {cluster} with data retention days: {time_interval_days}')
#         batch_num = 0
#         while True:
#             batch_num += 1
#             span_qs = Span.objects.using("dist_tracing_tasks").filter(cluster=cluster.uid, date_created__lte=now_time - timedelta(days=time_interval_days))[:delete_batch_size]
#             if not span_qs:
#                 break
#             try:
#                 span_qs._raw_delete(span_qs.db)
#                 verbose_log(f'Deleted spans. Batch: {batch_num}')
#             except Exception as e:
#                 logger.warn(f'Error deleting spans: {e}. Will delete one by one')
#                 for span in span_qs:
#                     try:
#                         span.delete()
#                     except Exception as e:
#                         logger.error(f'Error deleting span: {e}. Failed to delete: {span}')
#                         continue


#     verbose_log(f'Old spans are deleted in {datetime.now(UTC) - start_time} seconds')

#     start_time = datetime.now(UTC)

#     for cluster in Cluster.objects.using("dist_tracing_tasks").all().iterator():
#         cluster_owner = find_team_owner_if_exists(cluster.user)
#         time_interval_days = cluster_owner.plan.get('data_retention_days', 14)
#         verbose_log(f'Deleting old traces for cluster {cluster} with data retention days: {time_interval_days}')
#         batch_num = 0
#         while True:
#             batch_num += 1
#             trace_qs = Trace.objects.using("dist_tracing_tasks").filter(cluster=cluster.uid, start_time__lte=now_time - timedelta(days=time_interval_days))[:delete_batch_size]
#             if not trace_qs:
#                 break
#             try:
#                 trace_qs._raw_delete(trace_qs.db)
#                 verbose_log(f'Deleted traces. Batch: {batch_num}')
#             except Exception as e:
#                 logger.warn(f'Error deleting traces: {e}. Will delete one by one')
#                 for trace in trace_qs:
#                     try:
#                         trace.delete()
#                     except Exception as e:
#                         logger.error(f'Error deleting trace: {e}. Failed to delete: {trace}')
#                         continue

#     verbose_log(f'Old traces are deleted in {datetime.now(UTC) - start_time} seconds')

#     start_time = datetime.now(UTC)

#     # Truncate is not used as it also loses the traffic of the last 10 minutes
#     # cursor = connection.cursor()
#     # cursor.execute('TRUNCATE dist_tracing_traffic CASCADE')
#     # cursor.close()
    
#     delete_lock = threading.Lock()
#     batch_num = 0
#     while True:
#         batch_num += 1
#         traffic_qs = Traffic.objects.using("dist_tracing_tasks").filter(timestamp__lte=now_time - timedelta(minutes=time_interval_minutes))[:delete_batch_size]
#         if not traffic_qs:
#             break
        
#         with delete_lock:
#             try:
#                 traffic_qs._raw_delete(traffic_qs.db)
#                 verbose_log(f'Deleted traffic. Batch: {batch_num}')
#             except Exception as e:
#                 logger.warn(f'Error deleting traffic: {e}. Will delete one by one')
#                 for traffic in traffic_qs:
#                     try:
#                         traffic.delete()
#                     except Exception as e:
#                         logger.error(f'Error deleting traffic: {e}. Failed to delete: {traffic}')
#                         continue

#     verbose_log(f'Old traffic are deleted in {datetime.now(UTC) - start_time} seconds')


# @shared_task
# # @with_redis_lock(redis_client, settings.TRAFFIC_TASK_LOCK, settings.TRAFFIC_TASK_TIMEOUT_SECONDS)
# def task_write_traffic_from_redis_to_db():
#     verbose_log('Writing traffic from redis to db')
    
#     start_time = datetime.now(UTC)

#     batch_size = settings.DIST_TRACING_TRAFFIC_BATCH_SIZE
#     queue_name = settings.DIST_TRACING_TRAFFIC_QUEUE
#     length = 0

#     # Looping through the list in batches
#     while True:
#         # Check if x seconds have passed
#         current_time = datetime.now(UTC)
#         if (current_time - start_time).total_seconds() >= settings.WRITE_TASK_TIMEOUT_SEC:
#             verbose_log(f'write_requests_from_redis_to_db: {settings.WRITE_TASK_TIMEOUT_SEC} seconds have passed, exiting...')
#             break
        
#         bulk_create = []
#         # Calculate end index for the batch
#         # Retrieve the batch from Redis
#         try:
#             pipe = redis_client.pipeline()
#             pipe.llen(queue_name)
#             pipe.lrange(queue_name, 0, batch_size - 1)
#             pipe.ltrim(queue_name, batch_size, -1)
#             pipe.llen(queue_name)
#             before_len, batch, trim_count, after_len = pipe.execute()
#             verbose_log(f'Got batch of {len(batch)} traffic from redis. Also trimmed the queue for {trim_count} from {before_len} to {after_len}')
#             batch = [json.loads(item) for item in batch]  # Decoding bytes to strings
#             length += len(batch)
#             if not batch:
#                 break
#             for item in batch:
#                 try:
#                     item['timestamp'] = datetime.fromtimestamp(int(item['timestamp']) / 1000, UTC)
#                     bulk_create.append(Traffic(**item))
#                 except Exception as e:
#                     logger.error(f"An error occurred while processing traffic from redis to the db: {e}. Traffic: {item}")
#                     continue

#             # Process your batch here
#             try:
#                 Traffic.objects.using("dist_tracing_tasks").bulk_create(bulk_create)
#             except Exception as e:
#                 logger.warn(f"An error occurred while bulk writing traffic from redis to the db: {e}. Processing each instance one by one")
#                 for obj in bulk_create:
#                     try:
#                         obj.save()
#                     except Exception as e:
#                         logger.error(f"An error occurred while writing traffic from redis to the db: {e}. Failed to save: {obj}")
#                         continue

#         except Exception as e:
#             logger.error(f"An error occurred while writing traffic from redis to the db: {e}")
#             break

#     verbose_log(f'{length} traffic from redis to db is written in {datetime.now(UTC) - start_time} seconds')


# @shared_task
# @with_redis_lock(redis_client, settings.ORPHANS_DIST_TRACING_CLEAR_TASK_LOCK, settings.ORPHANS_DIST_TRACING_CLEAR_TASK_TIMEOUT_SECONDS)
# def dist_tracing_clear_orphans():
#     existent_clusters = set(Cluster.objects.using("dist_tracing_tasks").values_list('uid', flat=True))
    
#     distinct_cluster_uids = set(Span.objects.using("dist_tracing_tasks").values_list('cluster', flat=True).distinct())
#     non_existent_clusters = distinct_cluster_uids - existent_clusters
#     verbose_log(f'Deleting spans without clusters: {non_existent_clusters}')
#     for cluster_uid in non_existent_clusters:
#         batch_num = 0
#         while True:
#             batch_num += 1
#             span_qs = Span.objects.using("dist_tracing_tasks").filter(cluster=cluster_uid)[:delete_batch_size]
#             if not span_qs:
#                 break
#             try:
#                 span_qs._raw_delete(span_qs.db)
#                 verbose_log(f'Deleted spans without cluster. Batch: {batch_num}')
#             except Exception as e:
#                 logger.warn(f'Error deleting spans without cluster: {e}. Will delete one by one')
#                 for span in span_qs:
#                     try:
#                         span.delete()
#                     except Exception as e:
#                         logger.error(f'Error deleting span without cluster: {e}. Failed to delete: {span}')
#                         continue
            
#     verbose_log(f'Deleted spans without clusters: {non_existent_clusters}')
    
#     distinct_cluster_uids = set(Trace.objects.using("dist_tracing_tasks").values_list('cluster', flat=True).distinct())
#     non_existent_clusters = distinct_cluster_uids - existent_clusters
#     for cluster_uid in non_existent_clusters:
#         batch_num = 0
#         while True:
#             batch_num += 1
#             trace_qs = Trace.objects.using("dist_tracing_tasks").filter(cluster=cluster_uid)[:delete_batch_size]
#             if not trace_qs:
#                 break
#             try:
#                 trace_qs._raw_delete(trace_qs.db)
#                 verbose_log(f'Deleted traces without cluster. Batch: {batch_num}')
#             except Exception as e:
#                 logger.warn(f'Error deleting traces without cluster: {e}. Will delete one by one')
#                 for trace in trace_qs:
#                     try:
#                         trace.delete()
#                     except Exception as e:
#                         logger.error(f'Error deleting trace without cluster: {e}. Failed to delete: {trace}')
#                         continue
    
#     verbose_log(f'Deleted traces without clusters: {non_existent_clusters}')