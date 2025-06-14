import asyncio
from django.db import transaction
import datetime
import json
import time
from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from django.db import connection
from django.utils import timezone
import jwt
import redis
from celery.utils.debug import sample_mem, memdump
import requests
import websockets
from accounts.utils import find_team_owner_if_exists
from core.serializers import ClusterDirectSerializer, PodDirectSerializer
from core.requester import GithubRequester, MyBackendRequester
from core.utils import (
    convert_bytes_to_megabytes,
    get_onprem_retention_days,
    insert_requests,
    notify_bloat,
    save_cluster,
    with_redis_lock,
)
from django.db.models import Count, Q

from core.models import (
    Cluster,
    Container,
    DaemonSet,
    Deployment,
    Endpoint,
    KafkaEvent,
    Pod,
    ReplicaSet,
    Request,
    Service,
    Connection,
    Setting,
)

GB_TO_MB = 1024

logger = get_task_logger(__name__)
redis_client = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=0,
    charset="utf-8",
    decode_responses=True,
)
delete_batch_size = settings.REQUESTS_DELETE_BULK_BATCH_SIZE

# if settings.ANTEON_ENV != 'onprem':
#     from core.clickhouse_utils import get_clickhouse_client, mark_logs_as_processing, get_log_storage_usage, mark_logs_as_processed
#     clickhouse_client = get_clickhouse_client()


def verbose_log(log):
    if settings.CELERY_VERBOSE_LOG:
        logger.info(log)


@shared_task
@with_redis_lock(
    redis_client,
    settings.REQUESTS_CLEAR_TASK_LOCK,
    settings.REQUESTS_CLEAR_TASK_TIMEOUT_SECONDS,
)
def clear_alaz_requests():
    from core.models import Request

    # sample_count = 0
    # sample_count = sample_mem_wrapper(sample_count)
    verbose_log("Deleting old requests")
    if settings.ANTEON_ENV != 'onprem':
        cluster_uids = set()
        for cluster in Cluster.objects.using("core_tasks").all().iterator():
            cluster_uids.add(cluster.uid)
        cluster_owner = find_team_owner_if_exists(cluster.user)
        retention_days = cluster_owner.plan.get("data_retention_days", 14)
        verbose_log(
            f"Deleting old requests for cluster {cluster.uid}. Retention days: {retention_days}"
        )
        for cluster in Cluster.objects.all().iterator():
            cluster_uids.add(cluster.uid)
            cluster_owner = find_team_owner_if_exists(cluster.user)
            retention_days = cluster_owner.plan.get("data_retention_days", 14)
            verbose_log(
                f"Deleting old requests for cluster {cluster.uid}. Retention days: {retention_days}"
            )
            threshold = timezone.now() - timezone.timedelta(days=retention_days)
            batch_num = 0
            while True:
                batch_num += 1
                requests = Request.objects.using("default").filter(
                    cluster=cluster.uid, start_time__lte=threshold
                )[:delete_batch_size]
                if not requests:
                    break
                try:
                    requests._raw_delete(requests.db)
                    verbose_log(f"Deleted requests. Batch: {batch_num}")
                except Exception as e:
                    logger.warn(f"Error deleting requests: {e}. Deleting one by one")
                    for request in requests:
                        try:
                            request.delete()
                        except Exception as exc:
                            logger.error(f"Error deleting request {request}: {exc}")
    else:
        retention_days = get_onprem_retention_days()
        threshold = timezone.now() - timezone.timedelta(days=retention_days)
        batch_num = 0
        while True:
            batch_num += 1
            requests = Request.objects.using("core_tasks").filter(start_time__lte=threshold)[:delete_batch_size]
            if not requests:
                break
            try:
                requests._raw_delete(requests.db)
                verbose_log(f"Deleted requests. Batch: {batch_num}")
            except Exception as e:
                logger.warn(f"Error deleting requests: {e}. Deleting one by one")
                for request in requests:
                    try:
                        request.delete()
                    except Exception as exc:
                        logger.error(f"Error deleting request {request}: {exc}")

        
    verbose_log(f"Deleted old requests")
    # memdump(sample_count)


@shared_task
@with_redis_lock(
    redis_client,
    settings.KAFKA_EVENTS_CLEAR_TASK_LOCK,
    settings.KAFKA_EVENTS_CLEAR_TASK_TIMEOUT_SECONDS,
)
def clear_kafka_events():
    from core.models import KafkaEvent

    # sample_count = 0
    # sample_count = sample_mem_wrapper(sample_count)
    verbose_log("Deleting old kafka events")
    if settings.ANTEON_ENV != 'onprem':
        cluster_uids = set()
        for cluster in Cluster.objects.using("core_tasks").all().iterator():
            cluster_uids.add(cluster.uid)
            cluster_owner = find_team_owner_if_exists(cluster.user)
            retention_days = cluster_owner.plan.get("data_retention_days", 14)
            verbose_log(
                f"Deleting old kafka events for cluster {cluster.uid}. Retention days: {retention_days}"
            )
            threshold = timezone.now() - timezone.timedelta(days=retention_days)
            batch_num = 0
            while True:
                batch_num += 1
                kafka_events = KafkaEvent.objects.using("core_tasks").filter(
                    cluster=cluster.uid, start_time__lte=threshold
                )[:delete_batch_size]
                if not kafka_events:
                    break
                try:
                    kafka_events._raw_delete(kafka_events.db)
                    verbose_log(f"Deleted kafka events. Batch: {batch_num}")
                except Exception as e:
                    logger.warn(f"Error deleting kafka events: {e}. Deleting one by one")
                    for kafka_event in kafka_events:
                        try:
                            kafka_event.delete()
                        except Exception as exc:
                            logger.error(f"Error deleting kafka event {kafka_event}: {exc}")
    else:
        retention_days = get_onprem_retention_days()
        threshold = timezone.now() - timezone.timedelta(days=retention_days)
        batch_num = 0
        while True:
            batch_num += 1
            kafka_events = KafkaEvent.objects.using("core_tasks").filter(start_time__lte=threshold)[:delete_batch_size]
            if not kafka_events:
                break
            try:
                kafka_events._raw_delete(kafka_events.db)
                verbose_log(f"Deleted kafka events. Batch: {batch_num}")
            except Exception as e:
                logger.warn(f"Error deleting kafka events: {e}. Deleting one by one")
                for kafka_event in kafka_events:
                    try:
                        kafka_event.delete()
                    except Exception as exc:
                        logger.error(f"Error deleting kafka event {kafka_event}: {exc}")
                        
    verbose_log(f"Deleted old kafka events")
    # memdump(sample_count)


@shared_task
@with_redis_lock(
    redis_client,
    settings.CONNECTIONS_CLEAR_TASK_LOCK,
    settings.CONNECTIONS_CLEAR_TASK_TIMEOUT_SECONDS,
)
def clear_connections():
    from core.models import Connection

    verbose_log("Deleting old connections")
    if settings.ANTEON_ENV != 'onprem':
        cluster_uids = set()
        for cluster in Cluster.objects.using("core_tasks").all().iterator():
            cluster_uids.add(cluster.uid)
            cluster_owner = find_team_owner_if_exists(cluster.user)
            retention_days = cluster_owner.plan.get("data_retention_days", 14)
            verbose_log(
                f"Deleting old connections for cluster {cluster.uid}. Retention days: {retention_days}"
            )
            threshold = timezone.now() - timezone.timedelta(days=retention_days)
            batch_num = 0
            while True:
                batch_num += 1
                connections = Connection.objects.using("core_tasks").filter(
                    cluster=cluster.uid, timestamp__lte=threshold
                )[:delete_batch_size]
                if not connections:
                    break
                try:
                    connections._raw_delete(connections.db)
                    verbose_log(f"Deleted connections. Batch: {batch_num}")
                except Exception as e:
                    logger.warn(f"Error deleting connections: {e}. Deleting one by one")
                    for connection in connections:
                        try:
                            connection.delete()
                        except Exception as exc:
                            logger.error(f"Error deleting connection {connection}: {exc}")
    else:
        retention_days = get_onprem_retention_days()
        threshold = timezone.now() - timezone.timedelta(days=retention_days)
        batch_num = 0
        while True:
            batch_num += 1
            connections = Connection.objects.using("core_tasks").filter(timestamp__lte=threshold)[:delete_batch_size]
            if not connections:
                break
            try:
                connections._raw_delete(connections.db)
                verbose_log(f"Deleted connections. Batch: {batch_num}")
            except Exception as e:
                logger.warn(f"Error deleting connections: {e}. Deleting one by one")
                for connection in connections:
                    try:
                        connection.delete()
                    except Exception as exc:
                        logger.error(f"Error deleting connection {connection}: {exc}")
    verbose_log(f"Deleted old connections")


@shared_task
def set_clusters_with_old_heartbeats_as_not_alive():
    from core.models import Cluster
    from pytz import timezone
    from datetime import timedelta

    time_limit_seconds = settings.CURRENT_WINDOW_SECONDS
    threshold = datetime.datetime.now(datetime.UTC) - timedelta(
        seconds=time_limit_seconds
    )
    for cluster in Cluster.objects.using("core_tasks").all():
        if (
            not cluster.last_heartbeat
            or cluster.last_heartbeat.replace(tzinfo=timezone("UTC")) < threshold
        ):
            cluster.is_alive = False
            save_cluster(cluster)


@shared_task
def delete_expired_instances_of_clusters():
    from core.models import Cluster
    from pytz import timezone
    from datetime import timedelta

    instance_expiraton_time_days = settings.INSTANCE_EXPIRATION_TIME_DAYS
    instance_threshold = (
        datetime.datetime.now(datetime.UTC)
        - timedelta(days=instance_expiraton_time_days)
    ).timestamp() * 1000  # in ms
    for cluster in Cluster.objects.using("core_tasks").all():
        to_delete = []
        for instance in cluster.instances:
            if cluster.instances[instance] < instance_threshold:
                to_delete.append(instance)
        for instance in to_delete:
            del cluster.instances[instance]
        save_cluster(cluster)


@shared_task
def update_latest_instances_of_clusters():
    from core.models import Cluster
    from pytz import timezone
    from datetime import timedelta

    current_window_seconds = settings.CURRENT_WINDOW_SECONDS
    instance_threshold = int(
        (
            datetime.datetime.now(datetime.UTC)
            - timedelta(seconds=current_window_seconds)
        ).timestamp()
        * 1000
    )
    for cluster in Cluster.objects.using("core_tasks").all():
        to_delete = []
        for status, instances in cluster.last_data_ts.items():
            for instance in instances:
                latest_ts = None
                for _, ts in instances[instance].items():
                    if not latest_ts or ts > latest_ts:
                        latest_ts = ts
                if latest_ts < instance_threshold:
                    to_delete.append((status, instance))
        for status, instance in to_delete:
            del cluster.last_data_ts[status][instance]
        save_cluster(cluster)


@shared_task
# @with_redis_lock(redis_client, settings.KAFKA_EVENTS_TASK_LOCK, settings.KAFKA_EVENTS_TASK_TIMEOUT_SECONDS)
def write_kafka_events_from_redis_to_db():
    verbose_log(f"Will write kafka events from redis to db")
    start_time = datetime.datetime.now(datetime.UTC)

    queue_name = settings.KAFKA_EVENTS_QUEUE
    batch_size = settings.BULK_BATCH_SIZE
    length = 0

    # Looping through the list in batches
    while True:
        # Check if x seconds have passed
        current_time = datetime.datetime.now(datetime.UTC)
        if (
            current_time - start_time
        ).total_seconds() >= settings.WRITE_TASK_TIMEOUT_SEC:
            verbose_log(
                f"write_kafka_events_from_redis_to_db: {settings.WRITE_TASK_TIMEOUT_SEC} seconds have passed, exiting..."
            )
            break

        bulk_create = []
        # Calculate end index for the batch
        # Retrieve the batch from Redis
        try:
            pipe = redis_client.pipeline()
            pipe.llen(queue_name)
            pipe.lrange(queue_name, 0, batch_size - 1)
            pipe.ltrim(queue_name, batch_size, -1)
            pipe.llen(queue_name)
            before_len, batch, trim_count, after_len = pipe.execute()
            verbose_log(
                f"Got batch of {len(batch)} kafka events from redis. Also trimmed the queue for {trim_count} from {before_len} to {after_len}"
            )
            batch = [json.loads(item) for item in batch]  # Decoding bytes to strings
            length += len(batch)
            if not batch:
                break
            for item in batch:
                try:
                    item["start_time"] = datetime.datetime.fromtimestamp(
                        int(item["start_time"]) / 1000, datetime.UTC
                    )
                    bulk_create.append(KafkaEvent(**item))
                except Exception as e:
                    logger.error(
                        f"An error occurred while processing kafka event from redis to the db: {e}. Request: {item}"
                    )
                    continue

            # Process your batch here
            try:
                start_time = datetime.datetime.now(datetime.UTC)
                KafkaEvent.objects.using("core_tasks").bulk_create(bulk_create, ignore_conflicts=True)
                # verbose_log(f'Processed batch: {len(bulk_create)} in {datetime.datetime.now(datetime.UTC) - start_time} seconds')
            except Exception as e:
                logger.warn(
                    f"An error occurred while bulk writing kafka events from redis to the db: {e}. Processing each instance one by one"
                )
                for obj in bulk_create:
                    try:
                        obj.save()
                    except Exception as e:
                        logger.error(
                            f"An error occurred while writing kafka event from redis to the db: {e}. Failed to save: {obj}"
                        )
                        continue

        except Exception as e:
            logger.error(
                f"PREMATURE EXIT: An error occurred while writing kafka events from redis to the db: {e}"
            )
            break

    verbose_log(
        f"{length} kafka events are written from redis to db in {datetime.datetime.now(datetime.UTC) - start_time} seconds"
    )


@shared_task
# @with_redis_lock(redis_client, settings.CONNECTIONS_TASK_LOCK, settings.CONNECTIONS_TASK_TIMEOUT_SECONDS)
def write_connections_from_redis_to_db():
    verbose_log(f"Will write connections from redis to db")
    start_time = datetime.datetime.now(datetime.UTC)

    queue_name = settings.CONNECTIONS_QUEUE
    batch_size = settings.BULK_BATCH_SIZE
    length = 0

    # Looping through the list in batches
    while True:
        # Check if x seconds have passed
        current_time = datetime.datetime.now(datetime.UTC)
        if (
            current_time - start_time
        ).total_seconds() >= settings.WRITE_TASK_TIMEOUT_SEC:
            verbose_log(
                f"write_connections_from_redis_to_db: {settings.WRITE_TASK_TIMEOUT_SEC} seconds have passed, exiting..."
            )
            break

        bulk_create = []
        # Calculate end index for the batch
        # Retrieve the batch from Redis
        try:
            pipe = redis_client.pipeline()
            pipe.llen(queue_name)
            pipe.lrange(queue_name, 0, batch_size - 1)
            pipe.ltrim(queue_name, batch_size, -1)
            pipe.llen(queue_name)
            before_len, batch, trim_count, after_len = pipe.execute()
            verbose_log(
                f"Got batch of {len(batch)} connections from redis. Also trimmed the queue for {trim_count} from {before_len} to {after_len}"
            )
            batch = [json.loads(item) for item in batch]  # Decoding bytes to strings
            length += len(batch)
            if not batch:
                break
            for item in batch:
                try:
                    item["timestamp"] = datetime.datetime.fromtimestamp(
                        int(item["timestamp"]) / 1000, datetime.UTC
                    )
                    bulk_create.append(Connection(**item))
                except Exception as e:
                    logger.error(
                        f"An error occurred while processing connection from redis to the db: {e}. Connection: {item}"
                    )
                    continue

            # Process your batch here
            try:
                start_time = datetime.datetime.now(datetime.UTC)
                Connection.objects.using("core_tasks").bulk_create(bulk_create, ignore_conflicts=True)
                # verbose_log(f'Processed batch: {len(bulk_create)} in {datetime.datetime.now(datetime.UTC) - start_time} seconds')
            except Exception as e:
                logger.warn(
                    f"An error occurred while bulk writing connections from redis to the db: {e}. Processing each instance one by one"
                )
                for obj in bulk_create:
                    try:
                        obj.save()
                    except Exception as e:
                        logger.error(
                            f"An error occurred while writing connection from redis to the db: {e}. Failed to save: {obj}"
                        )
                        continue

            # Remove the processed elements from the list

        except Exception as e:
            logger.error(
                f"PREMATURE EXIT: An error occurred while writing connections from redis to the db: {e}"
            )
            break

    verbose_log(
        f"{length} connections are written from redis to db in {datetime.datetime.now(datetime.UTC) - start_time} seconds"
    )


@shared_task
def clear_unused_resources():
    days = settings.RESOURCES_UNUSED_CLEAR_DAYS
    time_limit = timezone.now() - timezone.timedelta(days=days)
    verbose_log(f"Will clear unused resources.")

    # TODO: date_updated'indan beri retention süresi geçenleri sil
    # TODO: Silmeden once bir kontrol
    pods = Pod.objects.using("core_tasks").filter(
        date_created__lt=time_limit, deleted=True
    )
    # verbose_log(f'Will delete {pods.count()} pods')
    try:
        pods.delete()
    except Exception as e:
        logger.warn(f"Bulk delete failed: {e}, deleting one by one")
        for pod in pods:
            try:
                pod.delete()
            except Exception as exc:
                logger.error(f"Error deleting pod {pod}: {exc}")

    deployments = Deployment.objects.using("core_tasks").filter(
        date_created__lt=time_limit, deleted=True
    )
    # verbose_log(f'Will delete {deployments.count()} deployments')
    try:
        deployments.delete()
    except Exception as e:
        logger.warn(f"Bulk delete failed: {e}, deleting one by one")
        for deployment in deployments:
            try:
                deployment.delete()
            except Exception as exc:
                logger.error(f"Error deleting deployment {deployment}: {exc}")

    replica_sets = ReplicaSet.objects.using("core_tasks").filter(
        date_created__lt=time_limit, deleted=True
    )
    # verbose_log(f'Will delete {replica_sets.count()} replica_sets')
    try:
        replica_sets.delete()
    except Exception as e:
        logger.warn(f"Bulk delete failed: {e}, deleting one by one")
        for replica_set in replica_sets:
            try:
                replica_set.delete()
            except Exception as exc:
                logger.error(f"Error deleting replica_set {replica_set}: {exc}")

    daemon_sets = DaemonSet.objects.using("core_tasks").filter(
        date_created__lt=time_limit, deleted=True
    )
    # verbose_log(f'Will delete {daemon_sets.count()} daemon_sets')
    try:
        daemon_sets.delete()
    except Exception as e:
        logger.warn(f"Bulk delete failed: {e}, deleting one by one")
        for daemon_set in daemon_sets:
            try:
                daemon_set.delete()
            except Exception as exc:
                logger.error(f"Error deleting daemon_set {daemon_set}: {exc}")

    services = Service.objects.using("core_tasks").filter(
        date_created__lt=time_limit, deleted=True
    )
    # verbose_log(f'Will delete {services.count()} services')
    try:
        services.delete()
    except Exception as e:
        logger.warn(f"Bulk delete failed: {e}, deleting one by one")
        for service in services:
            try:
                service.delete()
            except Exception as exc:
                logger.error(f"Error deleting service {service}: {exc}")

    endpoints = Endpoint.objects.using("core_tasks").filter(
        date_created__lt=time_limit, deleted=True
    )
    # verbose_log(f'Will delete {endpoints.count()} endpoints')
    try:
        endpoints.delete()
    except Exception as e:
        logger.warn(f"Bulk delete failed: {e}, deleting one by one")
        for endpoint in endpoints:
            try:
                endpoint.delete()
            except Exception as exc:
                logger.error(f"Error deleting endpoint {endpoint}: {exc}")


def sample_mem_wrapper(count=0):
    sample_mem()
    return count + 1


@shared_task
def cache_alaz_resources_in_redis():
    # TODO: Make this in batches
    verbose_log(f"Will cache alaz resources in redis")
    start_time = timezone.now()
    # sample_count = 0
    # sample_count = sample_mem_wrapper(sample_count)
    pods_to_deployments_hset = settings.PODS_TO_DEPLOYMENTS_HSET
    pods_to_daemonsets_hset = settings.PODS_TO_DAEMONSETS_HSET
    pods_to_statefulsets_hset = settings.PODS_TO_STATEFULSETS_HSET

    pods_to_deployments_query = """
    SELECT 
    core_pod.uid, core_deployment.uid
    FROM core_pod
    JOIN core_replicaset on core_replicaset.uid = core_pod.replicaset_owner
    JOIN core_deployment on core_deployment.uid = core_replicaset.owner;
    """

    cursor = connection.cursor()
    cursor.execute(pods_to_deployments_query)
    pods_to_deployments = cursor.fetchall()
    # sample_count = sample_mem_wrapper(sample_count)
    verbose_log(f"Got {len(pods_to_deployments)} pods to deployments")

    redis_client.delete(pods_to_deployments_hset)
    for pod_uid, deployment_uid in pods_to_deployments:
        redis_client.hset(pods_to_deployments_hset, str(pod_uid), str(deployment_uid))

    # sample_count = sample_mem_wrapper(sample_count)

    pods_to_daemonsets_query = """
    SELECT
    core_pod.uid, core_daemonset.uid
    FROM core_pod
    JOIN core_daemonset on core_pod.daemonset_owner = core_daemonset.uid
    """

    cursor.execute(pods_to_daemonsets_query)
    pods_to_daemonsets = cursor.fetchall()
    # sample_count = sample_mem_wrapper(sample_count)
    verbose_log(f"Got {len(pods_to_deployments)} pods to daemonsets")

    redis_client.delete(pods_to_daemonsets_hset)
    for pod_uid, daemonset_uid in pods_to_daemonsets:
        redis_client.hset(pods_to_daemonsets_hset, str(pod_uid), str(daemonset_uid))

    pods_to_statefulsets_query = """
    SELECT
    core_pod.uid, core_statefulset.uid
    FROM core_pod
    JOIN core_statefulset on core_pod.statefulset_owner = core_statefulset.uid
    """

    cursor.execute(pods_to_statefulsets_query)
    pods_to_statefulsets = cursor.fetchall()
    cursor.close()
    verbose_log(f"Got {len(pods_to_statefulsets)} pods to statefulsets")

    redis_client.delete(pods_to_statefulsets_hset)
    for pod_uid, statefulset_uid in pods_to_statefulsets:
        redis_client.hset(pods_to_statefulsets_hset, str(pod_uid), str(statefulset_uid))

    # sample_count = sample_mem_wrapper(sample_count)
    verbose_log(
        f"Cached alaz resources in redis in {datetime.datetime.now(datetime.UTC) - start_time} seconds"
    )
    # memdump(sample_count)


@shared_task
def update_version_from_github():
    requester = GithubRequester()
    version_data = requester.get_alaz_version()

    # Process the response and extract version number
    version_number = version_data.get("tag_name")
    # Update latest_alaz_version field in Setting model
    if version_number:
        setting, created = Setting.objects.using("core_tasks").get_or_create(name="default")
        setting.latest_alaz_version = version_number
        setting.save()


@shared_task
def delete_cluster(cluster_uid):
    try:
        cluster = Cluster.objects.using("core_tasks").get(uid=cluster_uid)
        name = cluster.name
        logger.info(f"Deleting cluster {cluster_uid}-{name}")
        with transaction.atomic():
            cluster.delete()
        verbose_log(f"Deleted cluster {cluster_uid}-{name}")
    except Exception as e:
        logger.error(f"Error deleting cluster {cluster_uid}: {e}")


# @shared_task
# def update_containers_without_logs():
#     from core.clickhouse_utils import get_last_2_container_ids

#     # TODO: Remove when logs is ready on onprem
#     if settings.ANTEON_ENV == "onprem":
#         return
#     rows = get_last_2_container_ids()

#     containers = Container.objects.using("core_tasks").all()

#     mapped_containers = {}
#     for row in rows:
#         # monitoring_id = str(row[0])
#         pod_uid = str(row[1])
#         container_name = row[2]
#         # current_container_num = row[3]
#         # prev_container_num = row[4]

#         key = (pod_uid, container_name)
#         mapped_containers[key] = True

#     # print(mapped_containers)
#     containers_to_update = []
#     for container in containers:
#         key = (str(container.pod), container.name)
#         if key not in mapped_containers:
#             container.has_logs = False
#             containers_to_update.append(container)

#     Container.objects.using("core_tasks").bulk_update(containers_to_update, ["has_logs"])


@shared_task
def check_log_lag(monitoring_id, deployment_name, container_name):
    # TODO: Remove when logs is ready on onprem
    if settings.ANTEON_ENV == "onprem":
        return

    async def connect_websocket(uri, seconds):
        async with websockets.connect(uri) as websocket:
            start_time = time.time()
            prev_message = None
            lag_exists = False
            while time.time() - start_time < seconds:
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                message = int(json.loads(response)["message"])
                if prev_message and message - prev_message != 1:
                    lag_exists = True
                    logger.error(f"Log lag: {message} and {prev_message}")
                prev_message = message
            if not prev_message:
                logger.error(
                    f"No message received from monitoring_id-deployment_name-container_name: {monitoring_id}-{deployment_name}-{container_name}"
                )
                return
            if not lag_exists:
                logger.info("No log lag")

    def get_websocket_uri(monitoring_id, pod_uid, container_name, container_num):
        # Connect to the other WebSocket
        payload = {
            "username": settings.ALAZ_BACKEND_USERNAME,
            "password": settings.ALAZ_BACKEND_PASSWORD,
        }

        websocket_secret_key = settings.WEBSOCKET_SECRET_KEY

        # Encode the token
        token = jwt.encode(payload, websocket_secret_key, algorithm="HS256")

        uri = f"ws://localhost:8008/ws/logs/{monitoring_id}/{token}/{pod_uid}/{container_name}/{container_num}/"
        return uri

    # Replace 'wss://echo.websocket.org' with the WebSocket URI you want to connect to
    # websocket_uri = 'wss://echo.websocket.org'
    # duration = 10  # Connection duration in seconds
    # asyncio.run(connect_websocket(websocket_uri, duration))

    try:
        cluster = Cluster.objects.using("core_tasks").get(monitoring_id=monitoring_id)
    except Cluster.DoesNotExist:
        logger.error(f"Cluster not found: {monitoring_id}")
        return
    except Exception as e:
        logger.error(f"Error getting cluster: {e}")
        return

    deployment_qs = Deployment.objects.using("core_tasks").filter(
        cluster=cluster.uid, name=deployment_name
    ).order_by("-date_created")
    if not deployment_qs.exists():
        logger.error(f"Deployment not found: {monitoring_id}-{deployment_name}")
        return

    deployment = deployment_qs.first()
    replicasets_qs = ReplicaSet.objects.using("core_tasks").filter(
        cluster=cluster.uid, owner=deployment.uid
    ).order_by("-date_created")
    if not replicasets_qs.exists():
        logger.error(
            f"No replicasets found for monitoring_id-deployment: {monitoring_id}-{deployment_name}"
        )
        return

    replicaset_uids = replicasets_qs.values_list("uid", flat=True)
    pods_qs = Pod.objects.using("core_tasks").filter(
        cluster=cluster.uid, replicaset_owner__in=replicaset_uids
    ).order_by("-date_created")
    if not pods_qs.exists():
        logger.error(
            f"No pod found for monitoring_id-deployment: {monitoring_id}-{deployment_name}"
        )
        return

    pod_uids = pods_qs.values_list("uid", flat=True)

    container_qs = Container.objects.using("core_tasks").filter(
        cluster=cluster.uid, pod__in=pod_uids, name=container_name
    ).order_by("-date_created")

    if not container_qs.exists():
        logger.error(
            f"No container found for monitoring_id-deployment-container_name: {monitoring_id}-{deployment_name}-{container_name}"
        )
        return

    target_container = None
    for container in container_qs:
        if container.has_logs:
            target_container = container
            break

    if not target_container:
        logger.error(
            f"Containers of given deployment do not have log stream: {monitoring_id}-{deployment_name}-{container_name}"
        )
        return

    pod_uid = target_container.pod

    container_nums = container.container_nums
    if len(container_nums) < 1:
        logger.error(
            f"No container nums for container: {monitoring_id}-{pod_uid}-{container_name}"
        )
        return

    latest_num = max(container_nums)
    uri = get_websocket_uri(monitoring_id, pod_uid, container_name, latest_num)
    print(uri)
    duration_seconds = 10
    asyncio.run(connect_websocket(uri, duration_seconds))


@shared_task
@with_redis_lock(
    redis_client,
    settings.ORPHANS_CORE_CLEAR_TASK_LOCK,
    settings.ORPHANS_CORE_CLEAR_TASK_TIMEOUT_SECONDS,
)
def core_clear_orphans():
    existent_clusters = set(
        Cluster.objects.using("core_tasks").values_list("uid", flat=True)
    )
    distinct_cluster_uids = set(
        Request.objects.using("core_tasks").values_list("cluster", flat=True).distinct()
    )
    non_existent_clusters = distinct_cluster_uids - existent_clusters
    verbose_log(f"Deleting requests without clusters: {non_existent_clusters}")
    for cluster_uid in non_existent_clusters:
        batch_num = 0
        while True:
            batch_num += 1
            requests = Request.objects.using("core_tasks").filter(cluster=cluster_uid)[
                :delete_batch_size
            ]
            if not requests:
                break
            try:
                requests._raw_delete(requests.db)
                verbose_log(f"Deleted requests without cluster. Batch: {batch_num}")
            except Exception as e:
                logger.warn(
                    f"Error deleting requests without cluster: {e}. Deleting one by one"
                )
                for request in requests:
                    try:
                        request.delete()
                    except Exception as exc:
                        logger.error(
                            f"Error deleting request without cluster {request}: {exc}"
                        )

    verbose_log(f"Deleted requests without clusters: {non_existent_clusters}")

    distinct_cluster_uids = set(
        Connection.objects.using("core_tasks").values_list("cluster", flat=True).distinct()
    )
    non_existent_clusters = distinct_cluster_uids - existent_clusters
    verbose_log(f"Deleting connections without clusters: {non_existent_clusters}")
    for cluster_uid in non_existent_clusters:
        batch_num = 0
        while True:
            batch_num += 1
            kafka_events = Connection.objects.using("core_tasks").filter(
                cluster=cluster_uid
            )[:delete_batch_size]
            if not kafka_events:
                break
            try:
                kafka_events._raw_delete(kafka_events.db)
                verbose_log(f"Deleted connections without cluster. Batch: {batch_num}")
            except Exception as e:
                logger.warn(
                    f"Error deleting connections without cluster: {e}. Deleting one by one"
                )
                for kafka_event in kafka_events:
                    try:
                        kafka_event.delete()
                    except Exception as exc:
                        logger.error(
                            f"Error deleting connection without cluster {kafka_event}: {exc}"
                        )
    verbose_log(f"Deleted connections without clusters: {non_existent_clusters}")

    distinct_cluster_uids = set(
        KafkaEvent.objects.using("core_tasks").values_list("cluster", flat=True).distinct()
    )
    non_existent_clusters = distinct_cluster_uids - existent_clusters
    verbose_log(f"Deleting kafka events without clusters: {non_existent_clusters}")
    for cluster_uid in non_existent_clusters:
        batch_num = 0
        while True:
            batch_num += 1
            kafka_events = KafkaEvent.objects.using("core_tasks").filter(
                cluster=cluster_uid
            )[:delete_batch_size]
            if not kafka_events:
                break
            try:
                kafka_events._raw_delete(kafka_events.db)
                verbose_log(f"Deleted kafka events without cluster. Batch: {batch_num}")
            except Exception as e:
                logger.warn(
                    f"Error deleting kafka events without cluster: {e}. Deleting one by one"
                )
                for kafka_event in kafka_events:
                    try:
                        kafka_event.delete()
                    except Exception as exc:
                        logger.error(
                            f"Error deleting kafka event without cluster {kafka_event}: {exc}"
                        )

    verbose_log(f"Deleted kafka events without clusters: {non_existent_clusters}")


@shared_task
def remove_duplicate_containers():
    # Group containers by (name, namespace, cluster, pod). Find the groups having > 1 containers

    container_groups = (
        Container.objects.using("core_tasks").values("cluster", "pod", "name", "namespace")
        .annotate(count=Count("uid"))
        .order_by("-count")
    )
    duplicate_containers = container_groups.filter(count__gt=1)

    container_uids_to_delete = []
    for group in duplicate_containers:
        containers_in_group = Container.objects.using("core_tasks").filter(
            cluster=group["cluster"],
            pod=group["pod"],
            name=group["name"],
            namespace=group["namespace"],
        ).order_by("-date_created")
        container_uids_to_delete.extend(
            [container.uid for container in containers_in_group[1:]]
        )

    Container.objects.using("core_tasks").filter(uid__in=container_uids_to_delete).delete()


@shared_task
def sync_clusters():
    # Send a request to backend, fetch all clusters and create/update them

    backend_requester = MyBackendRequester()
    cluster_uids_response = backend_requester.get_cluster_uids()
    cluster_uids = [cluster["uid"] for cluster in cluster_uids_response]

    # Find clusters to delete

    Cluster.objects.using("core_tasks").select_for_update().filter(~Q(uid__in=cluster_uids)).delete()

    # Find clusters to create

    existing_clusters = set(
        map(lambda x: str(x), Cluster.objects.using("core_tasks").values_list("uid", flat=True))
    )
    bulk_create = []

    for cluster in cluster_uids_response:
        if cluster["uid"] not in existing_clusters:
            cluster["user_id"] = cluster["user"]
            cluster["team_id"] = cluster["team"]
            cluster["last_data_ts"] = {"active": {}, "passive": {}}
            del cluster["user"]
            del cluster["team"]
            bulk_create.append(Cluster(**cluster))

    if bulk_create:
        try:
            Cluster.objects.using("core_tasks").bulk_create(bulk_create)
        except Exception as e:
            for cluster in bulk_create:
                try:
                    cluster.save()
                except Exception as e:
                    logger.error(f"Error saving cluster: {e}")


@shared_task
def cache_pods_and_clusters():
    pods = Pod.objects.using("core_tasks").all()
    clusters = Cluster.objects.using("core_tasks").all()
    containers = Container.objects.using("core_tasks").all()

    pods_key = settings.PODS_REDIS_KEY
    clusters_key = settings.CLUSTERS_REDIS_KEY
    containers_key = settings.CONTAINERS_REDIS_KEY

    # Clear all keys
    redis_client.delete(pods_key)
    redis_client.delete(clusters_key)

    containers = redis_client.hgetall(containers_key)
    # TODO: These create and updates could be done in bulk in the future
    for container_key, container_data in containers.items():
        container_data = json.loads(container_data)
        updated = container_data["updated"]
        container_nums = container_data["container_nums"]
        has_logs = container_data["has_logs"]
        try:
            cluster_id, pod_uid, name, namespace = container_key.split("_")
        except Exception as e:
            logger.error(f"Error splitting container key: {e}")
            continue
        if updated:
            container_qs = Container.objects.using("core_tasks").filter(
                cluster=cluster_id, pod=pod_uid, name=name, namespace=namespace
            )
            if container_qs.exists():
                container = container_qs.first()
                container.container_nums = container_nums
                container.has_logs = has_logs
                container.save()
            else:
                container = Container(
                    cluster=cluster_id,
                    pod=pod_uid,
                    name=name,
                    namespace=namespace,
                    container_nums=container_nums,
                    has_logs=has_logs,
                )
                container.save()

    redis_client.delete(containers_key)

    for pod in pods:
        pod_data = PodDirectSerializer(pod).data
        pod_data["uid"] = str(pod_data["uid"])
        pod_data["cluster"] = str(pod_data["cluster"])
        redis_client.hset(pods_key, str(pod.uid), json.dumps(pod_data))

    for cluster in clusters:
        cluster_data = ClusterDirectSerializer(cluster).data
        cluster_data["uid"] = str(cluster_data["uid"])
        cluster_data["user"] = str(cluster_data["user"])
        cluster_data["team"] = str(cluster_data["team"])
        redis_client.hset(clusters_key, str(cluster.uid), json.dumps(cluster_data))

    containers = Container.objects.using("core_tasks").all()

    for container in containers:
        container_data = {
            "updated": False,
            "container_nums": container.container_nums,
            "has_logs": container.has_logs,
        }
        if "_" in container.namespace:
            logger.error(f"Container namespace has underscore: {container.namespace}")
            continue
        redis_client.hset(
            containers_key,
            f"{container.cluster}_{container.pod}_{container.name}_{container.namespace}",
            json.dumps(container_data),
        )


# if settings.TCPDUMP_ENABLED:

#     @shared_task
#     @with_redis_lock(
#         redis_client,
#         settings.TCPDUMP_TO_CLICKHOUSE_TASK_LOCK,
#         settings.TCPDUMP_TO_CLICKHOUSE_TASK_TIMEOUT_SECONDS,
#     )
#     def write_tcp_dump_to_clickhouse():
#         from core.clickhouse_utils import get_clickhouse_client, insert_tcpdump

#         if not settings.TCPDUMP_ENABLED:
#             return
#         verbose_log(f"Will write tcpdump from redis to db")
#         start_time = datetime.datetime.now(datetime.UTC)
#         client = get_clickhouse_client()

#         queue_name = settings.TCPDUMP_QUEUE
#         length = redis_client.llen(queue_name)
#         batch_size = settings.TCPDUMP_BATCH_SIZE

#         # Looping through the list in batches
#         for start_index in range(0, length, batch_size):
#             bulk_create = []
#             # Calculate end index for the batch
#             end_index = min(start_index + batch_size - 1, length - 1)

#             # Retrieve the batch from Redis
#             try:
#                 batch = redis_client.lrange(queue_name, start_index, end_index)
#                 batch = [
#                     json.loads(item) for item in batch
#                 ]  # Decoding bytes to strings
#                 for item in batch:
#                     try:
#                         bulk_create.append(item)
#                     except Exception as e:
#                         logger.error(
#                             f"An error occurred while processing tcpdump from redis to clickhouse: {e}. Tcpdump: {item}"
#                         )
#                         continue

#                 # Process your batch here
#                 try:
#                     start_time = datetime.datetime.now(datetime.UTC)
#                     insert_tcpdump(bulk_create, client)
#                     # Request.objects.using("core_tasks").bulk_create(bulk_create, ignore_conflicts=True)
#                     # verbose_log(f'Processed batch: {len(bulk_create)} in {datetime.datetime.now(datetime.UTC) - start_time} seconds')
#                 except Exception as e:
#                     logger.error(
#                         f"An error occurred while bulk writing tcpdump from redis to clickhouse: {e}. Processing each instance one by one"
#                     )

#             except Exception as e:
#                 logger.error(
#                     f"PREMATURE EXIT: An error occurred while writing tcpdump from redis to clickhouse: {e}"
#                 )
#                 break

#         # Clear the written traffic

#         redis_client.ltrim(queue_name, length, -1)

#         verbose_log(
#             f"{length} tcpdump are written from redis to clickhouse in {datetime.datetime.now(datetime.UTC) - start_time} seconds"
#         )


# Monitoring tasks are separated as one's failure would not disrupt the others
if settings.ANTEON_ENV != "onprem":

    @shared_task
    def check_redis_size():
        if not settings.BLOAT_NOTIFICATION_ENABLED:
            return
        type = "redis"
        info = redis_client.info()
        memory_usage_bytes = info["used_memory"]
        memory_usage_mb = round(convert_bytes_to_megabytes(memory_usage_bytes), 3)
        memory_usage_limit = settings.REDIS_MEMORY_LIMIT_MB
        if memory_usage_mb >= memory_usage_limit:
            notify_bloat(type, usage=memory_usage_mb, limit=memory_usage_limit)

    @shared_task
    def check_postgres_size():
        if not settings.BLOAT_NOTIFICATION_ENABLED:
            return
        type = "postgres"
        total_size_bytes = 0
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 
                    SUM(pg_database_size(d.datname)) AS total_size_bytes
                FROM 
                    pg_database d;
            """
            )
            total_size_bytes = cursor.fetchone()[0]

        total_size_mb = round(convert_bytes_to_megabytes(total_size_bytes), 3)
        memory_size_limit = settings.POSTGRES_STORAGE_LIMIT_MB
        if total_size_mb >= memory_size_limit:
            notify_bloat(type, usage=total_size_mb, limit=memory_size_limit)

    @shared_task
    def check_rabbitmq_usage():
        if not settings.BLOAT_NOTIFICATION_ENABLED:
            return
        type = "rabbitmq"
        url = f"http://{settings.CELERY_BROKER_HOST}:15672/api/queues"
        response = requests.get(
            url, auth=(settings.RABBITMQ_USERNAME, settings.RABBITMQ_PASSWORD)
        )
        message_limit = settings.RABBITMQ_MESSAGE_LIMIT
        queues = response.json()
        for queue in queues:
            queue_name = queue["name"]
            messages = queue.get("messages", 0)
            if messages >= message_limit:
                notify_bloat(
                    type, usage=messages, limit=message_limit, queue_name=queue_name
                )

    # @shared_task
    # def check_clickhouse_size():
    #     if not settings.BLOAT_NOTIFICATION_ENABLED:
    #         return
    #     type = "clickhouse"
    #     query = """SELECT 
    #     SUM(bytes_on_disk) AS total_size_bytes
    # FROM 
    #     system.parts;"""
    #     rows = clickhouse_client.query(query).result_rows
    #     size_bytes = rows[0][0]
    #     size_mb = round(convert_bytes_to_megabytes(size_bytes), 3)
    #     if size_mb >= settings.CLICKHOUSE_STORAGE_LIMIT_MB:
    #         notify_bloat(
    #             type, usage=size_mb, limit=settings.CLICKHOUSE_STORAGE_LIMIT_MB
    #         )


@shared_task
def clear_redis_if_full():
    if not settings.REDIS_CLEAR_ENABLED:
        return
    requests_queue = settings.REQUESTS_QUEUE
    traffic_queue = settings.DIST_TRACING_TRAFFIC_QUEUE

    requests_size = redis_client.memory_usage(requests_queue)
    traffic_size = redis_client.memory_usage(traffic_queue)

    if requests_size is None:
        requests_size = 0
    if traffic_size is None:
        traffic_size = 0

    total_size = requests_size + traffic_size
    memory_usage_mb = convert_bytes_to_megabytes(total_size)
    if memory_usage_mb >= settings.REDIS_CLEAR_SIZE_LIMIT_GB * GB_TO_MB:
        redis_client.delete(settings.REQUESTS_QUEUE)
        redis_client.delete(settings.DIST_TRACING_TRAFFIC_QUEUE)
        logger.error(f"Redis storage is above 20 GBs. Cleared all queues")


# if settings.ANTEON_ENV != 'onprem':
#     @shared_task
#     def set_log_usage():
#         if settings.ANTEON_ENV == "onprem":
#             return
        
#         logs_size_key = settings.LOGS_SIZE_KEY
        
#         mark_logs_as_processing(clickhouse_client)
        
#         clusters = Cluster.objects.using("core_tasks").all()
        
#         monitoring_id_to_cluster_id_map = {}
#         for cluster in clusters:
#             monitoring_id_to_cluster_id_map[str(cluster.monitoring_id)] = str(cluster.uid)
        
#         storage_usages = get_log_storage_usage(clickhouse_client)
#         logger.info(f"Log storage usages: {storage_usages}")

#         for monitoring_id, total_size_bytes in storage_usages:
#             cluster_id = monitoring_id_to_cluster_id_map.get(monitoring_id)
#             if not cluster_id:
#                 logger.error(f"Cluster not found for monitoring_id: {monitoring_id}")
#                 continue
            
#             redis_client.hincrby(logs_size_key, cluster_id, total_size_bytes)
        
#         mark_logs_as_processed(clickhouse_client)