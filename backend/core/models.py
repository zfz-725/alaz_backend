import uuid

from accounts.models import Team, User
# from core.validate_utils import (PERIODIC_DICT, create_periodic_task_of_scheduler,
                                #  get_interval_schedule, validate_get_interval)
from django.db import models
from django.db.models.signals import post_delete
from django.dispatch import receiver
from django_celery_beat.models import PeriodicTask

"""
    Periodic:
        Celery task checks TestPlanTemplate with is_periodic=True. If exceeds, create Test instance and run. 
    
    Manual Load:
        If config_data["manual_load"] is not null:
            Hammer does not care request_count, duration and load_type
    frontend_extra_fields = Do not send to hammer manager. Its used by frontend only. 

"""

class RequestType():
    KAFKA_EVENT = "KAFKA"
    REQUEST = "REQUEST"


class Setting(models.Model):

    name = models.CharField(max_length=100, blank=False, null=False, unique=True)
    # gpt_prompts = models.JSONField(default=list)
    prometheus_queries = models.JSONField(default=dict)
    alert_messages = models.JSONField(default=dict)
    # predefined_alerts = models.JSONField(default=list)
    # ceo_welcome = models.JSONField(default=dict)
    onprem_key = models.UUIDField(editable=False, serialize=False, null=True, blank=True)
    # latest alaz version from github
    latest_alaz_version = models.CharField(max_length=100, blank=True, null=True)
    used_metrics = models.JSONField(default=list)
    bloat_notifier_channel = models.CharField(max_length=100, null=True, blank=True)

    def __str__(self) -> str:
        return f"{self.name}"

class Cluster(models.Model):
    uid = models.UUIDField(default=uuid.uuid4, primary_key=True, editable=False, serialize=False)
    name = models.CharField(max_length=100, blank=False, null=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=False, blank=False)
    monitoring_id = models.CharField(max_length=100, blank=False, null=False, unique=True)
    fresh = models.BooleanField(default=True)
    team = models.ForeignKey(Team, on_delete=models.CASCADE, null=True, blank=True)
    last_data_ts = models.JSONField(null=False, blank=True)
    is_alive = models.BooleanField(default=False)
    last_heartbeat = models.DateTimeField(null=True, blank=True)
    heartbeat_mail_sent = models.BooleanField(default=False)
    alaz_info = models.JSONField(default=dict, null=True, blank=True)
    alaz_version = models.CharField(max_length=100, blank=True, null=True)
    instances = models.JSONField(default=dict, null=True, blank=True)

    date_updated = models.DateTimeField(auto_now=True)
    date_created = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"{self.name}"

    def save(self, *args, **kwargs):
        if self.last_data_ts is None:
            self.last_data_ts = {'active': {}, 'passive': {}}

        super(Cluster, self).save(*args, **kwargs)


class Deployment(models.Model):
    uid = models.UUIDField(default=uuid.uuid4, primary_key=True, editable=True, serialize=False)
    cluster = models.UUIDField(null=False, blank=False)
    name = models.CharField(max_length=100, blank=False, null=False)
    namespace = models.CharField(max_length=100, blank=False, null=False)
    replicas = models.PositiveIntegerField(default=1)
    deleted = models.BooleanField(default=False)

    date_updated = models.DateTimeField(auto_now=True)
    date_created = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"{self.uid}"


class Service(models.Model):
    uid = models.UUIDField(default=uuid.uuid4, primary_key=True, editable=True, serialize=False)
    cluster = models.UUIDField(null=False, blank=False)
    name = models.CharField(max_length=100, blank=False, null=False)
    namespace = models.CharField(max_length=100, blank=False, null=False)
    type = models.CharField(max_length=100, blank=False, null=False)
    cluster_ips = models.JSONField(default=list, null=False, blank=False)
    deleted = models.BooleanField(default=False)
    ports = models.JSONField(default=list, null=False, blank=False)

    date_updated = models.DateTimeField(auto_now=True)
    date_created = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"{self.uid}"


class ReplicaSet(models.Model):
    uid = models.UUIDField(default=uuid.uuid4, primary_key=True, editable=True, serialize=False)
    cluster = models.UUIDField(null=False, blank=False)
    name = models.CharField(max_length=100, blank=False, null=False)
    namespace = models.CharField(max_length=100, blank=False, null=False)
    owner = models.UUIDField(null=True, blank=True)
    replicas = models.PositiveIntegerField(null=False, blank=False)
    deleted = models.BooleanField(default=False)

    date_updated = models.DateTimeField(auto_now=True)
    date_created = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"{self.uid}"


class DaemonSet(models.Model):
    uid = models.UUIDField(default=uuid.uuid4, primary_key=True, editable=True, serialize=False)
    cluster = models.UUIDField(null=False, blank=False)
    name = models.CharField(max_length=100, blank=False, null=False)
    namespace = models.CharField(max_length=100, blank=False, null=False)
    deleted = models.BooleanField(default=False)

    date_updated = models.DateTimeField(auto_now=True)
    date_created = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"{self.uid}"


class StatefulSet(models.Model):
    uid = models.UUIDField(default=uuid.uuid4, primary_key=True, editable=True, serialize=False)
    cluster = models.UUIDField(null=False, blank=False)
    name = models.CharField(max_length=100, blank=False, null=False)
    namespace = models.CharField(max_length=100, blank=False, null=False)
    deleted = models.BooleanField(default=False)

    date_updated = models.DateTimeField(auto_now=True)
    date_created = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"{self.uid}"

class Pod(models.Model):
    uid = models.UUIDField(default=uuid.uuid4, primary_key=True, editable=True, serialize=False)
    cluster = models.UUIDField(null=False, blank=False)
    name = models.CharField(max_length=100, blank=False, null=False)
    namespace = models.CharField(max_length=100, blank=False, null=False)
    ip = models.CharField(max_length=15, blank=True, null=True)
    deleted = models.BooleanField(default=False)
    replicaset_owner = models.UUIDField(null=True, blank=True)
    daemonset_owner = models.UUIDField(null=True, blank=True)
    statefulset_owner = models.UUIDField(null=True, blank=True)

    date_updated = models.DateTimeField(auto_now=True)
    date_created = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"{self.uid}"

class Request(models.Model):
    id = models.BigAutoField(primary_key=True)
    cluster = models.UUIDField(null=True, blank=True)
    start_time = models.DateTimeField(null=False, blank=False)
    latency = models.BigIntegerField(null=False, blank=False)
    from_ip = models.CharField(max_length=15, blank=False, null=False)
    to_ip = models.CharField(max_length=15, blank=False, null=False)
    from_port = models.PositiveIntegerField(null=False, blank=False)
    to_port = models.PositiveIntegerField(null=False, blank=False)

    from_uid_pod = models.UUIDField(null=True, blank=True)
    from_uid_service = models.UUIDField(null=True, blank=True)
    from_uid_deployment = models.UUIDField(null=True, blank=True)
    from_uid_daemonset = models.UUIDField(null=True, blank=True)
    from_uid_statefulset = models.UUIDField(null=True, blank=True)

    to_uid_pod = models.UUIDField(null=True, blank=True)
    to_uid_service = models.UUIDField(null=True, blank=True)
    to_url_outbound = models.CharField(max_length=1024, blank=True, null=True)
    to_uid_deployment = models.UUIDField(null=True, blank=True)
    to_uid_daemonset = models.UUIDField(null=True, blank=True)
    to_uid_statefulset = models.UUIDField(null=True, blank=True)

    protocol = models.CharField(max_length=30, blank=False, null=False)
    status_code = models.PositiveIntegerField(null=True, blank=True)
    fail_reason = models.CharField(max_length=100, blank=True, null=True)
    method = models.CharField(max_length=20, blank=False, null=False)
    path = models.CharField(max_length=1248, blank=True, null=False)
    non_simplified_path = models.CharField(max_length=1248, blank=True, null=False)
    tls = models.BooleanField(default=False)

    # tcp_seq_num = models.BigIntegerField(null=True, blank=True)
    # node_id = models.CharField(max_length=100, blank=True, null=True)
    # thread_id = models.BigIntegerField(null=True, blank=True)
    # span_exists = models.BooleanField(default=False)

    # date_updated = models.DateTimeField(auto_now=True)
    # date_created = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        # TODO: Try timescaledb
        index_together = [('start_time', )]
        indexes = [
            models.Index(fields=['cluster']),
            # models.Index(fields=['path']),
            # models.Index(fields=['protocol']),
            # models.Index(fields=['status_code']),
        ]

    def __str__(self) -> str:
        return f"{self.id}"


class KafkaEvent(models.Model):
    id = models.BigAutoField(primary_key=True)
    # cluster = models.ForeignKey(Cluster, on_delete=models.SET_NULL, null=True, blank=True)
    cluster = models.UUIDField(null=True, blank=True)
    start_time = models.DateTimeField(null=False, blank=False)
    latency = models.BigIntegerField(null=False, blank=False)
    from_ip = models.CharField(max_length=15, blank=False, null=False)
    to_ip = models.CharField(max_length=15, blank=False, null=False)
    from_port = models.PositiveIntegerField(null=False, blank=False)
    to_port = models.PositiveIntegerField(null=False, blank=False)

    from_uid_pod = models.UUIDField(null=True, blank=True)
    from_uid_service = models.UUIDField(null=True, blank=True)
    from_uid_deployment = models.UUIDField(null=True, blank=True)
    from_uid_daemonset = models.UUIDField(null=True, blank=True)
    from_uid_statefulset = models.UUIDField(null=True, blank=True)

    to_uid_pod = models.UUIDField(null=True, blank=True)
    to_uid_service = models.UUIDField(null=True, blank=True)
    to_url_outbound = models.CharField(max_length=1024, blank=True, null=True)
    to_uid_deployment = models.UUIDField(null=True, blank=True)
    to_uid_daemonset = models.UUIDField(null=True, blank=True)
    to_uid_statefulset = models.UUIDField(null=True, blank=True)

    topic = models.CharField(max_length=100, blank=False, null=False)
    partition = models.PositiveIntegerField(null=False, blank=False)
    key = models.CharField(max_length=100, blank=True, null=True)
    value = models.CharField(max_length=1024, blank=False, null=False)
    type = models.CharField(max_length=100, blank=False, null=False)
    tls = models.BooleanField(default=False)

    # tcp_seq_num = models.BigIntegerField(null=True, blank=True)
    # node_id = models.CharField(max_length=100, blank=True, null=True)
    # thread_id = models.BigIntegerField(null=True, blank=True)
    # span_exists = models.BooleanField(default=False)

    # date_updated = models.DateTimeField(auto_now=True)
    # date_created = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        index_together = [('start_time', )]
        indexes = [
            models.Index(fields=['cluster']),
            # models.Index(fields=['path']),
            # models.Index(fields=['protocol']),
            # models.Index(fields=['status_code']),
        ]

    def __str__(self) -> str:
        return f"{self.id}"
    

class Connection(models.Model):
    id = models.BigAutoField(primary_key=True)
    # cluster = models.ForeignKey(Cluster, on_delete=models.SET_NULL, null=True, blank=True)
    cluster = models.UUIDField(null=True, blank=True)

    timestamp = models.DateTimeField(null=False, blank=False)
    from_ip = models.CharField(max_length=15, blank=False, null=False)
    to_ip = models.CharField(max_length=15, blank=False, null=False)
    from_port = models.PositiveIntegerField(null=False, blank=False)
    to_port = models.PositiveIntegerField(null=False, blank=False)

    from_uid_pod = models.UUIDField(null=True, blank=True)
    from_uid_service = models.UUIDField(null=True, blank=True)
    from_uid_deployment = models.UUIDField(null=True, blank=True)
    from_uid_daemonset = models.UUIDField(null=True, blank=True)
    from_uid_statefulset = models.UUIDField(null=True, blank=True)

    to_uid_pod = models.UUIDField(null=True, blank=True)
    to_uid_service = models.UUIDField(null=True, blank=True)
    to_url_outbound = models.CharField(max_length=1024, blank=True, null=True)
    to_uid_deployment = models.UUIDField(null=True, blank=True)
    to_uid_daemonset = models.UUIDField(null=True, blank=True)
    to_uid_statefulset = models.UUIDField(null=True, blank=True)

    date_updated = models.DateTimeField(auto_now=True)
    date_created = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        index_together = [('timestamp', )]
        indexes = [
            models.Index(fields=['cluster']),
            # models.Index(fields=['path']),
            # models.Index(fields=['protocol']),
        ]

    def __str__(self) -> str:
        return f"{self.id}"


class Endpoint(models.Model):
    uid = models.UUIDField(default=uuid.uuid4, primary_key=True, editable=True, serialize=False)
    cluster = models.UUIDField(null=False, blank=False)
    name = models.CharField(max_length=100, blank=False, null=False)
    namespace = models.CharField(max_length=100, blank=False, null=False)
    addresses = models.JSONField(default=list, null=False, blank=False)
    deleted = models.BooleanField(default=False)

    date_updated = models.DateTimeField(auto_now=True)
    date_created = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"{self.uid}"


class Container(models.Model):
    uid = models.UUIDField(default=uuid.uuid4, primary_key=True, editable=True, serialize=False)
    cluster = models.UUIDField(null=True, blank=True)
    pod = models.UUIDField(null=False, blank=False)
    name = models.CharField(max_length=100, blank=False, null=False)
    image = models.CharField(max_length=1000, blank=True, null=True)
    namespace = models.CharField(max_length=100, blank=False, null=False)
    ports = models.JSONField(default=list, null=True, blank=True)
    has_logs = models.BooleanField(default=False)
    container_nums = models.JSONField(default=list, null=False, blank=True)

    date_updated = models.DateTimeField(auto_now=True)
    date_created = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"{self.uid}"


class K8sEvent(models.Model):
# {
#         "EventName": "pml-celery-worker-2-deployment-5f7f64f4f5-sgz2h.17ea8673c7091c07",
#         "Kind": "Pod",
#         "Namespace": "anteon-staging",
#         "Name": "pml-celery-worker-2-deployment-5f7f64f4f5-sgz2h",
#         "Uid": "4d807c79-52dd-4246-984c-8c9f1945338e",
#         "Reason": "Started",
#         "Message": "Started container pml-celery-worker-2",
#         "Count": 123,
#         "FirstTimestamp": "2024-08-11 00:53:59 +0000 UTC",
#         "LastTimestamp": "2024-08-16 10:34:30 +0000 UTC"
# }

    cluster = models.UUIDField(null=True, blank=True)
    event_name = models.CharField(unique=True, max_length=100, blank=False, null=False)
    kind = models.CharField(max_length=100, blank=False, null=False)
    namespace = models.CharField(max_length=100, blank=False, null=False)
    name = models.CharField(max_length=100, blank=False, null=False)
    uid = models.UUIDField(null=False, blank=False)
    reason = models.CharField(max_length=100, blank=False, null=False)
    message = models.CharField(max_length=1000, blank=False, null=False)
    count = models.PositiveIntegerField(null=False, blank=False)
    first_timestamp = models.DateTimeField(null=False, blank=False)
    last_timestamp = models.DateTimeField(null=False, blank=False)