import uuid
from django.core.management.base import BaseCommand
from core.models import Cluster, K8sEvent
from faker import Faker
import random
from datetime import datetime, UTC, timedelta

fake = Faker()

# class K8sEvent(models.Model):
# # {
# #         "EventName": "pml-celery-worker-2-deployment-5f7f64f4f5-sgz2h.17ea8673c7091c07",
# #         "Kind": "Pod",
# #         "Namespace": "anteon-staging",
# #         "Name": "pml-celery-worker-2-deployment-5f7f64f4f5-sgz2h",
# #         "Uid": "4d807c79-52dd-4246-984c-8c9f1945338e",
# #         "Reason": "Started",
# #         "Message": "Started container pml-celery-worker-2",
# #         "Count": 123,
# #         "FirstTimestamp": "2024-08-11 00:53:59 +0000 UTC",
# #         "LastTimestamp": "2024-08-16 10:34:30 +0000 UTC"
# # }

#     cluster = models.UUIDField(null=True, blank=True)
#     event_name = models.CharField(unique=True, max_length=100, blank=False, null=False)
#     kind = models.CharField(max_length=100, blank=False, null=False)
#     namespace = models.CharField(max_length=100, blank=False, null=False)
#     name = models.CharField(max_length=100, blank=False, null=False)
#     uid = models.UUIDField(null=False, blank=False)
#     reason = models.CharField(max_length=100, blank=False, null=False)
#     message = models.CharField(max_length=1000, blank=False, null=False)
#     count = models.PositiveIntegerField(null=False, blank=False)
#     first_timestamp = models.DateTimeField(null=False, blank=False)
#     last_timestamp = models.DateTimeField(null=False, blank=False)

kinds = [
    'Pod',
    'HelmRepository',
    'HelmChart'
]

reasons = [
    'ArtifactUpToDate',
    'Pulled',
    'Started',
    'Created',
    'BackOff'
]

namespaces = [
    'default',
    'kube-system',
    'kube-public',
    'kube-node-lease'
    'anteon',
    'kong'
]

first_timestamp = datetime.now(UTC) - timedelta(minutes=5)
last_timestamp = datetime.now(UTC)

class Command(BaseCommand):
    help = 'Populate K8s events with fake data'

    def add_arguments(self, parser):
        parser.add_argument('count', type=int, help='Number of k8s events to create')
        parser.add_argument('cluster', type=str, help='Monitoring id of the cluster to add k8s events to')

    def handle(self, *args, **kwargs):
        count = kwargs['count']
        monitoring_id = kwargs['cluster']
        cluster_qs = Cluster.objects.filter(monitoring_id=monitoring_id)
        if not cluster_qs.exists():
            print("No cluster with that monitoring id exists.")
            return
        
        cluster_uid = cluster_qs.first().uid

        bulk_save = []

        for _ in range(count):
            bulk_save.append(K8sEvent(
                cluster=cluster_uid,
                event_name=uuid.uuid4(),
                kind=random.choice(kinds),
                namespace=random.choice(namespaces),
                name=fake.uri_path(),
                uid=fake.uuid4(),
                reason=random.choice(reasons),
                message=fake.uri_path(),
                count=random.randint(1, 1000),
                first_timestamp=first_timestamp,
                last_timestamp=last_timestamp
            ))

            if len(bulk_save) >= 100:
                K8sEvent.objects.bulk_create(bulk_save)
                bulk_save = []
                
        if bulk_save:
            K8sEvent.objects.bulk_create(bulk_save)


        self.stdout.write(self.style.SUCCESS(f'Successfully populated {count} K8s events events.'))