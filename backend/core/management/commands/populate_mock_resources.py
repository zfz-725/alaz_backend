from math import e
from django.core.management.base import BaseCommand
from core.models import Cluster, Container, DaemonSet, Deployment, Endpoint, Pod, ReplicaSet, Service, StatefulSet
from accounts.models import User
from faker import Faker
import random

fake = Faker()


class Command(BaseCommand):
    help = 'Populate mock service map resources with fake data'

    def add_arguments(self, parser):
        parser.add_argument('count', type=int, help='Number of fake resources to create')
        parser.add_argument('cluster', help='Monitoring id of the cluster to add requests to. If not provided, a new cluster will be created.')

    def handle(self, *args, **kwargs):
        monitoring_id = kwargs['cluster']
        if monitoring_id == "new":
            user = User.objects.get(email='root@ddosify.com')
            cluster = Cluster.objects.create(name=f"cluster-{fake.name()}", monitoring_id=fake.uuid4(), user=user, alaz_info={"test": "test"})
        else:
            cluster = Cluster.objects.get(monitoring_id=monitoring_id)
        count = kwargs['count']
        namespaces = ['default_mock', 'kube-system_mock', 'kube-public_mock', 'kube-node-lease_mock', 'ddosify_mock', 'kong_mock', 'istio_mock', 'monitoring_mock']

        for _ in range(count):
            type = random.randint(1, 5)
            namespace = random.choice(namespaces)
            deleted = random.randint(0, 1)
            if deleted:
                deleted = True
            else:
                deleted = False
            if type == 1:
                pod = Pod.objects.create(cluster=cluster.uid, name=fake.name(), namespace=namespace, uid=fake.uuid4(), ip=fake.ipv4(), deleted=deleted)
            elif type == 2:
                pod = Pod.objects.create(cluster=cluster.uid, name=fake.name(), namespace=namespace, uid=fake.uuid4(), ip=fake.ipv4(), deleted=deleted)
                is_new_deployment = random.randint(0, 1)
                if is_new_deployment or ReplicaSet.objects.filter(cluster=cluster.uid).count() == 0 or Deployment.objects.filter(cluster=cluster.uid).count() == 0:
                    deployment = Deployment.objects.create(uid=fake.uuid4(), cluster=cluster.uid, name=fake.name(), namespace=namespace, replicas=random.randint(1, 10), deleted=deleted)
                    replicaset = ReplicaSet.objects.create(uid=fake.uuid4(), cluster=cluster.uid, name=fake.name(), namespace=namespace, owner=deployment.uid, replicas=random.randint(1, 10), deleted=deleted)
                    pod.replicaset_owner = replicaset.uid
                    pod.save()
                else:
                    # Find a replicaset and assign this to it
                    replicasets = ReplicaSet.objects.filter(cluster=cluster.uid)
                    if replicasets.count() > 0:
                        replicaset = random.choice(replicasets)
                        pod.replicaset_owner = replicaset.uid
                    pod.save()
            elif type == 3:
                is_new_daemonset = random.randint(0, 1)
                pod = Pod.objects.create(uid=fake.uuid4(), cluster=cluster.uid, name=fake.name(), namespace=namespace, ip=fake.ipv4(), deleted=deleted)
                if is_new_daemonset or DaemonSet.objects.all().count() == 0:
                    daemonset = DaemonSet.objects.create(uid=fake.uuid4(), cluster=cluster.uid, name=fake.name(), namespace=namespace, deleted=deleted)
                    pod.daemonset_owner = daemonset.uid
                    pod.save()
                else:
                    # Find a daemonset and assign this to it
                    daemonsets = DaemonSet.objects.all()
                    if daemonsets.count() > 0:
                        daemonset = random.choice(daemonsets)
                        pod.daemonset_owner = daemonset.uid
                    pod.save()
            elif type == 4:
                is_new_service = random.randint(0, 1)
                pod = Pod.objects.create(uid=fake.uuid4(), cluster=cluster.uid, name=fake.name(), namespace=namespace, ip=fake.ipv4(), deleted=deleted)
                pod_ip_payload = {'id': pod.uid, 'ip': pod.ip, 'name': pod.name, 'type': 'Pod', 'namespace': pod.namespace}
                if is_new_service or Service.objects.filter(cluster=cluster.uid).count() == 0:
                    port_count = random.randint(1, 3)
                    ports = []
                    for _ in range(port_count):
                        port = random.randint(1, 65535)
                        ports.append({'src': port, 'dest': port, 'protocol': 'TCP'})
                    service = Service.objects.create(uid=fake.uuid4(), cluster=cluster.uid, name=fake.name(), type = random.choice(['ClusterIP', 'NodePort', 'LoadBalancer']), cluster_ips=[fake.ipv4()], ports=ports, namespace=namespace, deleted=deleted)
                    endpoint_address = [{'ips': [pod_ip_payload], 'ports': []}]
                    endpoint = Endpoint.objects.create(uid=fake.uuid4(), cluster=cluster.uid, name=fake.name(), namespace=namespace, addresses=endpoint_address, deleted=deleted)
                else:
                    # Find an endpoint and assign this to it
                    endpoints = Endpoint.objects.filter(cluster=cluster.uid)
                    if endpoints.count() > 0:
                        endpoint = random.choice(endpoints)
                        endpoint.addresses[0]['ips'].append(pod_ip_payload)
                        endpoint.save()
            elif type == 5:
                is_new_statefulset = random.randint(0, 1)
                pod = Pod.objects.create(uid=fake.uuid4(), cluster=cluster.uid, name=fake.name(), namespace=namespace, ip=fake.ipv4(), deleted=deleted)
                if is_new_statefulset or StatefulSet.objects.all().count() == 0:
                    statefulset = StatefulSet.objects.create(uid=fake.uuid4(), cluster=cluster.uid, name=fake.name(), namespace=namespace, deleted=deleted)
                    pod.statefulset_owner = statefulset.uid
                    pod.save()
                else:
                    # Find a statefulset and assign this to it
                    statefulsets = StatefulSet.objects.all()
                    if statefulsets.count() > 0:
                        statefulset = random.choice(statefulsets)
                        pod.statefulset_owner = statefulset.uid
                    pod.save()

            container_count = random.randint(1, 10)
            for _ in range(container_count):
                port_count = random.randint(1, 3)
                ports = []
                for _ in range(port_count):
                    port = random.randint(1, 65535)
                    ports.append({'src': port, 'dest': port})
                Container.objects.create(uid=fake.uuid4(), cluster=cluster.uid, name=fake.name(), namespace=namespace, pod=pod.uid, image=fake.name(), ports=ports)

        self.stdout.write(self.style.SUCCESS(f'Successfully populated {count} resources.'))
