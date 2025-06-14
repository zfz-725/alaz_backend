import random
import uuid
import faker
import json
from django.core.management.base import BaseCommand
from core.models import Container, DaemonSet, Deployment, Endpoint, Pod, ReplicaSet, Service

namespaces = ['default_mock', 'kube-sys_mock', 'kong_mock', 'ddosify_mock', 'kube_mock']
faker_obj = faker.Faker()
class Command(BaseCommand):
    help = 'Populate AlazRequests with fake data'

    def add_arguments(self, parser):
        parser.add_argument('type', type=str, help='Type of the asked resource')
        parser.add_argument('cluster', type=str, help='Uid of the cluster to get resources from')
        parser.add_argument('update_count', type=int, help='Number of resources to update')
        parser.add_argument('create_count', type=int, help='Number of resources to create')

    def format_pod(self, pod):
        json = {
            "event_type": random.choice(['ADD', 'UPDATE', 'DELETE']),
            "uid": str(pod.uid),
            "name": "{{_randomWord}}",
            "namespace": "{{_randomWord}}",
            "ip": "{{_randomIP}}",
            "owner_type": "",
            "owner_name": "",  # Unused
            "owner_id": "",
        }
        if pod.replicaset_owner:
            json['owner_type'] = 'ReplicaSet'
            json['owner_id'] = str(pod.replicaset_owner)
        elif pod.daemonset_owner:
            json['owner_type'] = 'DaemonSet'
            json['owner_id'] = str(pod.daemonset_owner)

        return json

    def format_container(self, container):
        json = {
            "event_type": random.choice(['ADD', 'UPDATE']),
            "name": "{{_randomWord}}",
            "namespace": "{{_randomWord}}",
            "pod": str(container.pod),
            "image": "{{_randomWord}}",
            "ports": container.ports
        }
        return json
    
    def format_deployment(self, deployment):
        json = {
            "event_type": random.choice(['ADD', 'UPDATE', 'DELETE']),
            "uid": str(deployment.uid),
            "name": "{{_randomWord}}",
            "namespace": "{{_randomWord}}",
            "replicas": "{{_randomInt}}"
        }
        return json

    def format_service(self, service):
        json = {
            "event_type": random.choice(['ADD', 'UPDATE', 'DELETE']),
            "uid": str(service.uid),
            "name": service.name,
            "namespace": service.namespace,
            "type": "{{_randomWord}}",
            "ports": service.ports,  # TODO: Randomize this
            "cluster_ips": ["{{_randomIP}}"]
        }
        return json

    def format_daemonset(self, daemonset):
        json = {
            "event_type": random.choice(['ADD', 'UPDATE', 'DELETE']),
            "uid": str(daemonset.uid),
            "name": "{{_randomWord}}",
            "namespace": "{{_randomWord}}",
        }
        return json

    def format_replicaset(self, replicaset):
        json = {
            "event_type": random.choice(['ADD', 'UPDATE', 'DELETE']),
            "uid": str(replicaset.uid),
            "name": "{{_randomWord}}",
            "namespace": "{{_randomWord}}",
            "owner_type": "Deployment",
            "owner_name": "{{_randomWord}}",
            "owner_id": "{{_randomUUID}}",
            "replicas": "{{_randomInt}}"
        }

        if replicaset.owner:
            json['owner_type'] = 'Deployment'
            json['owner_id'] = str(replicaset.owner)

        return json

    def format_endpoint(self, endpoint):
        new_address = endpoint.addresses
        new_address[0]['ips'][0]['ip'] = "{{_randomIP}}"
        json = {
            "event_type": random.choice(['ADD', 'UPDATE', 'DELETE']),
            "uid": str(endpoint.uid),
            "name": endpoint.name,
            "namespace": endpoint.namespace,
            "addresses": new_address
        }

        return json

    def generate_pod(self):
        json = {
            "event_type": random.choice(['ADD', 'UPDATE']),
            "uid": "{{_randomUUID}}",
            "name": "pod-{{_randomUUID}}",
            "namespace": random.choice(namespaces),
            "ip": faker_obj.ipv4(),
            "owner_type": "",
            "owner_name": "",  # Unused
            "owner_id": "",
        }
        return json

    def generate_container(self):
        json = {
            "event_type": random.choice(['ADD', 'UPDATE']),
            "name": "container-{{_randomUUID}}",
            "namespace": random.choice(namespaces),
            "pod": "{{_randomUUID}}",
            "image": "image-" + str(uuid.uuid4()),
            "ports": [{'port': random.randint(1, 65535) , 'protocol': random.choice(['TCP', 'UDP'])}]
        }
        return json

    def generate_deployment(self):
        json = {
            "event_type": random.choice(['ADD', 'UPDATE']),
            "uid": "{{_randomUUID}}",
            "name": "deployment-{{_randomUUID}}",
            "namespace": random.choice(namespaces),
            "replicas": random.randint(1, 10)
        }
        return json
    
    def generate_service(self):
        json = {
            "event_type": random.choice(['ADD', 'UPDATE']),
            "uid": "{{_randomUUID}}",
            "name": "service-{{_randomUUID}}",
            "namespace": "{{_randomWord}}",
            "type": random.choice(['ClusterIP', 'NodePort', 'LoadBalancer']),
            "ports": [{'src': 80, 'dest': 80, 'protocol': 'TCP'}],
            "cluster_ips": [faker_obj.ipv4()]
        }
        return json

    def generate_daemonset(self):
        json = {
            "event_type": random.choice(['ADD', 'UPDATE']),
            "uid": "{{_randomUUID}}",
            "name": "daemonset-" + str(uuid.uuid4()),
            "namespace": random.choice(namespaces)
        }
        return json
    
    def generate_replicaset(self):
        json = {
            "event_type": random.choice(['ADD', 'UPDATE']),
            "uid": "{{_randomUUID}}",
            "name": "replicaset-{{_randomUUID}}",
            "namespace": random.choice(namespaces),
            "owner_type": "Deployment",
            "owner_name": "{{_randomWord}}",
            "owner_id": "{{_randomUUID}}",
            "replicas": random.randint(1, 10)
        }
        return json
    
    def generate_endpoint(self):
        port = "{{_randomInt}}"
        ip = "{{_randomIP}}"
        pod_uid = "{{_randomUUID}}"
        pod_name = "{{_randomWord}}"
        pod_namespace = "{{_randomWord}}"

        json = {
            "event_type": random.choice(['ADD', 'UPDATE']),
            "uid": "{{_randomUUID}}",
            "name": "endpoint-{{_randomUUID}}",
            "namespace": random.choice(namespaces),
            "addresses": [{'ips': [{'type': 'Pod', 'id': pod_uid, 'name': pod_name, 'namespace': pod_namespace, 'ip': ip}], 'ports': [{'port': port, 'protocol': 'TCP'}]}]
        }
        return json

    def handle(self, *args, **kwargs):
        cluster_uid = kwargs['cluster']
        update_count = kwargs['update_count']
        create_count = kwargs['create_count']
        type = kwargs['type']

        list = []
        if type == 'pod':
            pods = Pod.objects.filter(cluster=cluster_uid).order_by('?')[:update_count]
            for pod in pods:
                list.append(self.format_pod(pod))

            for _ in range(create_count):
                list.append(self.generate_pod())
        elif type == 'container':
            containers = Container.objects.filter(cluster=cluster_uid).order_by('?')[:update_count]
            for container in containers:
                list.append(self.format_container(container))

            for _ in range(create_count):
                list.append(self.generate_container())
        elif type == 'deployment':
            deployments = Deployment.objects.filter(cluster=cluster_uid).order_by('?')[:update_count]
            for deployment in deployments:
                list.append(self.format_deployment(deployment))

            for _ in range(create_count):
                list.append(self.generate_deployment())
        elif type == 'service':
            services = Service.objects.filter(cluster=cluster_uid).order_by('?')[:update_count]
            for service in services:
                list.append(self.format_service(service))

            for _ in range(create_count):
                list.append(self.generate_service())
        elif type == 'daemonset':
            daemonsets = DaemonSet.objects.filter(cluster=cluster_uid).order_by('?')[:update_count]
            for daemonset in daemonsets:
                list.append(self.format_daemonset(daemonset))

            for _ in range(create_count):
                list.append(self.generate_daemonset())
        elif type == 'replicaset':
            replicasets = ReplicaSet.objects.filter(cluster=cluster_uid).order_by('?')[:update_count]
            for replicaset in replicasets:
                list.append(self.format_replicaset(replicaset))
            
            for _ in range(create_count):
                list.append(self.generate_replicaset())
        elif type == 'endpoint':
            endpoints = Endpoint.objects.filter(cluster=cluster_uid).order_by('?')[:update_count]
            for endpoint in endpoints:
                list.append(self.format_endpoint(endpoint))

            for _ in range(create_count):
                list.append(self.generate_endpoint())
        else:
            print(f'Unknown type {type}')
            return

        
        print(json.dumps(list))
