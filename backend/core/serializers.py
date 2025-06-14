import json
from core.requester import MyBackendRequester
from core.models import Cluster, Container, DaemonSet, Deployment, Endpoint, Pod, ReplicaSet, Request, Service, Setting, StatefulSet
from core.utils import alaz_find_all_namespaces, get_containers_of_pod, get_endpoints_of_service, get_pods_of_daemonset, get_pods_of_replicaset, get_pods_of_statefulset, get_replicasets_of_deployment, get_total_instances_of_user
from rest_framework import serializers


class ClusterSerializer(serializers.ModelSerializer):
    monitoring_id = serializers.CharField()
    uid = serializers.UUIDField(read_only=False)

    class Meta:
        model = Cluster
        fields = ['uid', 'name', 'user', 'monitoring_id', 'fresh', 'team', 'last_data_ts', 'alaz_info', 'date_created', 'is_alive', 'last_heartbeat', 'alaz_version']

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        if hasattr(instance, 'summary_data'):
            representation['summary_data'] = instance.summary_data

        last_data_ts = None
        # Look for the active nodes
        for _, node_details in instance.last_data_ts['active'].items():
            for _, timestamp in node_details.items():
                if last_data_ts is None or timestamp > last_data_ts:
                    last_data_ts = timestamp

        # If no active node exists, look for the last scrape time
        if last_data_ts is None:
            for _, ts in instance.instances.items():
                if last_data_ts is None or ts > last_data_ts:
                    last_data_ts = ts

        if last_data_ts:
            representation['last_data_ts'] = last_data_ts
        else:
            representation['last_data_ts'] = None

        if len(instance.last_data_ts['active']) == 0:
            representation['alaz_version'] = 'N/A'

        active_instances, passive_instances = get_total_instances_of_user(instance.user)
        instances = {'active': len(active_instances), 'passive': len(passive_instances)}
        # instances = {'active': len(instance.last_data_ts['active']), 'passive': len(instance.last_data_ts['passive'])}
        representation['instances'] = instances

        representation['namespaces'] = alaz_find_all_namespaces(instance)

        return representation

    def create(self, validated_data):
        cluster = Cluster.objects.create(**validated_data)
        return cluster

    def update(self, instance, validated_data):
        validated_data.pop('uid', None)
        instance.name = validated_data.get('name', instance.name)
        instance.save()
        return instance


class ClusterDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = Cluster
        fields = ['uid', 'name', 'user', 'monitoring_id', 'fresh', 'team', 'last_data_ts', 'alaz_info', 'date_created', 'is_alive', 'last_heartbeat', 'alaz_version']

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        if hasattr(instance, 'summary_data'):
            representation['summary_data'] = instance.summary_data

        last_data_ts = None
        # Look for the active nodes
        for _, node_details in instance.last_data_ts['active'].items():
            for _, timestamp in node_details.items():
                if last_data_ts is None or timestamp > last_data_ts:
                    last_data_ts = timestamp

        # If no active node exists, look for the last scrape time
        if last_data_ts is None:
            for _, ts in instance.instances.items():
                if last_data_ts is None or ts > last_data_ts:
                    last_data_ts = ts

        if last_data_ts:
            representation['last_data_ts'] = last_data_ts
        else:
            representation['last_data_ts'] = None

        if len(instance.last_data_ts['active']) == 0:
            representation['alaz_version'] = 'N/A'

        representation['instances_last_ts'] = instance.last_data_ts

        active_instances, passive_instances = get_total_instances_of_user(instance.user)
        instances = {'active': len(active_instances), 'passive': len(passive_instances)}
        # instances = {'active': len(instance.last_data_ts['active']), 'passive': len(instance.last_data_ts['passive'])}
        representation['instances'] = instances

        return representation
    
class LatestAlazVersionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Setting
        fields = ['latest_alaz_version']
class PodSummarySerializer(serializers.ModelSerializer):
    uid = serializers.UUIDField()

    class Meta:
        model = Pod
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'daemonset_owner', 'replicaset_owner', 'statefulset_owner', 'date_updated', 'date_created']
        for field in excluded_fields:
            representation.pop(field, None)
        containers = get_containers_of_pod(instance, self.context)
        try:
            ports = instance.ports
        except Exception:
            ports = []
        for container in containers:
            for port in container.ports:
                if port not in ports:
                    ports.append(port)
        representation['ports'] = ports
        if hasattr(instance, 'connected_resources'):
            representation['connected_resources'] = instance.connected_resources
        if hasattr(instance, 'protocol'):
            representation['protocol'] = instance.protocol
        if hasattr(instance, 'is_zombie'):
            representation['is_zombie'] = instance.is_zombie
        return representation

class PodSerializer(serializers.ModelSerializer):
    uid = serializers.UUIDField()

    class Meta:
        model = Pod
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'daemonset_owner', 'replicaset_owner', 'date_updated', 'date_created']
        for field in excluded_fields:
            representation.pop(field, None)
        containers = get_containers_of_pod(instance, self.context)
        try:
            ports = instance.ports
        except Exception:
            ports = []
        for container in containers:
            for port in container.ports:
                if port not in ports:
                    ports.append(port)
        representation['ports'] = ports
        recursive = self.context.get('recursive', False)
        if not recursive:
            if hasattr(instance, 'top_5_latencies'):
                representation['top_5_latencies'] = instance.top_5_latencies
            if hasattr(instance, 'top_5_rps'):
                representation['top_5_rps'] = instance.top_5_rps
            if hasattr(instance, 'percentage_of_5xx'):
                representation['percentage_of_5xx'] = instance.percentage_of_5xx
        representation['containers'] = ContainerSerializer(containers, many=True).data

        return representation

    def create(self, validated_data):
        pod = Pod.objects.create(**validated_data)
        return pod

    def update(self, instance, validated_data):
        instance.name = validated_data.get('name', instance.name)
        instance.namespace = validated_data.get('namespace', instance.namespace)
        instance.ip = validated_data.get('ip', instance.ip)
        instance.deleted = validated_data.get('deleted', instance.deleted)
        instance.replicaset_owner = validated_data.get('replicaset_owner', instance.replicaset_owner)
        instance.daemonset_owner = validated_data.get('daemonset_owner', instance.daemonset_owner)
        instance.save()
        return instance

class DeploymentSummarySerializer(serializers.ModelSerializer):
    uid = serializers.UUIDField()

    class Meta:
        model = Deployment
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'date_updated', 'date_created']
        for field in excluded_fields:
            representation.pop(field, None)
        # replica_sets = get_replicasets_of_deployment(instance, self.context)
        try:
            ports = instance.ports
        except Exception:
            ports = []
        # for replica_set in replica_sets:
        #     pods = get_pods_of_replicaset(replica_set, self.context)
        #     for pod in pods:
        #         containers = get_containers_of_pod(pod, self.context)
        #         for container in containers:
        #             for port in container.ports:
        #                 if port not in ports:
        #                     ports.append(port)
        if hasattr(instance, 'connected_resources'):
            representation['connected_resources'] = instance.connected_resources
        if hasattr(instance, 'protocol'):
            representation['protocol'] = instance.protocol
        if hasattr(instance, 'is_zombie'):
            representation['is_zombie'] = instance.is_zombie
        representation['ports'] = ports

        return representation


class DeploymentSerializer(serializers.ModelSerializer):
    uid = serializers.UUIDField()

    class Meta:
        model = Deployment
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'date_updated', 'date_created']
        for field in excluded_fields:
            representation.pop(field, None)
        replica_sets = get_replicasets_of_deployment(instance, self.context)
        try:
            ports = instance.ports
        except Exception:
            ports = []
        for replica_set in replica_sets:
            pods = get_pods_of_replicaset(replica_set, self.context)
            for pod in pods:
                containers = get_containers_of_pod(pod, self.context)
                for container in containers:
                    for port in container.ports:
                        if port not in ports:
                            ports.append(port)
        recursive = self.context.get('recursive', False)
        if not recursive:
            if hasattr(instance, 'top_5_latencies'):
                representation['top_5_latencies'] = instance.top_5_latencies
            if hasattr(instance, 'top_5_rps'):
                representation['top_5_rps'] = instance.top_5_rps
            if hasattr(instance, 'percentage_of_5xx'):
                representation['percentage_of_5xx'] = instance.percentage_of_5xx
        copied_context = self.context.copy()
        copied_context['recursive'] = True
        representation['ports'] = ports
        representation['replica_sets'] = ReplicaSetSerializer(replica_sets, many=True, context=copied_context).data

        return representation

    def create(self, validated_data):
        deployment = Deployment.objects.create(**validated_data)
        return deployment

    def update(self, instance, validated_data):
        instance.name = validated_data.get('name', instance.name)
        instance.namespace = validated_data.get('namespace', instance.namespace)
        instance.replicas = validated_data.get('replicas', instance.replicas)
        instance.save()
        return instance

class ServiceSummarySerializer(serializers.ModelSerializer):
    uid = serializers.UUIDField()

    class Meta:
        model = Service
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'date_updated', 'date_created']
        for field in excluded_fields:
            representation.pop(field, None)

        representation['ports'] = instance.used_ports

        if hasattr(instance, 'connected_resources'):
            representation['connected_resources'] = instance.connected_resources
        if hasattr(instance, 'protocol'):
            representation['protocol'] = instance.protocol
        if hasattr(instance, 'is_zombie'):
            representation['is_zombie'] = instance.is_zombie

        return representation

class ServiceSerializer(serializers.ModelSerializer):
    uid = serializers.UUIDField()

    class Meta:
        model = Service
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'date_updated', 'date_created']
        for field in excluded_fields:
            representation.pop(field, None)

        endpoints = get_endpoints_of_service(instance, self.context)

        recursive = self.context.get('recursive', False)
        if not recursive:
            if hasattr(instance, 'top_5_latencies'):
                representation['top_5_latencies'] = instance.top_5_latencies
            if hasattr(instance, 'top_5_rps'):
                representation['top_5_rps'] = instance.top_5_rps
            if hasattr(instance, 'percentage_of_5xx'):
                representation['percentage_of_5xx'] = instance.percentage_of_5xx
        representation['endpoints'] = EndpointSerializer(endpoints, many=True, context=self.context).data

        return representation

    def validate_cluster_ips(self, value):
        if type(value) is not list:
            raise serializers.ValidationError("Invalid cluster_ips")
        return value

    def validate_ports(self, value):
        for port in value:
            src = port.get('src')
            dest = port.get('dest')
            if src is None or dest is None or not isinstance(src, int) or not isinstance(dest, int) or src < 0 or dest < 0:
                raise serializers.ValidationError("Invalid port configuration")
        return value

    def create(self, validated_data):
        service = Service.objects.create(**validated_data)
        return service

    def update(self, instance, validated_data):
        instance.name = validated_data.get('name', instance.name)
        instance.namespace = validated_data.get('namespace', instance.namespace)
        instance.type = validated_data.get('type', instance.type)
        instance.cluster_ips = validated_data.get('cluster_ips', instance.cluster_ips)
        instance.deleted = validated_data.get('deleted', instance.deleted)
        instance.ports = validated_data.get('ports', instance.ports)
        instance.save()
        return instance


class RequestSerializer(serializers.ModelSerializer):
    class Meta:
        model = Request
        fields = '__all__'

    def create(self, validated_data):
        request = Request.objects.create(**validated_data)
        return request


class ContainerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Container
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'date_created', 'date_updated', 'pod']
        for field in excluded_fields:
            representation.pop(field, None)
        return representation

    def validate_ports(self, value):
        for port_pair in value:
            port = port_pair.get('port')
            protocol = port_pair.get('protocol')
            if port is None or not isinstance(port, int) or port < 0:
                raise serializers.ValidationError("Invalid port number")
            if protocol is None or not isinstance(protocol, str):
                raise serializers.ValidationError("Invalid protocol of port")
        return value

    def create(self, validated_data):
        container = Container.objects.create(**validated_data)
        return container

    def update(self, instance, validated_data):
        instance.name = validated_data.get('name', instance.name)
        instance.image = validated_data.get('image', instance.image)
        instance.namespace = validated_data.get('namespace', instance.namespace)
        instance.pod = validated_data.get('pod', instance.pod)
        instance.ports = validated_data.get('ports', instance.ports)
        instance.save()
        return instance


class EndpointSerializer(serializers.ModelSerializer):
    uid = serializers.UUIDField()

    class Meta:
        model = Endpoint
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'date_created', 'date_updated']
        for field in excluded_fields:
            representation.pop(field, None)
        return representation

    def validate_addresses(self, value):
        try:
            addresses = value
            if not isinstance(addresses, list):
                raise serializers.ValidationError('Addresses field must be a list')

            for address_item in addresses:
                if not isinstance(address_item, dict):
                    raise serializers.ValidationError('Each address item must be a dictionary')

                ips = address_item.get('ips')
                ports = address_item.get('ports')

                if not isinstance(ips, list):
                    raise serializers.ValidationError('ips field must be a list')
                if not isinstance(ports, list):
                    raise serializers.ValidationError('ports field must be a list')

                for ip in ips:
                    if not isinstance(ip, dict):
                        raise serializers.ValidationError('Each IP item must be a dictionary')

                    ip_type = ip.get('type')
                    if ip_type not in ('Pod', 'external'):
                        raise serializers.ValidationError('Invalid IP type')

                    if ip_type == 'Pod':
                        # Additional validation logic specific to 'pod' IPs
                        id = ip.get('id')
                        if not isinstance(id, str):
                            raise serializers.ValidationError('id must be a string')
                        name = ip.get('name')
                        if not isinstance(name, str):
                            raise serializers.ValidationError('name must be a string')
                        namespace = ip.get('namespace')
                        if not isinstance(namespace, str):
                            raise serializers.ValidationError('namespace must be a string')
                        ip_address = ip.get('ip')
                        if not isinstance(ip_address, str):
                            raise serializers.ValidationError('ip must be a string')

                    elif ip_type == 'external':
                        # Additional validation logic specific to 'external' IPs
                        ip_address = ip.get('ip')
                        if not isinstance(ip_address, str):
                            raise serializers.ValidationError('ip must be a string')

                    if not ip_address:
                        raise serializers.ValidationError('Missing IP address')

                for port in ports:
                    if not isinstance(port, dict):
                        raise serializers.ValidationError('Each port must be a dictionary')

                    port_address = port.get('port')
                    if not isinstance(port_address, int):
                        raise serializers.ValidationError('port must be an integer')
                    if port_address < 0:
                        raise serializers.ValidationError('port must be a positive integer')
                    port_protocol = port.get('protocol')
                    if not isinstance(port_protocol, str):
                        raise serializers.ValidationError('protocol must be a string')

            return value

        except (json.JSONDecodeError, TypeError):
            raise serializers.ValidationError('Invalid JSON format for addresses field')

    def create(self, validated_data):
        endpoint = Endpoint.objects.create(**validated_data)
        return endpoint

    def update(self, instance, validated_data):
        instance.name = validated_data.get('name', instance.name)
        instance.namespace = validated_data.get('namespace', instance.namespace)
        instance.addresses = validated_data.get('addresses', instance.addresses)
        instance.save()
        return instance


class ReplicaSetSerializer(serializers.ModelSerializer):
    uid = serializers.UUIDField()

    class Meta:
        model = ReplicaSet
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'date_created', 'date_updated']
        for field in excluded_fields:
            representation.pop(field, None)

        pods = get_pods_of_replicaset(instance, self.context)
        representation['pods'] = PodSerializer(pods, many=True, context=self.context).data
        return representation

    def create(self, validated_data):
        replica_set = ReplicaSet.objects.create(**validated_data)
        return replica_set

    def update(self, instance, validated_data):
        instance.name = validated_data.get('name', instance.name)
        instance.namespace = validated_data.get('namespace', instance.namespace)
        instance.owner = validated_data.get('owner', instance.owner)
        instance.replicas = validated_data.get('replicas', instance.replicas)
        instance.save()
        return instance

class DaemonSetSummarySerializer(serializers.ModelSerializer):
    uid = serializers.UUIDField()

    class Meta:
        model = DaemonSet
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'date_updated', 'date_created']
        for field in excluded_fields:
            representation.pop(field, None)

        # all_pods = []
        try:
            ports = instance.ports
        except Exception:
            ports = []
        # pods = get_pods_of_daemonset(instance, self.context)
        # all_pods.extend(pods)
        # for pod in pods:
        #     containers = get_containers_of_pod(pod, self.context)
        #     for container in containers:
        #         for port in container.ports:
        #             if port not in ports:
        #                 ports.append(port)

        if hasattr(instance, 'connected_resources'):
            representation['connected_resources'] = instance.connected_resources
        if hasattr(instance, 'protocol'):
            representation['protocol'] = instance.protocol
        if hasattr(instance, 'is_zombie'):
            representation['is_zombie'] = instance.is_zombie
        representation['ports'] = ports

        return representation

class DaemonSetSerializer(serializers.ModelSerializer):
    uid = serializers.UUIDField()

    class Meta:
        model = DaemonSet
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'date_updated', 'date_created']
        for field in excluded_fields:
            representation.pop(field, None)

        all_pods = []
        try:
            ports = instance.ports
        except Exception:
            ports = []
        pods = get_pods_of_daemonset(instance, self.context)
        all_pods.extend(pods)
        for pod in pods:
            containers = get_containers_of_pod(pod, self.context)
            for container in containers:
                for port in container.ports:
                    if port not in ports:
                        ports.append(port)

        recursive = self.context.get('recursive', False)
        if not recursive:
            if hasattr(instance, 'top_5_latencies'):
                representation['top_5_latencies'] = instance.top_5_latencies
            if hasattr(instance, 'top_5_rps'):
                representation['top_5_rps'] = instance.top_5_rps
            if hasattr(instance, 'percentage_of_5xx'):
                representation['percentage_of_5xx'] = instance.percentage_of_5xx
        copied_context = self.context.copy()
        copied_context['recursive'] = True
        representation['ports'] = ports
        representation['pods'] = PodSerializer(all_pods, many=True, context=copied_context).data

        return representation

    def create(self, validated_data):
        daemon_set = DaemonSet.objects.create(**validated_data)
        return daemon_set

    def update(self, instance, validated_data):
        instance.name = validated_data.get('name', instance.name)
        instance.namespace = validated_data.get('namespace', instance.namespace)
        instance.save()
        return instance


class StatefulSetSummarySerializer(serializers.ModelSerializer):
    uid = serializers.UUIDField()

    class Meta:
        model = StatefulSet
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'date_updated', 'date_created']
        for field in excluded_fields:
            representation.pop(field, None)

        # all_pods = []
        try:
            ports = instance.ports
        except Exception:
            ports = []
        # pods = get_pods_of_daemonset(instance, self.context)
        # all_pods.extend(pods)
        # for pod in pods:
        #     containers = get_containers_of_pod(pod, self.context)
        #     for container in containers:
        #         for port in container.ports:
        #             if port not in ports:
        #                 ports.append(port)

        if hasattr(instance, 'connected_resources'):
            representation['connected_resources'] = instance.connected_resources
        if hasattr(instance, 'protocol'):
            representation['protocol'] = instance.protocol
        if hasattr(instance, 'is_zombie'):
            representation['is_zombie'] = instance.is_zombie
        representation['ports'] = ports

        return representation

class StatefulSetSerializer(serializers.ModelSerializer):
    uid = serializers.UUIDField()

    class Meta:
        model = StatefulSet
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        excluded_fields = ['cluster', 'date_updated', 'date_created']
        for field in excluded_fields:
            representation.pop(field, None)

        all_pods = []
        try:
            ports = instance.ports
        except Exception:
            ports = []
        pods = get_pods_of_statefulset(instance, self.context)
        all_pods.extend(pods)
        for pod in pods:
            containers = get_containers_of_pod(pod, self.context)
            for container in containers:
                for port in container.ports:
                    if port not in ports:
                        ports.append(port)

        recursive = self.context.get('recursive', False)
        if not recursive:
            if hasattr(instance, 'top_5_latencies'):
                representation['top_5_latencies'] = instance.top_5_latencies
            if hasattr(instance, 'top_5_rps'):
                representation['top_5_rps'] = instance.top_5_rps
            if hasattr(instance, 'percentage_of_5xx'):
                representation['percentage_of_5xx'] = instance.percentage_of_5xx
        copied_context = self.context.copy()
        copied_context['recursive'] = True
        representation['ports'] = ports
        representation['pods'] = PodSerializer(all_pods, many=True, context=copied_context).data

        return representation

    def create(self, validated_data):
        daemon_set = DaemonSet.objects.create(**validated_data)
        return daemon_set

    def update(self, instance, validated_data):
        instance.name = validated_data.get('name', instance.name)
        instance.namespace = validated_data.get('namespace', instance.namespace)
        instance.save()
        return instance


class PortSerializer(serializers.Serializer):
    port = serializers.IntegerField()
    protocol = serializers.CharField()

class OutboundSummarySerializer(serializers.Serializer):
    ports = PortSerializer(many=True)

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        if hasattr(instance, 'connected_resources'):
            representation['connected_resources'] = instance.connected_resources
        if hasattr(instance, 'protocol'):
            representation['protocol'] = instance.protocol
        if hasattr(instance, 'is_zombie'):
            representation['is_zombie'] = instance.is_zombie
        
        return representation

class OutboundSerializer(serializers.Serializer):
    ports = PortSerializer(many=True)

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        recursive = self.context.get('recursive', False)
        if not recursive:
            if hasattr(instance, 'top_5_latencies'):
                representation['top_5_latencies'] = instance.top_5_latencies
            if hasattr(instance, 'top_5_rps'):
                representation['top_5_rps'] = instance.top_5_rps
            if hasattr(instance, 'percentage_of_5xx'):
                representation['percentage_of_5xx'] = instance.percentage_of_5xx
        
        return representation


class ClusterDirectSerializer(serializers.ModelSerializer):
    class Meta:
        model = Cluster
        exclude = ('date_created', 'date_updated')

class DeploymentDirectSerializer(serializers.ModelSerializer):
    class Meta:
        model = Deployment
        exclude = ('date_created', 'date_updated')

class ServiceDirectSerializer(serializers.ModelSerializer):
    class Meta:
        model = Service
        exclude = ('date_created', 'date_updated')

class PodDirectSerializer(serializers.ModelSerializer):
    class Meta:
        model = Pod
        exclude = ('date_created', 'date_updated')

class ContainerDirectSerializer(serializers.ModelSerializer):
    class Meta:
        model = Container
        exclude = ('date_created', 'date_updated')

class EndpointDirectSerializer(serializers.ModelSerializer):
    class Meta:
        model = Endpoint
        exclude = ('date_created', 'date_updated')

class ReplicaSetDirectSerializer(serializers.ModelSerializer):
    class Meta:
        model = ReplicaSet
        exclude = ('date_created', 'date_updated')

class DaemonSetDirectSerializer(serializers.ModelSerializer):
    class Meta:
        model = DaemonSet
        exclude = ('date_created', 'date_updated')
