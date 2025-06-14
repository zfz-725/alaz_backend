
import logging
from core.models import DaemonSet, Deployment, Pod, Service
from core.requester import PrometheusRequester
from core.utils import prometheus_validate_single_value, verbose_log

logger = logging.getLogger(__name__)


def get_telemetry_metrics(cluster):
    requester = PrometheusRequester()
    job = 'alaz'
    metrics = requester.get_cluster_telemetry_metrics(cluster.monitoring_id, job)
    result = {}
    for key, data in metrics.items():
        single_val, metric_result = prometheus_validate_single_value(key, data, [])
        if not single_val:
            logger.error(f'Prometheus data is not single value: {key} - {data}')
            continue
        try:
            result[key] = int(metric_result[key][1])
        except Exception:
            logger.error(f'Prometheus data is not integer: {key} - {metric_result}')
            result[key] = metric_result[key][1]
    return result

def get_telemetry_k8s(cluster):
    node_count_active = len(cluster.last_data_ts['active'])
    node_count_passive = len(cluster.last_data_ts['passive'])
    pod_count = Pod.objects.filter(cluster=cluster.uid, deleted=False).count()
    deployment_count = Deployment.objects.filter(cluster=cluster.uid, deleted=False).count()
    daemonset_count = DaemonSet.objects.filter(cluster=cluster.uid, deleted=False).count()
    service_count = Service.objects.filter(cluster=cluster.uid, deleted=False).count()

    # Get namespaces in a list
    verbose_log(f'Namespaces of pods: {Pod.objects.filter(cluster=cluster.uid, deleted=False).values_list("namespace", flat=True).distinct()}')
    pod_namespaces = Pod.objects.filter(cluster=cluster.uid, deleted=False).values_list('namespace', flat=True).distinct()
    pod_namespaces = set(Pod.objects.filter(cluster=cluster.uid, deleted=False).values_list('namespace', flat=True).distinct())
    deployment_namespaces = set(Deployment.objects.filter(cluster=cluster.uid, deleted=False).values_list('namespace', flat=True).distinct())
    daemonset_namespaces = set(DaemonSet.objects.filter(cluster=cluster.uid, deleted=False).values_list('namespace', flat=True).distinct())
    service_namespaces = set(Service.objects.filter(cluster=cluster.uid, deleted=False).values_list('namespace', flat=True).distinct())
    namespaces = pod_namespaces | deployment_namespaces | daemonset_namespaces | service_namespaces
    namespace_count = len(namespaces)

    return {
        'node_count_active': node_count_active,
        'node_count_passive': node_count_passive,
        'pod_count': pod_count,
        'deployment_count': deployment_count,
        'daemonset_count': daemonset_count,
        'service_count': service_count,
        'namespace_count': namespace_count,
    }

def get_instance_names(cluster):
    instances = []
    instances.extend(cluster.last_data_ts['active'])
    instances.extend(cluster.last_data_ts['passive'])
    return instances

