import datetime
from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from analytics.models import Telemetry
from accounts.models import User
from analytics.serializers import TelemetrySerializer
from analytics.utils import get_instance_names, get_telemetry_k8s, get_telemetry_metrics
from core.requester import CloudAlazBackendRequester, MyBackendRequester
from core.models import Cluster, Setting

logger = get_task_logger(__name__)

def verbose_log(log):
    if settings.CELERY_VERBOSE_LOG:
        logger.info(log)

@shared_task
def send_telemetry_data_to_cloud():
    if settings.ANTEON_ENV != 'onprem':
        return

    if not settings.ANONYMOUS_TELEMETRY_ENABLED:
        verbose_log('Anonymous telemetry is disabled')
        return

    default_settings_qs = Setting.objects.filter(name='default')
    if not default_settings_qs.exists():
        logger.error('Default settings do not exist')
        return

    default_settings = default_settings_qs.first()
    onprem_key = default_settings.onprem_key
    payload = {
        'auth_key': settings.TELEMETRY_AUTH_KEY, 
        'onprem_key': str(onprem_key),
        'data': []
    }
    start_time = datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=settings.TELEMETRY_DATA_INTERVAL_SECONDS)

    clusters = {}
    telemetry_data_objects = Telemetry.objects.using('default').filter(timestamp__gte=start_time)
    my_backend_requester = MyBackendRequester()
    selfhosted_stats = my_backend_requester.get_selfhosted_stats()
    company_name = selfhosted_stats.get('company_name', '')
    contact = selfhosted_stats.get('contact', '')
    for telemetry_data in telemetry_data_objects:
        telemetry_data.company_name = company_name
        telemetry_data.contact = contact

        if telemetry_data.monitoring_id not in clusters:
            cluster_qs = Cluster.objects.filter(monitoring_id=telemetry_data.monitoring_id)
            if not cluster_qs.exists():
                logger.error(f'Cluster with monitoring id {telemetry_data.monitoring_id} does not exist')
                continue
            clusters[telemetry_data.monitoring_id] = cluster_qs.first()
        
        cluster = clusters[telemetry_data.monitoring_id]
        
        alaz_details = telemetry_data.data.copy()
        telemetry_data.data = {}
        telemetry_data.data['cluster_info'] = {
            'name': cluster.name,
            'instance_names': get_instance_names(cluster),
            'alaz_version': cluster.alaz_version,
            'cluster_details': alaz_details
        }
        telemetry_data.data['alaz_info'] = cluster.alaz_info
        telemetry_data.data['metrics'] = get_telemetry_metrics(cluster)
        telemetry_data.data['k8s'] = get_telemetry_k8s(cluster)

        serialized_telemetry = TelemetrySerializer(telemetry_data)
        payload['data'].append(serialized_telemetry.data)

    root_user_qs = User.objects.filter(email=settings.ROOT_EMAIL)
    if not root_user_qs.exists():
        logger.error('Root user does not exist')
        return
    
    root_user = root_user_qs.first()

    # Add Selfhosted data
    if len(payload['data']) == 0:
        mock_data = {
            'timestamp': datetime.datetime.now(datetime.UTC).isoformat(),
            'onprem_key': str(onprem_key),
            'monitoring_id': None,
            'data': {},
            'company_name': company_name,
            'contact': contact,
        }
        payload['data'].append(mock_data)

    if payload['data']:
        cloud_alaz_backend_requester = CloudAlazBackendRequester()
        cloud_alaz_backend_requester.send_telemetry_data(payload)
    else:
        verbose_log('No telemetry data to send')

    telemetry_data_objects.delete()
    verbose_log('Telemetry data deleted')
