
import core.views as core_views
from django.conf import settings
from django.urls import path, re_path
from django.urls.conf import include
from rest_framework import routers


class OptionalSlashRouter(routers.SimpleRouter):
    def __init__(self):
        super().__init__()
        self.trailing_slash = '/?'


urlpatterns = []
router = OptionalSlashRouter()

urlpatterns += [
    path('', include(router.urls)),
    path('cluster/', core_views.ClusterView.as_view(), name="cluster-view"),
    path('cluster/<str:monitoring_id>/', core_views.ClusterRetrieveView.as_view(), name="cluster-view"),
    path('cluster/details/<str:monitoring_id>/', core_views.ClusterDetailsView.as_view(), name="cluster-detail-view"),
    path('user_last_data_ts/<str:user_id>/', core_views.UserLastDataTsView.as_view(), name="user-last-data-ts-view"),
    path('requests/', core_views.RequestsView.as_view(), name="requests-view"),
    path('connections/', core_views.ConnectionsView.as_view(), name="connections-view"),
    path('service_map/', core_views.ServiceMapView.as_view(), name="service-map-view"),
    path('resource/traffic/', core_views.ResourceTrafficView.as_view(), name="resource-traffic-view"),
    path('resource/endpoints/', core_views.ResourceEndpointsView.as_view(), name="resource-endpoints-view"),
    path('resource/metrics/', core_views.ResourceMetricsView.as_view(), name="resource-metrics-view"),
    path('metrics/', core_views.MetricsView.as_view(), name="metrics-view"),
    path('metrics/scrape/', core_views.MetricsScrapeView.as_view(), name="metrics-scrape-view"),
    path('metrics/instances/', core_views.MetricsInstancesView.as_view(), name="metrics-instances-view"),
    path('pod/', core_views.PodEventsView.as_view(), name="pod-events-view"),
    path('deployment/', core_views.DeploymentEventsView.as_view(), name="deployment-events-view"),
    path('replicaset/', core_views.ReplicaSetEventsView.as_view(), name="replicaset-events-view"),
    path('container/', core_views.ContainerEventsView.as_view(), name="container-events-view"),
    path('endpoint/', core_views.EndpointEventsView.as_view(), name="endpoint-events-view"),
    path('svc/', core_views.ServiceEventsView.as_view(), name="service-events-view"),
    path('daemonset/', core_views.DaemonSetEventsView.as_view(), name="daemonset-events-view"),
    path('statefulset/', core_views.StatefulSetEventsView.as_view(), name="statefulset-events-view"),
    path('healthcheck/', core_views.HealthCheckView.as_view(), name="healthcheck-view"),
    path('log_resources/', core_views.LogResourcesView.as_view(), name="log-resources-view"),
    path('logs/', core_views.LogsDownloadView.as_view(), name="logs-download-view"),
    path('events/kafka/', core_views.KafkaEventsView.as_view(), name="kafka-events-view"),
    path('last_heartbeat_three_days_ago/', core_views.ClusterListViewHeartbeatThreeDaysAgo.as_view(), name='cluster-list'),
    path('handle_unsended_heartbeat_mails/', core_views.ClusterHandleHeartbeatThreeDaysAgo.as_view(), name='cluster-list'),
    path('api_catalog/options/<str:monitoring_id>/', core_views.APICatalogOptionsView.as_view(), name='api-catalog-options'),
    path('api_catalog/', core_views.APICatalogMainView.as_view(), name='api-catalog-main'),
    path('api_catalog/details/', core_views.APICatalogDetailsView.as_view(), name='api-catalog-details'),
    path('k8s-events/', core_views.K8sEventsView.as_view(), name='k8s-events'),
    path('k8s_events/filters/<str:monitoring_id>/', core_views.K8sEventsFiltersView.as_view(), name='k8s-events-filters'),
    path('k8s_events/list/', core_views.K8sEventsListView.as_view(), name='k8s-events-list'),
]

# if settings.TCPDUMP_ENABLED:
#     urlpatterns += [
#         path('tcpdump/', core_views.TcpDumpView.as_view(), name="tcpdump-view"),
#     ]