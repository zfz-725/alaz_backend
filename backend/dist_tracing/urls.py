from django.urls import path
from django.urls.conf import include
from rest_framework.routers import DefaultRouter
from rest_framework import routers
import dist_tracing.views as dist_tracing_views

class OptionalSlashRouter(routers.SimpleRouter):
    def __init__(self):
        super().__init__()
        self.trailing_slash = '/?'


router = DefaultRouter()
# router.register(r'span', dist_tracing_views.SpanViewSet, basename='span')

urlpatterns = [
    path('', include(router.urls)),
    path('span/<str:span_id>/', dist_tracing_views.SpanView.as_view(), name='span-list'),
    path('traffic/', dist_tracing_views.TrafficView.as_view(), name='traffic'),
    path('trace/', dist_tracing_views.TraceListView.as_view(), name='trace-list'),
    path('trace/<str:trace_id>/', dist_tracing_views.TraceRetrieveView.as_view(), name='trace-retrieve'),
    path('trace_metadata/', dist_tracing_views.TraceMetadataView.as_view(), name='trace-metadata'),
]
