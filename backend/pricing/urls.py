import pricing.views as pricing_views
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

if settings.ANTEON_ENV != 'onprem':
    urlpatterns += [
        path('metric_counts/', pricing_views.MetricCountView.as_view(), name="metric-count-view"),
        path('storage_usage/', pricing_views.StorageUsageView.as_view(), name="storage-usage-view"),
    ]