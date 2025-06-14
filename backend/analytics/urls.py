
from django.conf import settings
from django.urls import path, re_path
from django.urls.conf import include
from rest_framework import routers
import analytics.views as analytics_views


class OptionalSlashRouter(routers.SimpleRouter):
    def __init__(self):
        super().__init__()
        self.trailing_slash = '/?'


urlpatterns = []
router = OptionalSlashRouter()

if settings.ANTEON_ENV != 'onprem':
    urlpatterns += [
        path('telemetry/', analytics_views.TelemetryView.as_view(), name="telemetry-view"),
    ]