from django.conf import settings
from django.contrib import admin

from pricing.models import MetricCount

# Register your models here.

class MetricCountAdmin(admin.ModelAdmin):
    list_display = ('user', 'cluster', 'count', 'created_at', 'updated_at')
    list_filter = ('user', 'cluster', 'created_at', 'updated_at')
    search_fields = ('user__email', 'cluster__name')
        
    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

if settings.ANTEON_ENV != 'onprem':
    admin.site.register(MetricCount, MetricCountAdmin)