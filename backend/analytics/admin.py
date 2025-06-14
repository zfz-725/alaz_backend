from django.contrib import admin

from analytics.models import Telemetry

# Register your models here.

class TelemetryAdmin(admin.ModelAdmin):
    list_display = ('id', 'timestamp', 'onprem_key', 'monitoring_id', 'data', 'company_name', 'contact', )
    list_filter = ('monitoring_id', 'onprem_key', 'company_name', 'contact',)
    search_fields = ('id', 'monitoring_id', 'onprem_key', 'company_name', 'contact',)

    def delete_queryset(self, request, queryset):
        queryset.delete()

admin.site.register(Telemetry, TelemetryAdmin)