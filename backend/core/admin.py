from core.models import (Cluster, Container, DaemonSet, Deployment, Endpoint, K8sEvent, KafkaEvent, Pod, ReplicaSet, Request, Service, Setting, Connection, StatefulSet)
from django.conf import settings
from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html


class SettingAdmin(admin.ModelAdmin):
    list_display = ['name',
                    'onprem_key',
                    'prometheus_queries',
                    'latest_alaz_version',
                    'used_metrics',
                    ]

    list_filter = ("name",)
    search_fields = ['name']
    ordering = ('name',)

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True


class ClusterAdmin(admin.ModelAdmin):
    list_display = ['uid',
                    'name',
                    'user',
                    'fresh',
                    'team',
                    'last_data_ts',
                    'is_alive',
                    'alaz_info',
                    'alaz_version',
                    'last_heartbeat',
                    'instances',
                    'monitoring_id',
                    'date_created',
                    'date_updated']

    list_filter = ("user", 'alaz_version', 'is_alive')
    search_fields = ['uid', 'name', 'user__email', 'monitoring_id']
    autocomplete_fields = ['user']
    ordering = ('-date_created',)
    actions = ['delete_selected']

    def delete_selected(self, request, queryset):
        for obj in queryset:
            obj.delete()

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True


class PodAdmin(admin.ModelAdmin):
    list_display = ['uid',
                    'cluster',
                    'name',
                    'namespace',
                    'replicaset_owner',
                    'daemonset_owner',
                    'statefulset_owner',
                    'ip',
                    'deleted',
                    'date_created',
                    'date_updated']

    list_filter = ("deleted", "namespace", 'cluster')
    search_fields = ['uid', 'name', 'namespace', 'replicaset_owner', 'daemonset_owner', 'statefulset_owner', 'cluster']
    ordering = ('-date_created',)
    actions = ['delete_selected']

    def delete_selected(self, request, queryset):
        for obj in queryset:
            obj.delete()

    delete_selected.short_description = "Delete selected pods"

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True


class DeploymentAdmin(admin.ModelAdmin):
    list_display = ['uid',
                    'cluster',
                    'name',
                    'namespace',
                    'replicas',
                    'deleted',
                    'date_created',
                    'date_updated']

    list_filter = ("namespace", "deleted", 'cluster')
    search_fields = ['uid', 'name', 'namespace', 'cluster']
    ordering = ('-date_created',)

    def delete_selected(self, request, queryset):
        for obj in queryset:
            obj.delete()

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True


class ServiceAdmin(admin.ModelAdmin):
    list_display = ['uid',
                    'cluster',
                    'name',
                    'namespace',
                    'type',
                    'cluster_ips',
                    'deleted',
                    'ports',
                    'date_created',
                    'date_updated']

    list_filter = ("namespace", 'deleted', 'type', 'cluster')
    search_fields = ['uid', 'name', 'namespace', 'type', 'cluster']
    ordering = ('-date_created',)

    def delete_selected(self, request, queryset):
        for obj in queryset:
            obj.delete()

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True


class RequestAdmin(admin.ModelAdmin):
    list_display = ['id',
                    'cluster',
                    'start_time',
                    'latency',
                    'from_ip',
                    'from_uid_pod',
                    'from_uid_service',
                    'from_uid_deployment',
                    'from_uid_daemonset',
                    'from_uid_statefulset',
                    'from_port',
                    'to_ip',
                    'to_uid_pod',
                    'to_uid_service',
                    'to_url_outbound',
                    'to_uid_deployment',
                    'to_uid_daemonset',
                    'to_uid_statefulset',
                    'to_port',
                    'protocol',
                    'status_code',
                    'method',
                    'path',
                    'tls',
                    # 'tcp_seq_num',
                    # 'node_id',
                    # 'thread_id',
                    # 'span_exists',
                    # 'date_created',
                    # 'date_updated'
                    ]

    list_filter = ("method", "protocol", 'cluster', 'status_code', 'tls')
    readonly_fields = ("id",)
    # search_fields = ['id', 'from_uid_pod',
    #  'to_uid_pod', 'to_uid_service', 'to_url_outbound', 'path', 'method', 'protocol', 'cluster__name']
    search_fields = [
        # 'cluster', 'start_time', 'from_ip', 'to_ip', 'from_port', 'to_port',
        'cluster', 
        'from_uid_pod', 'from_uid_service', 'to_uid_pod', 'to_uid_service', 'to_url_outbound',
        'fail_reason', 'path']
    ordering = ('-start_time',)
    actions = ['delete_selected']

    def delete_selected(self, request, queryset):
        for obj in queryset:
            obj.delete()

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    
class KafkaEventAdmin(admin.ModelAdmin):
    list_display = ['id',
                    'cluster',
                    'start_time',
                    'latency',
                    'from_ip',
                    'from_uid_pod',
                    'from_uid_service',
                    'from_uid_deployment',
                    'from_uid_daemonset',
                    'from_uid_statefulset',
                    'from_port',
                    'to_ip',
                    'to_uid_pod',
                    'to_uid_service',
                    'to_url_outbound',
                    'to_uid_deployment',
                    'to_uid_daemonset',
                    'to_uid_statefulset',
                    'to_port',
                    'topic',
                    'partition',
                    'key',
                    'value',
                    'type',
                    'tls',
                    # 'tcp_seq_num',
                    # 'node_id',
                    # 'thread_id',
                    # 'span_exists',
                    # 'date_created',
                    # 'date_updated'
                    ]

    list_filter = ("type", "topic", 'cluster', 'tls')
    readonly_fields = ("id",)
    search_fields = ['id', 'from_uid_pod',
                     'to_uid_pod', 'to_uid_service', 'to_url_outbound']
    ordering = ('-start_time',)
    actions = ['delete_selected']

    def delete_selected(self, request, queryset):
        for obj in queryset:
            obj.delete()

    delete_selected.short_description = "Delete selected requests"


class ConnectionAdmin(admin.ModelAdmin):
    list_display = ['id',
                    'cluster',
                    'timestamp',
                    'from_ip',
                    'from_uid_pod',
                    'from_uid_service',
                    'from_uid_deployment',
                    'from_uid_daemonset',
                    'from_uid_statefulset',
                    'from_port',
                    'to_ip',
                    'to_uid_pod',
                    'to_uid_service',
                    'to_url_outbound',
                    'to_uid_deployment',
                    'to_uid_daemonset',
                    'to_uid_statefulset',
                    'to_port',
                    'date_created',
                    'date_updated'
                    ]

    list_filter = ('cluster',)
    readonly_fields = ("id",)
    search_fields = ['id', 'from_uid_pod',
                     'to_uid_pod', 'to_uid_service', 'to_url_outbound']
    ordering = ('-date_created',)
    actions = ['delete_selected']

    def delete_selected(self, request, queryset):
        for obj in queryset:
            obj.delete()

    delete_selected.short_description = "Delete selected requests"

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True



class EndpointAdmin(admin.ModelAdmin):
    list_display = ['uid',
                    'cluster',
                    'name',
                    'namespace',
                    'addresses',
                    'deleted',
                    'date_created',
                    'date_updated']

    list_filter = ("cluster", "namespace", "deleted")
    search_fields = ['uid', 'name', 'namespace', 'cluster']
    ordering = ('-date_created',)

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True


class ContainerAdmin(admin.ModelAdmin):
    list_display = ['uid',
                    'cluster',
                    'name',
                    'namespace',
                    'pod',
                    'ports',
                    'image',
                    'has_logs',
                    'container_nums',
                    'date_created',
                    'date_updated']

    list_filter = ("namespace", 'cluster', 'has_logs')
    search_fields = ['uid', 'name', 'namespace', 'pod', 'image']
    ordering = ('-date_created',)

    def delete_selected(self, request, queryset):
        for obj in queryset:
            obj.delete()

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True


class ReplicaSetAdmin(admin.ModelAdmin):
    list_display = ['uid',
                    'cluster',
                    'name',
                    'namespace',
                    'owner',
                    'deleted',
                    'replicas',
                    'date_created',
                    'date_updated']

    list_filter = ("namespace", 'deleted', 'cluster')
    search_fields = ['uid', 'name', 'namespace', 'replicas']
    ordering = ('-date_created',)

    def delete_selected(self, request, queryset):
        for obj in queryset:
            obj.delete()

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True


class DaemonSetAdmin(admin.ModelAdmin):
    list_display = ['uid',
                    'cluster',
                    'name',
                    'namespace',
                    'deleted',
                    'date_created',
                    'date_updated']

    list_filter = ("namespace", 'deleted', 'cluster')
    search_fields = ['uid', 'name', 'namespace']
    ordering = ('-date_created',)

    def delete_selected(self, request, queryset):
        for obj in queryset:
            obj.delete()

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

class StatefulSetAdmin(admin.ModelAdmin):
    list_display = ['uid',
                    'cluster',
                    'name',
                    'namespace',
                    'deleted',
                    'date_created',
                    'date_updated']

    list_filter = ("namespace", 'deleted', 'cluster')
    search_fields = ['uid', 'name', 'namespace']
    ordering = ('-date_created',)

    def delete_selected(self, request, queryset):
        for obj in queryset:
            obj.delete()

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True


class K8sEventAdmin(admin.ModelAdmin):
    list_display = ['cluster',
                    'event_name',
                    'kind',
                    'namespace',
                    'name',
                    'uid',
                    'reason',
                    'message',
                    'count',
                    'first_timestamp',
                    'last_timestamp']
    
    list_filter = ("kind", "namespace", "reason", "cluster")
    
    search_fields = ['cluster', 'event_name', 'kind', 'namespace', 'name', 'uid', 'reason', 'message']
    
    ordering = ('-last_timestamp',)


# admin.site.register(Test, TestAdmin)
# admin.site.register(TestPlanTemplate, TestTemplateAdmin)
# admin.site.register(Environment, EnvironmentAdmin)
admin.site.register(Setting, SettingAdmin)
admin.site.register(Cluster, ClusterAdmin)
admin.site.register(Pod, PodAdmin)
admin.site.register(Deployment, DeploymentAdmin)
admin.site.register(Service, ServiceAdmin)
admin.site.register(Request, RequestAdmin)
admin.site.register(KafkaEvent, KafkaEventAdmin)
admin.site.register(Connection, ConnectionAdmin)
admin.site.register(Endpoint, EndpointAdmin)
admin.site.register(Container, ContainerAdmin)
admin.site.register(ReplicaSet, ReplicaSetAdmin)
admin.site.register(DaemonSet, DaemonSetAdmin)
admin.site.register(StatefulSet, StatefulSetAdmin)
admin.site.register(K8sEvent, K8sEventAdmin)

# if settings.ANTEON_ENV == "onprem":
#     admin.site.unregister(beat_admin.PeriodicTask)
#     admin.site.unregister(beat_admin.ClockedSchedule)
#     admin.site.unregister(beat_admin.CrontabSchedule)
#     admin.site.unregister(beat_admin.IntervalSchedule)
#     admin.site.unregister(beat_admin.SolarSchedule)
# else:
#     admin.site.register(Scheduler, SchedulerAdmin)

# admin.site.register(Scheduler, SchedulerAdmin)