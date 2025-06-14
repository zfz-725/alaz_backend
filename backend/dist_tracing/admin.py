# from django.contrib import admin

# from dist_tracing.models import Span, Trace, Traffic

# # Register your models here.
# class SpanAdmin(admin.ModelAdmin):
#     list_display = ('id', 'cluster', 'name', 'trace_id', 'parent_id', 'start_time', 'end_time', 'attributes', 'events', 'node_id', 'egress_tcp_num', 'egress_thread_id', 'ingress_tcp_num', 'ingress_thread_id', 'date_created', 'date_updated')
#     list_filter = ('name', 'cluster', )

#     search_fields = ('name', 'trace_id', 'parent_id', )
#     ordering = ('-date_created',)
#     actions = ['delete_selected']

#     def delete_selected(self, request, queryset):
#         for obj in queryset:
#             obj.delete()


# class TrafficAdmin(admin.ModelAdmin):
#     list_display = ('id', 'cluster_id', 'timestamp', 'tcp_seq_num', 'thread_id', 'ingress', 'span_exists', 'node_id', 'date_created')
#     list_filter = ('node_id', )

#     search_fields = ('tcp_seq_num', 'thread_id', )
#     ordering = ('-date_created', 'cluster_id', )
#     actions = ['delete_selected']

#     def delete_selected(self, request, queryset):
#         for obj in queryset:
#             obj.delete()


# class TraceAdmin(admin.ModelAdmin):
#     list_display = ('id', 'cluster', 'name', 'start_time', 'end_time', 'attributes', 'duration_ms', 'span_count', 'date_created', 'date_updated')
#     list_filter = ('name', 'cluster', )

#     search_fields = ('name', 'id', )
#     ordering = ('-date_created',)
#     actions = ['delete_selected']

#     def delete_selected(self, request, queryset):
#         for obj in queryset:
#             obj.delete()

# admin.site.register(Span, SpanAdmin)
# admin.site.register(Traffic, TrafficAdmin)
# admin.site.register(Trace, TraceAdmin)