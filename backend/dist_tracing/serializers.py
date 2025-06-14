# import datetime
# import pytz
# from rest_framework import serializers

# # from dist_tracing.models import Span, Trace

# class SpanRetrieveSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Span
#         exclude = ('id', 'date_created', 'date_updated', 'egress_tcp_num', 'egress_thread_id', 'ingress_tcp_num', 'ingress_thread_id', 'node_id')

#     def to_representation(self, instance):
#         representation = super().to_representation(instance)
#         representation['context'] = {
#             'trace_id': instance.trace_id,
#             'span_id': str(instance.id),
#         }
#         representation['parent_id'] = str(instance.parent_id) if instance.parent_id else None
#         del representation['parent_id']
#         representation['attributes']['monitoring_id'] = self.context['monitoring_id']
#         del representation['cluster']
#         del representation['trace_id']
#         formatted_start_time = instance.start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#         formatted_end_time = instance.end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#         representation['start_time'] = formatted_start_time
#         representation['end_time'] = formatted_end_time
#         return representation


# class SpanListSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Span
#         exclude = ('id', 'date_created', 'date_updated', 'egress_tcp_num', 'egress_thread_id', 'ingress_tcp_num', 'ingress_thread_id', 'node_id')

#     def to_representation(self, instance):
#         representation = super().to_representation(instance)
#         representation['context'] = {
#             'trace_id': instance.trace_id,
#             'span_id': str(instance.id),
#         }
#         representation['parent_id'] = str(instance.parent_id) if instance.parent_id else None
#         del representation['parent']
#         representation['attributes']['monitoring_id'] = self.context['monitoring_id']
#         del representation['cluster']
#         del representation['trace_id']
#         formatted_start_time = instance.start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#         formatted_end_time = instance.end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#         representation['start_time'] = formatted_start_time
#         representation['end_time'] = formatted_end_time
#         return representation


# class SpanCreateSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Span
#         fields = '__all__'

# class TraceListSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Trace
#         exclude = ['date_created', 'date_updated', 'attributes']

#     def to_representation(self, instance):
#         repr = super().to_representation(instance)
#         del repr['cluster']
#         return repr


# class TraceRetrieveSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Trace
#         exclude = ['date_created', 'date_updated', 'attributes']

#     def to_representation(self, instance):
#         repr = super().to_representation(instance)
#         # spans = instance.spans.all().order_by('start_time')
#         spans = Span.objects.filter(trace_id=instance.id).order_by('start_time')
#         serializer = SpanRetrieveSerializer(spans, many=True, context=self.context)
#         repr['spans'] = serializer.data
#         del repr['cluster']
#         formatted_start_time = instance.start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#         formatted_end_time = instance.end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#         repr['start_time'] = formatted_start_time
#         repr['end_time'] = formatted_end_time
#         return repr


# class TraceWriteSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Trace
#         fields = '__all__'