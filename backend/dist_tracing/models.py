# from django.db import models

# # Create your models here.

# class Span(models.Model):
#     # cluster = models.ForeignKey(Cluster, on_delete=models.SET_NULL, null=True, blank=True)
#     cluster = models.UUIDField(null=True, blank=True)
#     name = models.CharField(max_length=1024, null=False, blank=True)
#     start_time = models.DateTimeField()
#     end_time = models.DateTimeField()
#     attributes = models.JSONField(default=dict, null=False, blank=True)
#     events = models.JSONField(default=list, null=False, blank=True)
#     trace_id = models.BigIntegerField(null=True, blank=True)
#     parent_id = models.BigIntegerField(null=True, blank=True)
#     node_id = models.CharField(max_length=256, null=True, blank=True)

#     egress_tcp_num = models.BigIntegerField(null=True, blank=True)
#     egress_thread_id = models.BigIntegerField(null=True, blank=True)
#     ingress_tcp_num = models.BigIntegerField(null=True, blank=True)
#     ingress_thread_id = models.BigIntegerField(null=True, blank=True)
#     # egress_traffic = models.ForeignKey('Traffic', on_delete=models.CASCADE, related_name='egress_traffic')
#     # ingress_traffic = models.ForeignKey('Traffic', on_delete=models.CASCADE, related_name='ingress_traffic', null=True, blank=True)
#     # request = models.ForeignKey(Request, on_delete=models.CASCADE, null=True, blank=False)

#     date_created = models.DateTimeField(auto_now_add=True)
#     date_updated = models.DateTimeField(auto_now=True)

#     class Meta:
#         index_together = [('date_created', )]
#         indexes = [
#             models.Index(fields=['trace_id'])
#         ]

#     def __str__(self) -> str:
#         return f"{self.id}"

#     def save(self, *args, **kwargs):
#         to_delete = []
#         for key, data in self.attributes.items():
#             # Attributes should be flat
#             if isinstance(data, dict):
#                 to_delete.append(key)
#             elif isinstance(data, list):
#                 to_delete.append(key)
#             # Force integers, floats and booleans to strings in order to allow trace querying
#             elif isinstance(data, int) or isinstance(data, float) or isinstance(data, bool):
#                 self.attributes[key] = str(data)

#         for key in to_delete:
#             del self.attributes[key]
        
#         super(Span, self).save(*args, **kwargs)


# class Traffic(models.Model):
#     cluster_id = models.UUIDField()
#     timestamp = models.DateTimeField()   
#     tcp_seq_num = models.BigIntegerField()
#     thread_id = models.BigIntegerField()
#     ingress = models.BooleanField()
#     node_id = models.CharField(max_length=256)

#     # Assuming that a traffic can't be a part of both spans
#     span_exists = models.BooleanField(default=False)

#     date_created = models.DateTimeField(auto_now_add=True)

#     class Meta:
#         index_together = [('date_created', )]

#     def __str__(self) -> str:
#         return f" id: {self.id}, tcp: {self.tcp_seq_num}, node_id: {self.node_id}, thread_id: {self.thread_id}, ingress: {self.ingress}"


# class Trace(models.Model):
#     # cluster = models.ForeignKey(Cluster, on_delete=models.SET_NULL, null=True, blank=True)
#     cluster = models.UUIDField(null=True, blank=True)
#     name = models.CharField(max_length=1024, null=False, blank=True)
#     start_time = models.DateTimeField()
#     end_time = models.DateTimeField()
#     attributes = models.JSONField(default=dict, null=False, blank=True)
#     duration_ms = models.FloatField(null=True, blank=True)
#     # Spans are referenced by foreign keys
#     # spans = models.ManyToManyField(Span, related_name='spans')
#     span_count = models.IntegerField(default=0, null=False, blank=False)

#     date_created = models.DateTimeField(auto_now_add=True)
#     date_updated = models.DateTimeField(auto_now=True)

#     def __str__(self) -> str:
#         return f"{self.id}"

#     def save(self, *args, **kwargs):
#         if self.start_time and self.end_time:
#             self.duration_ms = (self.end_time - self.start_time).total_seconds() * 1000
#         else:
#             self.duration_ms = None
#         super().save(*args, **kwargs)

#     class Meta:
#         index_together = [('start_time', 'end_time')]
#         indexes = [
#             models.Index(fields=['cluster']),
#             models.Index(fields=['name'])
#         ]