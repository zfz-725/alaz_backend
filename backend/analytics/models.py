from django.db import models
from django.utils import timezone

# Create your models here.

class Telemetry(models.Model):
    timestamp = models.DateTimeField(blank=True, null=False)  # Allow blank for user-provided timestamps
    onprem_key = models.UUIDField(blank=True, null=True)
    monitoring_id = models.UUIDField(blank=False, null=True)
    data = models.JSONField(blank=True, null=True)
    company_name = models.CharField(max_length=255, blank=True, null=True)
    contact = models.CharField(max_length=255, blank=True, null=True)

    def save(self, *args, **kwargs):
        if not self.timestamp:  # If timestamp is not provided
            self.timestamp = timezone.now()  # Set it to the current time
        super(Telemetry, self).save(*args, **kwargs)
