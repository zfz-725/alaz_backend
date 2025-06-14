from django.db import models

from accounts.models import User
from core.models import Cluster

# Create your models here.

class MetricCount(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    cluster = models.ForeignKey(Cluster, on_delete=models.SET_NULL, null=True)  # It should stay even if the cluster is deleted
    count = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f'{self.user.email} - {self.cluster.name} - {self.count}'