from django.db.models.signals import pre_delete, post_delete, post_save, pre_save
from django.dispatch import receiver
from accounts.models import User
from core.requester import MyBackendRequester
from core.models import Cluster, Container, DaemonSet, Deployment, Endpoint, Pod, ReplicaSet, Service, Setting, StatefulSet 

@receiver(post_delete, sender=Cluster)
def delete_cluster_resources(sender, instance, **kwargs):
    Pod.objects.using('default').filter(cluster=instance.uid).delete()
    ReplicaSet.objects.using('default').filter(cluster=instance.uid).delete()
    Deployment.objects.using('default').filter(cluster=instance.uid).delete()
    DaemonSet.objects.using('default').filter(cluster=instance.uid).delete()
    Endpoint.objects.using('default').filter(cluster=instance.uid).delete()
    Service.objects.using('default').filter(cluster=instance.uid).delete()
    Container.objects.using('default').filter(cluster=instance.uid).delete()
    StatefulSet.objects.using('default').filter(cluster=instance.uid).delete()

@receiver(pre_save, sender=Cluster)
def set_cluster_as_not_fresh(sender, instance, **kwargs):
    # Skip if cluster does not exist
    if not Cluster.objects.filter(uid=instance.uid).exists():
        return
    # Check if fresh is set from False to True
    cluster = Cluster.objects.get(uid=instance.uid)
    if cluster.fresh == True and instance.fresh == False:
        my_backend_requester = MyBackendRequester()
        my_backend_requester.set_cluster_as_not_fresh(instance.monitoring_id)

@receiver(pre_delete, sender=ReplicaSet)
def delete_replicaset_pods(sender, instance, **kwargs):
    Pod.objects.using('default').filter(replicaset_owner=instance.uid).delete()

@receiver(pre_delete, sender=Deployment)
def delete_deployment_replicasets(sender, instance, **kwargs):
    ReplicaSet.objects.using('default').filter(owner=instance.uid).delete()

@receiver(pre_delete, sender=DaemonSet)
def delete_daemonset_pods(sender, instance, **kwargs):
    Pod.objects.using('default').filter(daemonset_owner=instance.uid).delete()

@receiver(post_save, sender=Pod)
def delete_pod_containers(sender, instance, **kwargs):
    # Delete containers if pod is set to deleted
    if instance.deleted == True:
        Container.objects.using('default').filter(pod=instance.uid).delete()

@receiver(pre_delete, sender=Pod)
def delete_pod_containers(sender, instance, **kwargs):
    Container.objects.using('default').filter(pod=instance.uid).delete()