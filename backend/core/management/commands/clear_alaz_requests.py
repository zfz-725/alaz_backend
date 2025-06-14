from django.core.management.base import BaseCommand, CommandParser
from core.models import Cluster, Request


class Command(BaseCommand):
    help = 'Clear AlazRequests'

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument('cluster', type=str, help='Monitoring id of the cluster to clear requests from')

    def handle(self, *args, **kwargs):
        monitoring_id = kwargs['cluster']
        cluster = Cluster.objects.get(monitoring_id=monitoring_id)
        Request.objects.using('default').filter(cluster=cluster.uid).delete()

        self.stdout.write(self.style.SUCCESS(f'Successfully cleared AlazRequests.'))