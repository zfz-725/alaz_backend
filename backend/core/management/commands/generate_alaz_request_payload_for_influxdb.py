import datetime
from django.core.management.base import BaseCommand

from core.models import Cluster, Request

class Command(BaseCommand):
    help = 'Populate mock service map resources with fake data'

    def add_arguments(self, parser):
        parser.add_argument('start_time', type=int, help='Start timestamp in ms')
        parser.add_argument('end_time', type=int, help='End timestamp in ms')
        parser.add_argument('monitoring_id', help='Monitoring id of the cluster to retrieve requests from')
        parser.add_argument('file_name', type=str, help='File name to write to')

    def handle(self, *args, **kwargs):
        start_time = kwargs['start_time']
        end_time = kwargs['end_time']
        monitoring_id = kwargs['monitoring_id']
        file_name = kwargs['file_name']

        cluster = Cluster.objects.get(monitoring_id=monitoring_id)

        # turn start/end times to datetime
        start_time = datetime.datetime.fromtimestamp(start_time / 1000)
        end_time = datetime.datetime.fromtimestamp(end_time / 1000)

        print(start_time, end_time)

        # requests = AlazRequest.objects.filter(start_time__gte=start_time, start_time__lte=end_time, cluster=cluster).order_by('start_time')
        requests = Request.objects.filter().all()[:20000]
        # create the file
        f = open(file_name, "w")

        measurement = 'alaz_requests'

        print(requests.count())

        # measurement,monitoring_id,to_port,from_uid,to_uid,protocol,path,method,status_code start_time,latency,fail_reason,from_ip,to_ip,from_port,from_uid_pod,to_uid_pod,tls

        for request in requests:
            from_uid = request.from_uid_deployment if request.from_uid_deployment else request.from_uid_daemonset if request.from_uid_daemonset else request.from_uid_pod if request.from_uid_pod else request.from_uid_service if request.from_uid_service else None
            to_uid = request.to_uid_deployment if request.to_uid_deployment else request.to_uid_daemonset if request.to_uid_daemonset else request.to_uid_pod if request.to_uid_pod else request.to_uid_service if request.to_uid_service else request.to_url_outbound if request.to_url_outbound else None
            # Turn start_time datetime to ms timestamp
            timestamp_ms = int(request.start_time.timestamp() * 1000)
            if not from_uid or not to_uid:
                print(f'from_uid or to_uid is None. Skipping request: {request}')
                continue
            f.write(f'{measurement},monitoring_id="{monitoring_id}",to_port="{request.to_port}",from_uid="{from_uid}",to_uid="{to_uid}",protocol="{request.protocol}",path="{request.path}",method="{request.method}",status_code="{request.status_code}" latency={request.latency},fail_reason="{request.fail_reason}",from_ip="{request.from_ip}",to_ip="{request.to_ip}",from_port="{request.from_port}",from_uid_pod="{request.from_uid_pod}",to_uid_pod="{request.to_uid_pod}",tls="{request.tls}" {timestamp_ms}\n')