from datetime import timedelta
from django.core.management.base import BaseCommand
from core.models import Cluster, Request, Deployment, DaemonSet, Service
from dist_tracing.utils import add_span_to_trace, get_or_create_trace_of_span
from dist_tracing.models import Span, Trace, Traffic
from django.utils import timezone
from faker import Faker
import random

fake = Faker()


class Command(BaseCommand):
    help = 'Populate mock service map resources with fake data'

    def add_arguments(self, parser):
        parser.add_argument('traffic_count', type=int, help='Number of traffic to create')
        parser.add_argument('span_count', help='Number of spans to create')
        parser.add_argument('trace_count', help='Number of traces to create')
        parser.add_argument('monitoring_id', help='Monitoring id of the cluster to add requests to.')

    def handle(self, *args, **kwargs):
        monitoring_id = kwargs['monitoring_id']
        cluster_qs = Cluster.objects.filter(monitoring_id=monitoring_id)
        if not cluster_qs.exists():
            print("No cluster with that monitoring id exists.")
            return
        cluster = cluster_qs.first()
        traffic_count = int(kwargs['traffic_count'])
        span_count = int(kwargs['span_count'])
        trace_count = int(kwargs['trace_count'])

        start_time = timezone.now() - timedelta(days = 4)
        end_time = timezone.now() - timedelta(days = 5)

        # request = Request.objects.all().first()

        deployment_1 = Deployment.objects.create(
            cluster=cluster.uid,
            name='backend',
            namespace='testserver',
            uid=fake.uuid4(),
            replicas=random.randint(0, 5),
        )

        daemonset_1 = DaemonSet.objects.create(
            cluster=cluster.uid,
            name='collector',
            namespace='testserver',
            uid=fake.uuid4(),
        )

        service_1 = Service.objects.create(
            cluster=cluster.uid,
            name='backend',
            namespace='testserver',
            uid=fake.uuid4(),
            type=random.choice(['ClusterIP', 'NodePort', 'LoadBalancer']),
            ports=[{"src": 53, "dest": 53, "protocol": "UDP"}, {"src": 53, "dest": 53, "protocol": "TCP"}]
        )

        deployment_2 = Deployment.objects.create(
            cluster=cluster.uid,
            name='frontend',
            namespace='testserver',
            uid=fake.uuid4(),
            replicas=random.randint(0, 5),
        )

        daemonset_2 = DaemonSet.objects.create(
            cluster=cluster.uid,
            name='exporter',
            namespace='testserver',
            uid=fake.uuid4(),
        )

        service_2 = Service.objects.create(
            cluster=cluster.uid,
            name='frontend',
            namespace='testserver',
            uid=fake.uuid4(),
            type=random.choice(['ClusterIP', 'NodePort', 'LoadBalancer']),
            ports=[{"src": 53, "dest": 53, "protocol": "UDP"}, {"src": 53, "dest": 53, "protocol": "TCP"}]
        )

        traffic_bulk = []
        for i in range(traffic_count):
            traffic_bulk.append(Traffic(
                cluster_id=cluster.uid,
                timestamp=start_time,
                tcp_seq_num=random.randint(0, 1000),
                thread_id=random.randint(0, 1000),
                ingress=random.randint(0, 1),
                node_id=fake.uuid4(),
            ))

            if i % 100 == 0:
                Traffic.objects.bulk_create(traffic_bulk)
                traffic_bulk = []
                print(f"Created {i} traffic")

        traffic_bulk = []
        span_bulk = []
        for i in range(span_count):
            traffic = Traffic(
                cluster_id=cluster.uid,
                timestamp=start_time,
                tcp_seq_num=random.randint(0, 1000),
                thread_id=random.randint(0, 1000),
                ingress=random.randint(0, 1),
                node_id=fake.uuid4(),
            )
            
            traffic_bulk.append(traffic)
    
            span_bulk.append(Span(
                cluster=cluster.uid,
                name=fake.name(),
                parent_id=None,
                start_time=start_time,
                end_time=end_time,
                attributes={},
                events=[],
                trace_id=None,
                egress_tcp_num = traffic.tcp_seq_num,
                egress_thread_id = traffic.thread_id,
            ))
            
            if i % 100 == 0:
                Traffic.objects.bulk_create(traffic_bulk)
                Span.objects.bulk_create(span_bulk)
                traffic_bulk = []
                span_bulk = []
                print(f"Created {i} spans")


        for i in range(trace_count):
            minutes_ago = random.randint(1, 100)
            start_time = timezone.now() - timedelta(minutes=minutes_ago)
            # Span attributes
        #                 'name': name,
        #     'start_time': egress.timestamp,
        #     'end_time': egress.timestamp + timedelta(seconds=latency),
        #     'egress_traffic': egress.id,
        #     'ingress_traffic': ingress.id if ingress else None,
        #     'cluster': egress.cluster.uid,
        #     'attributes': {
        #         'from_ip': request.from_ip,
        #         'to_ip': request.to_ip,
        #         'from_port': request.from_port,
        #         'to_port': request.to_port,
        #         'protocol': request.protocol,
        #         'status_code': request.status_code,
        #         'method': request.method,
        #         'path': request.path,
        #     },
        # }
            trace_span_count = random.randint(1, 10)
            last_span = None
            spans = []
            trace_id = None
            last_span_duration = 0
            for _ in range(trace_span_count):
                traffic = Traffic.objects.create(
                    cluster_id=cluster.uid,
                    timestamp=start_time,
                    tcp_seq_num=random.randint(0, 1000),
                    thread_id=random.randint(0, 1000),
                    ingress=random.randint(0, 1),
                    node_id=fake.uuid4(),
                )

                # print(f"Creating span {start_time} {last_span_duration}")

                if last_span is None:
                    # Generate a random span as first
                    duration_ms = random.randint(1000, 3000)
                    span_start = start_time
                    span_end = span_start + timedelta(milliseconds=duration_ms)
                else:
                    # The next spans should start later than the previous, and end before it
                    start_delay = random.randint(1, 100)
                    # print(f"Start delay: {start_delay}, last span duration: {last_span_duration}")
                    try:
                        duration_ms = random.randint(100, last_span_duration - start_delay)
                    except:
                        break
                    span_start = start_time + timedelta(milliseconds=start_delay)
                    span_end = span_start + timedelta(milliseconds=duration_ms)

                start_time = span_start
                last_span_duration = duration_ms

                # duration_ms = random.randint(500, 3000) if last_span is None else 
                # span_start = start_time if last_span is None else start_time + timedelta(milliseconds=random.randint(1, last_span_duration))
                # start_time = span_start
                # span_end = span_start + timedelta(milliseconds=duration_ms)
                protocol = random.choice(['HTTP', 'POSTGRES', 'AMQP', 'HTTPS'])
                if last_span is None or protocol in ['HTTP', 'HTTPS']:
                    protocol = random.choice(['HTTP', 'HTTPS'])
                    # Generate a random endpoint (not url)
                    version = random.choice(['v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8'])
                    name = version + '/' + random.choice(['users', 'teams', 'accounts', 'billing', 'payments', 'subscriptions', 'auth', 'login', 'logout', 'register', 'api'])
                    status_code = random.randint(200, 500)
                    method = random.choice(['GET', 'POST', 'PUT', 'DELETE'])
                elif protocol == 'POSTGRES':
                    # Generate random full queries
                    name = random.choice(['SELECT * FROM users WHERE user_id = 5', 'INSERT INTO users VALUES (7, \'test_user\', \'test@ddosify.com\')', 'UPDATE users where user_id = 5 set name = \'test_user\'', 'DELETE FROM users', 'DROP TABLE users', 'ALTER TABLE users ALTER COLUMN user_email VARCHAR(256) NULL', 'TRUNCATE users', 'BEGIN', 'COMMIT', 'ROLLBACK'])
                    status_code = random.randint(0, 1) 
                    method = random.choice(['SIMPLE_QUERY', 'CLOSE_OR_TERMINATE'])
                elif protocol == 'AMQP':
                    method = random.choice(['PUBLISH'])
                    name = method
                    status_code = 0

                from_type = random.choice(['deployment', 'daemonset', 'service'])
                to_type = random.choice(['deployment', 'daemonset', 'service'])

                if from_type == 'deployment':
                    from_uid = deployment_1.uid
                    from_name = deployment_1.name
                elif from_type == 'daemonset':
                    from_uid = daemonset_1.uid
                    from_name = daemonset_1.name
                else:
                    from_uid = service_1.uid
                    from_name = service_1.name

                if to_type == 'deployment':
                    to_uid = deployment_2.uid
                    to_name = deployment_2.name
                elif to_type == 'daemonset':
                    to_uid = daemonset_2.uid
                    to_name = daemonset_2.name
                else:
                    to_uid = service_2.uid
                    to_name = service_2.name
        
                span = Span.objects.create(
                    cluster=cluster.uid,
                    name=name,
                    parent_id=last_span.id if last_span else None,
                    start_time=span_start,
                    end_time=span_end,
                    attributes={
                        'from_ip': fake.ipv4(),
                        'to_ip': fake.ipv4(),
                        'from_port': random.randint(0, 1000),
                        'to_port': random.randint(0, 1000),
                        'protocol': protocol,
                        'status_code': status_code,
                        'method': method,
                        'path': name,
                        'from_type': from_type,
                        'to_type': to_type,
                        'from_uid': from_uid,
                        'to_uid': to_uid,
                        'from_name': from_name,
                        'to_name': to_name,
                    },
                    events=[],
                    trace_id=None,
                    egress_tcp_num = traffic.tcp_seq_num,
                    egress_thread_id = traffic.thread_id,
                )

                if last_span is not None:
                    add_span_to_trace(trace_id, span)
                else:
                    trace_id = get_or_create_trace_of_span(span)

                spans.append(span)
                last_span = span
                
            if i % 100 == 0:
                print(f"Created {i} traces")

        self.stdout.write(self.style.SUCCESS(f'Successfully populated {traffic_count} traffic, {span_count} spans, {trace_count} traces.'))