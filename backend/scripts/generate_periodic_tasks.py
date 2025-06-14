import json
import os
import sys

import django

sys.path.append('/workspace/backend')
sys.path.append('/workspaces/alaz-backend/backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
from django.conf import settings
django.setup()
from datetime import datetime, timedelta

from django_celery_beat.models import IntervalSchedule, PeriodicTask, CrontabSchedule

DAYS = 'days'
HOURS = 'hours'
MINUTES = 'minutes'
SECONDS = 'seconds'
MICROSECONDS = 'microseconds'

task_set_clusters_with_old_heartbeats_as_not_alive = {
    'every': 30,
    'period': SECONDS,
    'task': 'core.tasks.set_clusters_with_old_heartbeats_as_not_alive',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_clear_alaz_requests = {
    'every': 5,
    'period': MINUTES,
    'task': 'core.tasks.clear_alaz_requests',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_clear_kafka_events = {
    'every': 1,
    'period': HOURS,
    'task': 'core.tasks.clear_kafka_events',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_delete_expired_instances_of_clusters = {
    'every': 1,
    'period': DAYS,
    'task': 'core.tasks.delete_expired_instances_of_clusters',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_update_latest_instances_of_clusters = {
    'every': 30,
    'period': SECONDS,
    'task': 'core.tasks.update_latest_instances_of_clusters',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}


task_write_kafka_events_from_redis_to_db = {
    'every': 5,
    'period': SECONDS,
    'task': 'core.tasks.write_kafka_events_from_redis_to_db',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_clear_unused_resources = {
    'every': 1,
    'period': DAYS,
    'task': 'core.tasks.clear_unused_resources',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_write_connections_from_redis_to_db = {
    'every': 5,
    'period': SECONDS,
    'task': 'core.tasks.write_connections_from_redis_to_db',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_cache_alaz_resources_in_redis = {
    'every': 1,
    'period': MINUTES,
    'task': 'core.tasks.cache_alaz_resources_in_redis',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_clear_connections = {
    'every': 1,
    'period': HOURS,
    'task': 'core.tasks.clear_connections',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_update_version_from_github = {
    'every': 30,
    'period': MINUTES,
    'task': 'core.tasks.update_version_from_github',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

# task_update_containers_without_logs = {
#     'every': 5,
#     'period': MINUTES,
#     'task': 'core.tasks.update_containers_without_logs',
#     'enabled': True,
#     'last_run_at': None,
#     'kwargs': {}
# }

task_core_clear_orphans = {
    'every': 1,
    'period': HOURS,
    'task': 'core.tasks.core_clear_orphans',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_remove_duplicate_containers = {
    'every': 10,
    'period': MINUTES,
    'task': 'core.tasks.remove_duplicate_containers',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_sync_clusters = {
    'every': 1,
    'period': MINUTES,
    'task': 'core.tasks.sync_clusters',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_cache_pods_and_clusters = {
    'every': 1,
    'period': MINUTES,
    'task': 'core.tasks.cache_pods_and_clusters',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}

task_clear_redis_if_full = {
    'every': 10,
    'period': SECONDS,
    'task': 'core.tasks.clear_redis_if_full',
    'enabled': True,
    'last_run_at': None,
    'kwargs': {}
}


if settings.ANTEON_ENV == 'onprem':
    periodic_tasks = {
        'task_send_telemetry_data_to_cloud': {
            'every': settings.TELEMETRY_DATA_INTERVAL_SECONDS,
            'period': SECONDS,
            'task': 'analytics.tasks.send_telemetry_data_to_cloud',
            'enabled': True,
            'last_run_at': datetime.now() - timedelta(days=90),
            'kwargs': {}
        },
        'task_check_enterprise_selfhosted': {
            'every': 5,
            'period': MINUTES,
            'task': 'accounts.tasks.check_enterprise_selfhosted',
            'enabled': True,
            'last_run_at': None,
            'kwargs': {}
        },
        'task_update_users_teams': {
            'every': 5,
            'period': MINUTES,
            'task': 'accounts.tasks.update_users_teams',
            'enabled': True,
            'last_run_at': None,
            'kwargs': {}
        },
    }
else:
    periodic_tasks = {
        'task_update_users_teams': {
            'every': 30,
            'period': MINUTES,
            'task': 'accounts.tasks.update_users_teams',
            'enabled': True,
            'last_run_at': None,
            'kwargs': {}
        },
        'task_check_log_lag': {
            'every': 1,
            'period': HOURS,
            'task': 'core.tasks.check_log_lag',
            'enabled': True,
            'last_run_at': None,
            'kwargs': {
                # TODO: Change these
                'monitoring_id': '3bc2d8b5-110b-41b8-9eea-7dda0bfc3b08',
                'deployment_name': 'test',
                'container_name': 'TammySweeney'
            }
        },
        # 'task_set_log_usage': {
        #     'every': 5,
        #     'period': MINUTES,
        #     'task': 'core.tasks.set_log_usage',
        #     'enabled': True,
        #     'last_run_at': None,
        #     'kwargs': {}
        # }
    }

periodic_tasks['task_set_clusters_with_old_heartbeats_as_not_alive'] = task_set_clusters_with_old_heartbeats_as_not_alive
periodic_tasks['task_clear_alaz_requests'] = task_clear_alaz_requests
periodic_tasks['task_delete_expired_instances_of_clusters'] = task_delete_expired_instances_of_clusters
periodic_tasks['task_update_latest_instances_of_clusters'] = task_update_latest_instances_of_clusters
periodic_tasks['clear_unused_resources'] = task_clear_unused_resources
periodic_tasks['task_write_connections_from_redis_to_db'] = task_write_connections_from_redis_to_db
periodic_tasks['task_cache_alaz_resources_in_redis'] = task_cache_alaz_resources_in_redis
periodic_tasks['task_clear_connections'] = task_clear_connections
periodic_tasks['task_update_version_from_github'] = task_update_version_from_github
# periodic_tasks['task_update_containers_without_logs'] = task_update_containers_without_logs
periodic_tasks['task_core_clear_orphans'] = task_core_clear_orphans
periodic_tasks['task_remove_duplicate_containers'] = task_remove_duplicate_containers
periodic_tasks['task_sync_clusters'] = task_sync_clusters
periodic_tasks['task_clear_kafka_events'] = task_clear_kafka_events
periodic_tasks['task_write_kafka_events_from_redis_to_db'] = task_write_kafka_events_from_redis_to_db
periodic_tasks['task_sync_clusters'] = task_sync_clusters
periodic_tasks['task_cache_pods_and_clusters'] = task_cache_pods_and_clusters
periodic_tasks['task_clear_redis_if_full'] = task_clear_redis_if_full

if settings.BLOAT_NOTIFICATION_ENABLED:
    periodic_tasks['check_redis_size'] = {
        'every': 1,
        'period': HOURS,
        'task': 'core.tasks.check_redis_size',
        'enabled': True,
        'last_run_at': None,
        'kwargs': {}
    }

    periodic_tasks['check_postgres_size'] = {
        'every': 1,
        'period': HOURS,
        'task': 'core.tasks.check_postgres_size',
        'enabled': True,
        'last_run_at': None,
        'kwargs': {}
    }

    periodic_tasks['check_rabbitmq_usage'] = {
        'every': 1,
        'period': HOURS,
        'task': 'core.tasks.check_rabbitmq_usage',
        'enabled': True,
        'last_run_at': None,
        'kwargs': {}
    }

    # periodic_tasks['check_clickhouse_size'] = {
    #     'every': 1,
    #     'period': HOURS,
    #     'task': 'core.tasks.check_clickhouse_size',
    #     'enabled': True,
    #     'last_run_at': None,
    #     'kwargs': {}
    # }

# if settings.TCPDUMP_ENABLED:
#     task_write_tcp_dump_to_clickhouse = {
#         'every': 5,
#         'period': SECONDS,
#     'task': 'core.tasks.write_tcp_dump_to_clickhouse',
#         'enabled': True,
#         'last_run_at': None,
#         'kwargs': {}
#     }
#     periodic_tasks['task_write_tcp_dump_to_clickhouse'] = task_write_tcp_dump_to_clickhouse

def get_interval_schedule(task_configuration):
    interval_schedules_queryset = IntervalSchedule.objects.filter(
        every=task_configuration['every'], period=task_configuration['period'])
    if not interval_schedules_queryset:
        interval_schedule = IntervalSchedule(every=task_configuration['every'], period=task_configuration['period'])
        interval_schedule.save()
    else:
        interval_schedule = interval_schedules_queryset[0]
    return interval_schedule


if __name__ == '__main__':
    # Delete periodic tasks not in the periodic_tasks dict
    existing_tasks = PeriodicTask.objects.all()
    for existing_task in existing_tasks:
        if existing_task.name not in periodic_tasks:
            # (celery.backend_cleanup) is a default periodic task that should not be deleted
            if existing_task.name.startswith('celery.'):
                continue
            print(f"Deleting periodic task ({existing_task.name})")
            existing_task.delete()

    for task_name, task_configuration in periodic_tasks.items():
        if 'cron' in task_configuration:
            cron_configuration = task_configuration['cron']
            crontab_schedules_queryset = CrontabSchedule.objects.filter(
                minute=cron_configuration['minute'],
                hour=cron_configuration['hour']
            )
            if not crontab_schedules_queryset:
                crontab_schedule = CrontabSchedule(minute=cron_configuration['minute'],
                                                   hour=cron_configuration['hour'])
                crontab_schedule.save()
            else:
                crontab_schedule = crontab_schedules_queryset[0]
            if PeriodicTask.objects.filter(name=task_name).count() > 0:
                print(f"Periodic task ({task_name}) exists !!!")
                continue
            periodic_task = PeriodicTask(name=task_name,
                                         task=task_configuration['task'],
                                         crontab=crontab_schedule,
                                         enabled=task_configuration["enabled"],
                                         last_run_at=task_configuration["last_run_at"],
                                         kwargs=json.dumps(task_configuration['kwargs']))
            periodic_task.save()
            print(f"Periodic task ({task_name}) created")
            continue
        else:
            if PeriodicTask.objects.filter(name=task_name).count() > 0:
                print(f"Periodic task ({task_name}) exists !!!")
                continue

            interval_schedule = get_interval_schedule(task_configuration=task_configuration)
            periodic_task = PeriodicTask(name=task_name,
                                         task=task_configuration['task'],
                                         interval=interval_schedule,
                                         enabled=task_configuration["enabled"],
                                         last_run_at=task_configuration["last_run_at"],
                                         kwargs=json.dumps(task_configuration['kwargs']))
            periodic_task.save()
            print(f"Periodic task ({task_name}) created")