
import sys
import os
import uuid
import django
sys.path.append('/workspace/backend')
sys.path.append('/workspaces/alaz-backend/backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
django.setup()
from core.models import Setting
from django.conf import settings

prometheus_queries = {
    'metrics': {
        'cpu_usage': {
            'busy_other_percent': {'query': '(sum by(exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mode!="idle", mode!="user", mode!="system", mode!="iowait", mode!="irq", mode!="softirq"}}[{interval}])) / on(exported_instance) group_left sum by (exported_instance)((irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[{interval}])))) * 100', 'fields': []},

            'idle_percent': {'query': '(sum by(exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mode=\"idle\"}}[{interval}])) / on(exported_instance) group_left sum by (exported_instance)((irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[{interval}])))) * 100', 'fields': []},

            'busy_iowait_percent': {'query': '(sum by(exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mode=\"iowait\"}}[{interval}])) / on(exported_instance) group_left sum by (exported_instance)((irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[{interval}])))) * 100', 'fields': []},

            'busy_irqs_percent': {'query': '(sum by(exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mode=~\".*irq\"}}[{interval}])) / on(exported_instance) group_left sum by (exported_instance)((irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[{interval}])))) * 100', 'fields': []},

            'busy_system_percent': {'query': '(sum by(exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mode=\"system\"}}[{interval}])) / on(exported_instance) group_left sum by (exported_instance)((irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[{interval}])))) * 100', 'fields': []},

            'busy_user_percent': {'query': '(sum by(exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mode=\"user\"}}[{interval}])) / on(exported_instance) group_left sum by (exported_instance)((irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[{interval}])))) * 100', 'fields': []},
        },
        'disk_usage': {
            'used_percent': {'query': '100 - ((node_filesystem_avail_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", device!~"rootfs", mountpoint="/"}} * 100) / node_filesystem_size_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", device!~"rootfs", mountpoint="/"}})', 'fields': []}
        },
        'memory_usage': {
            'used_ram_bytes': {'query': '(node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} - node_memory_MemFree_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} - (node_memory_Cached_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} + node_memory_Buffers_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} + node_memory_SReclaimable_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}))', 'fields': []},

            'free_ram_bytes': {'query': 'node_memory_MemFree_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},

            'total_ram_bytes': {'query': 'node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},

            'cache_and_buffer_ram_bytes': {'query': '(node_memory_Cached_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} + node_memory_Buffers_bytes{{exported_instance="{instance}", exported_job="{job}"}} + node_memory_SReclaimable_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}})', 'fields': []},

            'used_swap_bytes': {'query': '(node_memory_SwapTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} - node_memory_SwapFree_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}})', 'fields': []}
        },
        'memory_usage_percent': {
            'used_ram_percentage': {'query': '100 - ((node_memory_MemAvailable_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100) / node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}})', 'fields': []},

            'free_ram_percentage': {'query': 'node_memory_MemFree_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100', 'fields': []},

            'total_ram_percentage': {'query': 'node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100', 'fields': []},

            'cache_and_buffer_ram_percentage': {'query': '(node_memory_Cached_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} + node_memory_Buffers_bytes{{exported_instance="{instance}", exported_job="{job}"}} + node_memory_SReclaimable_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) / node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100', 'fields': []},

            'used_swap_percentage': {'query': '(node_memory_SwapTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} - node_memory_SwapFree_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) / node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100', 'fields': []}
        },
        'network_usage': {
            'received_bits': {'query': 'irate(node_network_receive_bytes_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[{interval}]) * 8', 'fields': ['device']},
            'transmitted_bits': {'query': 'irate(node_network_transmit_bytes_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[{interval}]) * 8', 'fields': ['device']}
        },
        'sys_5m_load_percent': {'query': 'avg(node_load5{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) / count(count(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) by (cpu)) * 100', 'fields': []},

        'sys_15m_load_percent': {'query': 'avg(node_load15{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) / count(count(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) by (cpu)) * 100', 'fields': []},

        'cpu_usage_percent': {'query': '(sum by(exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mode!="idle"}}[{interval}])) / on(exported_instance) group_left sum by (exported_instance)((irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[{interval}])))) * 100', 'fields': []},

        'ram_used_percent': {'query': '100 - ((node_memory_MemAvailable_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100) / node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}})', 'fields': []},

        # 'available_ram_percent': {'query': '100 - ((node_memory_MemAvailable_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100) / node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}})', 'fields': []},

        'root_fs_used_percent': {'query': '100 - ((node_filesystem_avail_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mountpoint="/", fstype!="rootfs"}} * 100) / node_filesystem_size_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mountpoint="/", fstype!="rootfs"}})', 'fields': []},

        'swap_used_percent': {'query': '((node_memory_SwapTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} - node_memory_SwapFree_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) / (node_memory_SwapTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} )) * 100', 'fields': []},

        'total_ram_bytes': {'query': 'node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},

        'total_root_fs_bytes': {'query': 'node_filesystem_size_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mountpoint="/", fstype!="rootfs"}}', 'fields': []},

        'total_swap_bytes': {'query': 'node_memory_SwapTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},

        'uptime_seconds': {'query': 'node_time_seconds{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} - node_boot_time_seconds{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},

        'cpu_count': {'query': 'count(count(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) by (cpu))', 'fields': []},
    },
    'cluster_summary' : {
        'utilized_cpu_count': {'query': '(sum(irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mode!="idle"}}[1m])) / sum(irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[1m]))) * count(count(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) by (cpu))', 'fields': []},

        'total_cpu_count': {'query': 'count(count(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) by (cpu))', 'fields': []},

        'utilized_memory_bytes': {'query': 'node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} - node_memory_MemAvailable_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},

        'total_memory_bytes': {'query': 'node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},

        'utilized_filesystem_bytes': {'query': 'node_filesystem_size_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mountpoint="/"}} - node_filesystem_avail_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mountpoint="/"}}', 'fields': []},

        'total_filesystem_bytes': {'query': 'node_filesystem_size_bytes{{mountpoint="/",fstype!="rootfs", exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []}
    },
    'weekly_summary': {
        'cpu_usage_avg': {'query': 'avg_over_time(((sum by (exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mode!="idle"}}[5m]))) / on(exported_instance) group_left sum by (exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[5m]) != 0))[{interval}:5m]) * 100', 'fields': []},

        'cpu_usage_min': {'query': 'min_over_time(((sum by (exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mode!="idle"}}[5m]))) / on(exported_instance) group_left sum by (exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[5m])))[{interval}:5m]) * 100', 'fields': []},

        'cpu_usage_max': {'query': 'max_over_time(((sum by (exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mode!="idle"}}[5m]))) / on(exported_instance) group_left sum by (exported_instance) (irate(node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}[5m])))[{interval}:5m]) * 100', 'fields': []},

        # RAM usage
        'ram_usage_avg': {'query': '100 - avg_over_time(((node_memory_MemAvailable_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) * 100)[{interval}:5m])', 'fields': []},

        'ram_usage_min': {'query': '100 - max_over_time(((node_memory_MemAvailable_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) * 100)[{interval}:5m])', 'fields': []},

        'ram_usage_max': {'query': '100 - min_over_time(((node_memory_MemAvailable_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / node_memory_MemTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) * 100)[{interval}:5m])', 'fields': []},
        
        # Root FS usage
        'root_fs_usage_avg': {'query': '100 - avg_over_time(((node_filesystem_avail_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mountpoint="/", fstype!="rootfs"}} / node_filesystem_size_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mountpoint="/", fstype!="rootfs"}}) * 100)[{interval}:5m])', 'fields': []},

        'root_fs_usage_min': {'query': '100 - max_over_time(((node_filesystem_avail_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mountpoint="/", fstype!="rootfs"}} / node_filesystem_size_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mountpoint="/", fstype!="rootfs"}}) * 100)[{interval}:5m])', 'fields': []},

        'root_fs_usage_max': {'query': '100 - min_over_time(((node_filesystem_avail_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mountpoint="/", fstype!="rootfs"}} / node_filesystem_size_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", mountpoint="/", fstype!="rootfs"}}) * 100)[{interval}:5m])', 'fields': []},

        # SWAP usage
        'swap_usage_avg': {'query': 'avg_over_time(((node_memory_SwapTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} - node_memory_SwapFree_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) / node_memory_SwapTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100)[{interval}:5m])', 'fields': []},

        'swap_usage_min': {'query': 'min_over_time(((node_memory_SwapTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} - node_memory_SwapFree_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) / node_memory_SwapTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100)[{interval}:5m])', 'fields': []},

        'swap_usage_max': {'query': 'max_over_time(((node_memory_SwapTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} - node_memory_SwapFree_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}) / node_memory_SwapTotal_bytes{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100)[{interval}:5m])', 'fields': []},
    },
    'telemetry_metrics': {
        'cpu_count': {'query': 'count(count by (exported_instance, cpu) (node_cpu_seconds_total{{exported_job="{job}", monitoring_id="{monitoring_id}"}}))', 'fields': []},
        
        'total_ram_bytes': {'query': 'sum(node_memory_MemTotal_bytes{{exported_job="{job}", monitoring_id="{monitoring_id}"}})', 'fields': []},

        'total_root_filesystem_bytes': {'query': 'sum(node_filesystem_size_bytes{{mountpoint="/", fstype!="rootfs", exported_job="{job}", monitoring_id="{monitoring_id}"}})', 'fields': []},
    },

    'gpu_uids': {
        'query': 'alaz_nvml_bar1{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []
        },

    'gpu': {

        'global': {

            'driver_version': {'query': 'alaz_nvml_gpu_driver{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []}
            
        },
        
        'gauge': {
            
            'free_memory_mib': {'query': 'alaz_nvml_free_mem{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},
            
            'used_memory_mib': {'query': 'alaz_nvml_used_mem{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},
            
            'total_memory_mib': {'query': 'alaz_nvml_total_mem{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},
            
            'memory_utilization_percent': {'query': 'alaz_nvml_used_mem{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / alaz_nvml_total_mem{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100', 'fields': []},

            'GPU_info': {'query': 'alaz_nvml_gpu_info{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},

            'temperature_celcius': {'query': 'alaz_nvml_temp_celcius{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},
            
            'power_usage_watts': {'query': 'alaz_nvml_power_usage{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / 1000', 'fields': []},

            'power_limit_watts': {'query': 'alaz_nvml_power_limit{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / 1000', 'fields': []},

            'power_utilization_percent': {'query': 'alaz_nvml_power_usage{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / alaz_nvml_power_limit{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100', 'fields': []},

            'GPU_utilization_percent': {'query': 'alaz_nvml_gpu_utz{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},

            'core_clock_mhz': {'query': 'alaz_nvml_core_clock_mhz{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", type="core"}}', 'fields': []},
            
            'memory_clock_mhz': {'query': 'alaz_nvml_mem_clock_mhz{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", type="memory"}}', 'fields': []},

            'fan_speed_percentage': {'query': 'alaz_nvml_fan_speed{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},
            
        },
        
        'time_series': {
            
            'bar1_utilized_percent': {'query': 'alaz_nvml_bar1_used{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / alaz_nvml_bar1{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100', 'fields': []},
        
            'core_clock_mhz': {'query': 'alaz_nvml_core_clock_mhz{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", type="core"}}', 'fields': []},
        
            'memory_utilization_percent': {'query': 'alaz_nvml_used_mem{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / alaz_nvml_total_mem{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100', 'fields': []},
        
            'GPU_utilization_percent': {'query': 'alaz_nvml_gpu_utz{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},
        
            'memory_clock_mhz': {'query': 'alaz_nvml_mem_clock_mhz{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}", type="memory"}}', 'fields': []},
        
            'temperature_celsius': {'query': 'alaz_nvml_temp_celcius{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},
        
            'power_limit_watts': {'query': 'alaz_nvml_power_limit{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / 1000', 'fields': []},
            
            'power_usage_watts': {'query': 'alaz_nvml_power_usage{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / 1000', 'fields': []},

            'power_utilization_percent': {'query': 'alaz_nvml_power_usage{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} / alaz_nvml_power_limit{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}} * 100', 'fields': []},

            'fan_speed_percentage': {'query': 'alaz_nvml_fan_speed{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}}', 'fields': []},
        }
    }
}

if settings.ANTEON_ENV == 'onprem':
    onprem_key = uuid.uuid4()
else:
    onprem_key = None

alert_messages = {
    'slack': {
        'bloat': {
            'redis': [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":rotating_light: Alaz-Backend : {env} | Redis memory usage *{usage} MB* >= *{limit} MB*"
                    }
                },
            ],
            'postgres': [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":rotating_light: Alaz-Backend : {env} | Postgres size *{usage} MB* >= *{limit} MB*"
                    }
                },
            ],
            'rabbitmq': [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":rotating_light: Alaz-Backend : {env} | RabbitMQ queue *{queue_name}* messages *{usage}* >= *{limit}*"
                    }
                },
            ],
            'clickhouse': [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":rotating_light: Alaz-Backend : {env} | Clickhouse size *{usage} MB* >= *{limit} MB*"
                    }
                },
            ],
        },
    },
    
}

used_metrics = ['node_cpu_seconds_total', 'node_filesystem_avail_bytes', 'node_filesystem_size_bytes', 'node_memory_MemTotal_bytes', 'node_memory_MemFree_bytes', 'node_memory_Cached_bytes', 'node_memory_Buffers_bytes', 'node_memory_SReclaimable_bytes', 'node_memory_SwapTotal_bytes', 'node_memory_SwapFree_bytes', 'node_memory_MemAvailable_bytes', 'node_network_receive_bytes_total', 'node_network_transmit_bytes_total', 'node_time_seconds', 'node_boot_time_seconds', 'alaz_nvml_bar1', 'alaz_nvml_gpu_driver', 'alaz_nvml_free_mem', 'alaz_nvml_used_mem', 'alaz_nvml_total_mem', 'alaz_nvml_gpu_info', 'alaz_nvml_temp_celcius', 'alaz_nvml_power_usage', 'alaz_nvml_power_limit', 'alaz_nvml_gpu_utz', 'alaz_nvml_core_clock_mhz', 'alaz_nvml_mem_clock_mhz', 'alaz_nvml_fan_speed']

if Setting.objects.filter(name="default").exists():
    Setting.objects.using('default').filter(name="default").update(prometheus_queries=prometheus_queries, used_metrics=used_metrics, alert_messages=alert_messages)
else:
    Setting.objects.create(name="default", prometheus_queries=prometheus_queries, onprem_key=onprem_key, used_metrics=used_metrics, alert_messages=alert_messages)
