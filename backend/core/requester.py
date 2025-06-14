
import datetime
import json
import logging
import re

import requests
from core.models import Setting
from core import exceptions
from django.conf import settings
from requests.auth import HTTPBasicAuth

def verbose_log(log):
    if settings.VERBOSE_LOG:
        logger.info(log)

logger = logging.getLogger(__name__)

backend_urls = {
    'selfhosted_stats': 'account/selfhosted_stats/',
    'accounts_summary': 'account/summary/',
    'selfhosted_enterprise_status': 'account/selfhosted_enterprise_status/',
    'cluster_not_fresh': 'alaz/cluster_not_fresh',
    'cluster_uids': 'alaz/cluster_uids/'
}

alaz_backend_urls = {
    'telemetry': 'analytics/telemetry/'
}

SLACK_ENDPOINTS = {
    'send_message': 'chat.postMessage',
    'get_channels': 'conversations.list',
    'set_channel': 'conversations.join',
    'base_url': 'https://slack.com/api',
}

class PrometheusRequester:

    def __init__(self) -> None:
        self.prometheus_url = settings.PROMETHEUS_URL + '/api/v1'
        anteon_setting_qs = Setting.objects.filter(name='default')
        if not anteon_setting_qs.exists():
            raise exceptions.NoDataFound({'msg': 'No default setting for the prometheus queries found'})
        anteon_settings = anteon_setting_qs.first()
        if 'metrics' not in anteon_settings.prometheus_queries:
            raise exceptions.NoDataFound({'msg': 'No metrics query found in the prometheus queries'})
        
        if 'cluster_summary' not in anteon_settings.prometheus_queries:
            raise exceptions.NoDataFound({'msg': 'No cluster summary query found in the prometheus queries'})
        self.queries = anteon_settings.prometheus_queries

    def get_node_cpu_seconds_total_range(self, start_time, end_time, step_size, instance, monitoring_id, job):
        query = 'sum by(exported_instance) (node_cpu_seconds_total{{exported_instance="{instance}", exported_job="{job}", monitoring_id="{monitoring_id}"}})'
        formatted_query = self.format_query(query, instance, monitoring_id, job)
        data = self.send_query(formatted_query, time_sec=None, range=True, start_time=start_time, end_time=end_time, step_size=step_size)
        return data

    def format_query(self, query, instance, monitoring_id, job, interval=None, extra_specifiers=None):
        if not instance:
            pattern = r',?\s?exported_instance="[^"]+",?\s?'
            query = re.sub(pattern, '', query)
        if not monitoring_id:
            pattern = r',?\s?monitoring_id="[^"]+",?\s?'
            query = re.sub(pattern, '', query)
        formatted_query = query.format(instance=instance, interval=interval, monitoring_id=monitoring_id, job=job)
        return formatted_query 

    def get_all_metrics(self, instance, interval, start_time, end_time, step_size, monitoring_id, job):
        result = {}
        for key, value in self.queries['metrics'].items():
            if 'query' not in value:
                group = key
                result[group] = {}
                for key, pair in value.items():
                    query = pair['query']
                    query = self.format_query(query, instance, monitoring_id, job, interval)
                    data = self.send_query(query, range=True, start_time=start_time, end_time=end_time, step_size=step_size)
                    result[group][key] = {'fields': pair['fields'], 'data': data}
            else:
                query = value['query']
                query = self.format_query(query, instance, monitoring_id, job, interval)
                data = self.send_query(query, monitoring_id, job, range=False, time_sec = end_time)
                result[key] = {'fields': value['fields'], 'data': data}
        return result

    def get_current_summary_metrics(self, instance, interval, monitoring_id, job):
        results = {}

        for key, value in self.queries['weekly_summary'].items():
            query = value['query']
            query = self.format_query(query, instance, monitoring_id, job, interval)
            data = self.send_query(query, range=False)
            results[key] = data

        return results

    def get_cluster_telemetry_metrics(self, monitoring_id, job):
        queries = self.queries['telemetry_metrics']
        results = {}
        for key, query_pair in queries.items():
            query = query_pair['query']
            query = self.format_query(query, None, monitoring_id, job, None)
            data = self.send_query(query, range=False)
            results[key] = data

        return results

    def send_query(self, query, start_time=None, end_time=None, step_size=None, range=False, time_sec=None):
        params = {
            "query": query
        }
        if range:
            url = self.prometheus_url + '/query_range'
            params['start'] = start_time
            params['end'] = end_time
            params['step'] = step_size
        else:
            url = self.prometheus_url + '/query'
            if time_sec:
                params['time'] = time_sec

        try:
            res = requests.get(url=url, params=params, timeout=5)
        except Exception as exc:
            exception_code = "PR-100"
            if settings.ANTEON_ENV == 'dev':
                logger.error("Can not reach to Prometheus. URL: {}. Code: {}".format(url, exception_code))
            else:
                logger.fatal("", extra={"metric": "prometheus_query_can_not_reach",
                         'error': f"Can not reach to Prometheus. URL: {url}. Code: {exception_code}"}, exc_info=True)
            
            raise exceptions.ServerError({'msg': 'Prometheus server reach error'})
        if res.status_code != 200:
            exception_code = 'PR-101'
            if settings.ANTEON_ENV == 'dev':
                logger.error("Prometheus query error. URL: {}. Reason: {}. Code: {}".format(url, str(res.reason), str(res.content), exception_code))
            else:
                logger.fatal("", extra={"metric": "prometheus_query_error",
                         'error': f"Prometheus query error. URL: {url}. Reason: {str(res.reason)}. Code: {exception_code}"})
            raise exceptions.InvalidRequestError({'msg': 'Prometheus query error'})
        return res.json()

    def get_all_cluster_summary(self, instance, monitoring_id, job):
        result = {}
        for key, value in self.queries['cluster_summary'].items():
            query = value['query']
            fields = value['fields']
            query = self.format_query(query, instance, monitoring_id, job)
            data = self.send_query(query, range=False)
            result[key] = {'fields': fields, 'data': data}
        return result

    def get_gpu_uids(self, instance, monitoring_id, job):
        query = self.queries['gpu_uids']['query']
        query = self.format_query(query, instance, monitoring_id, job)
        data = self.send_query(query, range=False)
        uids = []
        if 'data' in data and 'result' in data['data']:
            for metric in data['data']['result']:
                try:
                    uids.append(metric['metric']['uuid'])
                except KeyError:
                    pass
        return uids

    def get_gpu_metrics(self, instance, monitoring_id, job, start_time, end_time, step_size, interval, uuids):
        results = {}
        globals = {}

        now = datetime.datetime.now(datetime.UTC)
        for key,value in self.queries['gpu']['global'].items():
            query = value['query']
            fields = value['fields']
            query = self.format_query(query, instance, monitoring_id, job)
            data = self.send_query(query, range=False)
            if 'data' in data and 'result' in data['data']:
                for metric_pair in data['data']['result']:
                    metric = metric_pair['metric']
                    value = metric_pair['value']
                    try:
                        if key == 'driver_version':
                            driver_version = metric['driver_version']
                            globals['driver_version'] = driver_version

                            cuda_version = metric['cuda_version']
                            # Find the first 0, turn it into .
                            # And remove all trailing zeros
                            cuda_version = re.sub(r'0+', '', re.sub(r'0+', '.', cuda_version, 1))
                            globals['cuda_version'] = cuda_version

                    except KeyError as exc:
                        logger.error(f"Error in getting global gpu metrics. Error: {exc}", extra={"metric": "gpu_metrics_error"}, exc_info=True)

        fan_id = None
        for key, value in self.queries['gpu']['gauge'].items():
            query = value['query']
            fields = value['fields']
            query = self.format_query(query, instance, monitoring_id, job)
            data = self.send_query(query, range=False)
            if 'data' in data and 'result' in data['data']:
                for metric_pair in data['data']['result']:
                    metric = metric_pair['metric']
                    value = metric_pair['value']
                    try:
                        uuid = metric['uuid']
                        if uuid not in uuids:
                            continue
                        if uuid not in results:
                            results[uuid] = {'gauge': {}, 'time_series': {}}

                        if key == 'GPU_info':
                            # results[uuid]['gauge']['display_state'] = metric['displayState']
                            results[uuid]['gauge']['gpu_name'] = metric['gpu_name']
                            results[uuid]['gauge']['persistence_mode'] = metric['persistenceMode']
                        elif key == 'fan_speed_percentage':
                            if fan_id is None: # Set it only once
                                fan_id = metric['fan_id']
                                results[uuid]['gauge'][key] = value[1]    
                        else:
                            if key not in results[uuid]['gauge']:
                                results[uuid]['gauge'][key] = value[1]
                    except KeyError as exc:
                        logger.error(f"Error in getting gauge gpu metrics. Error: {exc}", extra={"metric": "gpu_metrics_error"}, exc_info=True)

        for key,value in self.queries['gpu']['time_series'].items():
            query = value['query']
            fields = value['fields']
            query = self.format_query(query, instance, monitoring_id, job)
            # Send range query
            data = self.send_query(query, start_time, end_time, step_size, range=True)
            if 'data' in data and 'result' in data['data']:
                for metric_pair in data['data']['result']:
                    metric = metric_pair['metric']
                    values = metric_pair['values']
                    try:
                        uuid = metric['uuid']
                        if uuid not in uuids:
                            continue
                        if uuid not in results:
                            results[uuid] = {'gauge': {}, 'time_series': {}}
                        if key == 'fan_speed_percentage':
                            timeseries_fan_id = metric['fan_id']
                            if fan_id != timeseries_fan_id:
                                continue
                        if key not in results[uuid]['time_series']:
                            results[uuid]['time_series'][key] = []
                        for value in values:
                            results[uuid]['time_series'][key].append({'timestamp': int(value[0]) * 1000, 'value': value[1]})
                    except KeyError as exc:
                        logger.error(f"Error in getting time series gpu metrics. Error: {exc}", extra={"metric": "gpu_metrics_error"}, exc_info=True)

        for uuid, value in results.items():
            if 'power_limit_watts' in value['time_series'] and 'power_usage_watts' in value['time_series']:
                for i in range(len(value['time_series']['power_limit_watts'])):
                    value['time_series']['power_usage_watts'][i]['usage'] = value['time_series']['power_usage_watts'][i]['value']
                    value['time_series']['power_usage_watts'][i]['limit'] = value['time_series']['power_limit_watts'][i]['value']
                    del value['time_series']['power_usage_watts'][i]['value']
                del value['time_series']['power_limit_watts']
            if 'bar1_utilized_percent' in value['time_series'] and 'memory_utilization_percent' in value['time_series']:
                for i in range(len(value['time_series']['bar1_utilized_percent'])):
                    value['time_series']['memory_utilization_percent'][i]['bar1'] = value['time_series']['bar1_utilized_percent'][i]['value']
                    value['time_series']['memory_utilization_percent'][i]['memory'] = value['time_series']['memory_utilization_percent'][i]['value']
                    del value['time_series']['memory_utilization_percent'][i]['value']
                del value['time_series']['bar1_utilized_percent']
            if 'power_utilization_percent' in value['time_series'] and 'GPU_utilization_percent' in value['time_series']:
                for i in range(len(value['time_series']['power_utilization_percent'])):
                    value['time_series']['GPU_utilization_percent'][i]['power'] = value['time_series']['power_utilization_percent'][i]['value']
                    value['time_series']['GPU_utilization_percent'][i]['gpu'] = value['time_series']['GPU_utilization_percent'][i]['value']
                    del value['time_series']['GPU_utilization_percent'][i]['value']
                del value['time_series']['power_utilization_percent']
                    
        return results, globals

    # def get_alert_metric_data(self, metric, interval, extra_specifiers, window_duration_seconds, instance, monitoring_id, job):
        # # TODO: Fix this
        # from alerts.utils import map_alert_metric_to_prometheus_query
        # query = map_alert_metric_to_prometheus_query(metric)
        # if query is None:
        #     return None
        # query = self.format_query(query, instance, monitoring_id, job, interval, extra_specifiers)
        # end_datetime = datetime.utcnow()
        # start_datetime = end_datetime - timedelta(seconds=window_duration_seconds)
        # data_point = settings.ALERT_WINDOW_DATA_POINT_LIMIT
        # step_size_seconds = window_duration_seconds / data_point

        # end_timestamp = int(end_datetime.timestamp())
        # start_timestamp = int(start_datetime.timestamp())

        # # print(f'Formatted query: {query}')
        # # print(start_timestamp, end_timestamp, step_size_seconds)
        # data = self.send_query(query, start_timestamp, end_timestamp, step_size=step_size_seconds, range=True)
        # return data

    def get_query_metrics(self, query, job):
        scrape_interval_seconds = settings.PROMETHEUS_SCRAPE_INTERVAL_SECONDS
        interval = str(scrape_interval_seconds * 4) + 's'
        query = self.format_query(query, None, None, job, interval)
        data = self.send_query(query, range=False)
        if 'data' not in data or 'result' not in data['data'] or len(data['data']['result']) == 0:
            return None
        metrics = []
        for metric in data['data']['result']:
            metrics.append(metric['metric'])
        return metrics

    def get_clusters_instances(self, job):
        query = 'node_cpu_seconds_total{{exported_job="{job}"}}'
        scrape_interval_seconds = settings.PROMETHEUS_SCRAPE_INTERVAL_SECONDS
        interval = str(scrape_interval_seconds * 4) + 's'
        query = self.format_query(query, None, None, job, interval)
        data = self.send_query(query, range=False)
        if 'data' not in data or 'result' not in data['data'] or len(data['data']['result']) == 0:
            return None
        metrics = []
        for metric in data['data']['result']:
            metrics.append(metric['metric'])
        return metrics


class MyBackendRequester:

    def __init__(self) -> None:
        self.url = settings.BACKEND_URL
        self.auth = HTTPBasicAuth(settings.BACKEND_USERNAME, settings.BACKEND_PASSWORD)

    def get_cluster_uids(self):
        url = f'{self.url}/{backend_urls["cluster_uids"]}'
        response = requests.get(url=url, auth=self.auth)
        if response.status_code != 200:
            raise exceptions.InvalidRequestError({'msg': 'Cluster uids query error'})

        return response.json()

    def get_selfhosted_stats(self):
        url = f'{self.url}/{backend_urls["selfhosted_stats"]}'
        response = requests.get(url=url, auth=self.auth)
        if response.status_code != 200:
            raise exceptions.InvalidRequestError({'msg': 'Selfhosted stats query error'})

        return response.json()
                                          
    def get_accounts_summary(self):
        url = f'{self.url}/{backend_urls["accounts_summary"]}'
        response = requests.get(url=url, auth=self.auth)
        if response.status_code != 200:
            raise exceptions.InvalidRequestError({'msg': 'Accounts summary query error'})
        
        return response.json()

    def get_selfhosted_enterprise_status(self):
        url = f'{self.url}/{backend_urls["selfhosted_enterprise_status"]}'
        response = requests.get(url=url, auth=self.auth)
        if response.status_code != 200:
            raise exceptions.InvalidRequestError({'msg': 'Selfhosted enterprise status query error'})
        
        return response.json()

    def set_cluster_as_not_fresh(self, monitoring_id):
        url = f'{self.url}/{backend_urls["cluster_not_fresh"]}/?monitoring_id={monitoring_id}'
        response = requests.put(url=url, auth=self.auth)
        if response.status_code != 200:
            raise exceptions.InvalidRequestError({'msg': 'Cluster set as not fresh error'})


class CloudAlazBackendRequester:

    def __init__(self) -> None:
        self.url = settings.CLOUD_ALAZ_BACKEND_URL
        # self.auth = HTTPBasicAuth(settings.CLOUD_ALAZ_BACKEND_USERNAME, settings.CLOUD_ALAZ_BACKEND_PASSWORD)

    def send_telemetry_data(self, data):
        url = f'{self.url}/{alaz_backend_urls["telemetry"]}'
        response = requests.post(url=url, json=data)
        if response.status_code != 200:
            logger.error(f'Telemetry data send error. Response: {response.text}')
            raise exceptions.InvalidRequestError({'msg': 'Telemetry data send error.'})
        return response.json()

class GithubRequester:
    help = 'Update version from GitHub'

    def get_alaz_version(self):
        response = requests.get('https://api.github.com/repos/ddosify/alaz/releases/latest')
        if response.status_code != 200:
            raise exceptions.InvalidRequestError({'msg': 'Failed to fetch version from GitHub'})
        
        return response.json()


class SlackRequester:

    def __init__(self):
        self.base_url = SLACK_ENDPOINTS['base_url']
        self.headers = {
            'Content-Type': 'application/json'
        }

    def set_token(self, token):
        self.headers['Authorization'] = f'Bearer {token}'

    def send_message(self, token, channel, message, blocks):
        url = f'{self.base_url}/{SLACK_ENDPOINTS["send_message"]}'
        self.set_token(token)
        payload = {
            'channel': channel,
        }

        extended_headers = self.headers.copy()
        extended_headers['Content-Type'] = 'application/json; charset=utf-8'

        if message:
            payload['text'] = message
        elif blocks:
            payload['blocks'] = json.dumps(blocks)
        # send post
        try:
            response = requests.post(url=url, headers=extended_headers, data=json.dumps(payload))
        except Exception as exc:
            exception_code = 'S-100'
            exception_msg = f"Can not reach to Slack. Make sure that you have an internet connection. Code: {exception_code}. Exception: {exc}"
            logger.fatal("", extra={"metric": "Slack_send_message_can_not_reach",
                         "error": exception_msg}, exc_info=True)
            raise exceptions.ServerError({"msg": exception_msg})
        if response.status_code != 200:
            exception_code = 'S-101'
            exception_msg = f"Slack error on send message. Reason: {str(response.reason)}. Code: {exception_code}"
            logger.fatal("", extra={"metric": "Slack_send_message_error1",
                         "error": exception_msg})
            raise exceptions.ServerError({"msg": exception_msg})
        response = response.json()
        if 'ok' not in response or response['ok'] != True:
            exception_code = 'S-105'
            error = 'Unknown'
            if 'error' in response:
                error = response['error']
            if error == 'invalid_auth':
                error = 'Invalid token'
            exception_msg = f"Not ok Slack response. Error: {error}"
            logger.fatal("", extra={"metric": "Slack_send_message_error2",
                            "error": exception_msg})
            raise exceptions.ServerError({"msg": exception_msg})
        verbose_log(f'Sent the message')

    def get_channels(self, access_token, cursor=None):
        url = f'{self.base_url}/{SLACK_ENDPOINTS["get_channels"]}?limit=100'
        self.set_token(access_token)

        if cursor:
            url += f'&cursor={cursor}'

        try:
            response = requests.get(url=url, headers=self.headers)
        except Exception as exc:
            exception_code = 'S-106'
            exception_msg = f"Can not reach to Slack. Make sure that you have an internet connection. Code: {exception_code}. Exception: {exc}"
            logger.fatal("", extra={"metric": "Slack_get_channels_can_not_reach",
                            "error": exception_msg}, exc_info=True)
            raise exceptions.ServerError({"msg": exception_msg})
        if response.status_code != 200:
            exception_code = 'S-107'
            exception_msg = f"Slack error on get channels. Reason: {str(response.reason)}. Code: {exception_code}"
            logger.fatal("", extra={"metric": "Slack_get_channels_error1",
                            "error": exception_msg})
            raise exceptions.ServerError({"msg": exception_msg})
        response = response.json()
        if 'ok' not in response or response['ok'] != True:
            exception_code = 'S-108'
            error = 'Unknown'
            if 'error' in response:
                error = response['error']
            if error == 'invalid_auth':
                error = 'Invalid token'
            exception_msg = f"Not ok Slack response. Error: {error}"
            logger.fatal("", extra={"metric": "Slack_get_channels_error2",
                            "error": exception_msg})
            raise exceptions.ServerError({"msg": exception_msg})
        return response
        
    def set_channel(self, token, channel):
        url = f'{self.base_url}/{SLACK_ENDPOINTS["set_channel"]}'

        self.set_token(token)
        data = {
            'token': token,
            'channel': channel
        }

        try:
            response = requests.post(url=url, data=data)
        except Exception as exc:
            exception_code = 'S-109'
            exception_msg = f"Can not reach to Slack. Make sure that you have an internet connection. Code: {exception_code}. Exception: {exc}"
            logger.fatal("", extra={"metric": "Slack_set_channel_can_not_reach",
                            "error": exception_msg}, exc_info=True)
            raise exceptions.ServerError({"msg": exception_msg})
        if response.status_code != 200:
            exception_code = 'S-110'
            exception_msg = f"Slack error on set channel. Reason: {str(response.reason)}. Code: {exception_code}"
            logger.fatal("", extra={"metric": "Slack_set_channel_error1",
                            "error": exception_msg})
            raise exceptions.ServerError({"msg": exception_msg})
        response = response.json()
        if 'ok' not in response or response['ok'] != True:
            exception_code = 'S-111'
            error = 'Unknown'
            if 'error' in response:
                error = response['error']
            if error == 'invalid_auth':
                error = 'Invalid token'
            exception_msg = f"Not ok Slack response. Error: {error}"
            logger.fatal("", extra={"metric": "Slack_set_channel_error2",
                            "error": exception_msg})
            raise exceptions.ServerError({"msg": exception_msg})
        return response

