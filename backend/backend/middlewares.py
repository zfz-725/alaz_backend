import logging
import os
from django.conf import settings
import psutil

logger = logging.getLogger('django.request')

class LogBadRequestMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)

        if response.status_code == 400:
            try:
                body = request.body.decode()[0:50]
            except:
                body = None
            try:
                path = request.get_full_path()
            except:
                path = None
            try:
                content = response.content.decode()
            except:
                content = None

            logger.warning(
                f'Bad Request from Middleware: {path} - {content} - Body: {body}',
                extra={
                    'status_code': 400,
                    'request': request
                }
            )

        return response


class LogUrlMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        
    def __call__(self, request):
        response = self.get_response(request)
        logger.info(
            f'URL: {request.get_full_path()}',
            extra={
                'status_code': response.status_code,
                'request': request
            }
        )
        return response

class MemoryUsageMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def process_request(self, request):
        request._mem = psutil.Process(os.getpid()).memory_info()

    def process_response(self, request, response):
        mem = psutil.Process(os.getpid()).memory_info()
        diff = mem.rss - request._mem.rss
        logger.info(f'Memory usage: {diff} bytes - {request.path}')
        # if diff > THRESHOLD:
            # print >> sys.stderr, 'MEMORY USAGE %r' % ((diff, request.path),)

    def __call__(self, request):
        self.process_request(request)
        response = self.get_response(request)
        self.process_response(request, response)
        return response

    