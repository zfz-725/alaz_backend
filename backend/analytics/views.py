from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from django.conf import settings
from rest_framework import status
from rest_framework.exceptions import Throttled
from analytics.serializers import TelemetrySerializer
from core import throttling

# Create your views here.

class TelemetryView(GenericAPIView):
    throttle_classes = [throttling.ConcurrencyTelemetryThrottle]
    # TODO: Add authentication here

    def throttled(self, request, wait):
        raise Throttled(detail={"msg": "Request was throttled"})

    def post(self, request):
        print(f'Telemetry data came: {request.data}')
        payload = request.data
        auth_key = payload.get('auth_key', None)
        if not auth_key:
            return Response({'msg': 'auth_key not provided'}, status=status.HTTP_400_BAD_REQUEST)
        if auth_key != settings.TELEMETRY_AUTH_KEY:
            return Response({'msg': 'auth_key invalid'}, status=status.HTTP_400_BAD_REQUEST)

        payload_data = payload.get('data', None)
        if not payload_data:
            return Response({'msg': 'data not provided'}, status=status.HTTP_400_BAD_REQUEST)
        errors = []
        for num, cluster_data in enumerate(payload_data):
            try:
                serializer = TelemetrySerializer(data=cluster_data)
                serializer.is_valid(raise_exception=True)
                serializer.save()
            except Exception as e:
                errors.append({'id': num, 'errors': [str(e)]})
        if errors:
            return Response({'msg': errors}, status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response({'msg': 'ok'}, status=status.HTTP_200_OK)
    