
from analytics.models import Telemetry
from rest_framework import serializers

class TelemetrySerializer(serializers.ModelSerializer):
    class Meta:
        model = Telemetry
        exclude = ('id', )
