
import json
from rest_framework import viewsets
from accounts.utils import sync_users_and_teams
from core import throttling

from backend.permissions import BasicAuth
from .models import Team, User
from .serializers import RootUserSerializer, TeamSerializer, UserSerializer
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from rest_framework import status

class UserViewSet(viewsets.ModelViewSet):
    permission_classes = [BasicAuth]
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    queryset = User.objects.all()
    serializer_class = UserSerializer

class RootUserViewSet(GenericAPIView):
    permission_classes = [BasicAuth]
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]

    def post(self, request):
        serializer = RootUserSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class TeamViewSet(viewsets.ModelViewSet):
    permission_classes = [BasicAuth]
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]
    queryset = Team.objects.all()
    serializer_class = TeamSerializer

class SyncUserViewSet(GenericAPIView):
    permission_classes = [BasicAuth]
    throttle_classes = [throttling.ConcurrencyThrottleGetApiKey]

    def post(self, request):
        sync_users_and_teams(request.data)
        return Response(status=status.HTTP_200_OK)