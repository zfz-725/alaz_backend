from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import RootUserViewSet, TeamViewSet, UserViewSet, SyncUserViewSet

router = DefaultRouter()
router.register(r'user', UserViewSet)
router.register(r'team', TeamViewSet)

urlpatterns = [
    path('', include(router.urls)),
    path('root_user/', RootUserViewSet.as_view(), name='root_user'),
    path('sync_users_and_teams/', SyncUserViewSet.as_view(), name='root_user'),
]