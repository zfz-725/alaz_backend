import base64
from django.conf import settings
from rest_framework import permissions, status

from core.exceptions import PermissionDenied

class BasicAuth(permissions.BasePermission):
    def has_permission(self, request, view):
        if 'HTTP_AUTHORIZATION' in request.META:
            auth = request.META['HTTP_AUTHORIZATION'].split()
            if len(auth) == 2 and auth[0].lower() == 'basic':
                try:
                    username, password = base64.b64decode(auth[1]).decode('utf-8').split(':')
                    if username == settings.ALAZ_BACKEND_USERNAME and password == settings.ALAZ_BACKEND_PASSWORD:
                        return True
                except ValueError:
                    # Handle the case where the base64 encoded string does not contain a colon
                    pass
        # If the request does not meet the condition, raise PermissionDenied
        raise PermissionDenied(detail="Invalid credentials.")