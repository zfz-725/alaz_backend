import sys
from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import path
from django.urls.conf import include, re_path


def generate_urlpatterns(prefix=''):
    url_patterns = [
        path(f'{prefix}', include('core.urls')),
        path(f'{prefix}account/', include('accounts.urls')),
        path(f'{prefix}analytics/', include('analytics.urls')),
        path(f'{prefix}dist_tracing/', include('dist_tracing.urls')),
    ]
    
    if settings.ANTEON_ENV != 'onprem':
        url_patterns.append(path(f'{prefix}pricing/', include('pricing.urls')))

    return url_patterns


urlpatterns = generate_urlpatterns()

if settings.ANTEON_ENV == "production" or settings.ANTEON_ENV == "staging":
    urlpatterns.append(path('admin_huLJW6J7aaA8w2y/', admin.site.urls))
elif settings.ANTEON_ENV == "onprem":
    urlpatterns += generate_urlpatterns(prefix='api-alaz/')
    urlpatterns.append(path('api-alaz/admin_15aswASfa12asfasSA/', admin.site.urls))

else:
    urlpatterns.append(path('admin/', admin.site.urls))
    urlpatterns.append(path('api-auth/', include('rest_framework.urls', namespace='rest_framework')))
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

if settings.SILK_ENABLED:
    if settings.ANTEON_ENV == "production":
        urlpatterns += [re_path('silk-47nrKHhPHGxWHt7VXGAWGZQE/', include('silk.urls', namespace='silk'))]
    else:
        urlpatterns += [path('silk/', include('silk.urls', namespace='silk'))]