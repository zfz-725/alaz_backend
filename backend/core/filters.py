from django_filters.rest_framework import DjangoFilterBackend

class CustomFilterBackend(DjangoFilterBackend):
    def filter_queryset(self, request, queryset, view):
        filterset = self.get_filterset(request, queryset, view)
        if filterset is None:
            return queryset

        if not filterset.is_valid() and self.raise_exception:
            return []   # overriden: raise utils.translate_validation(filterset.errors)
        return filterset.qs