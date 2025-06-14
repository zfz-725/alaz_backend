from django import forms
from django.conf import settings
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.models import Group

from .models import Team, User

# https://docs.djangoproject.com/en/3.2/topics/auth/customizing/#a-full-example


class UserCreationForm(forms.ModelForm):
    """A form for creating new users. Includes all the required
    fields, plus a repeated password."""
    password = forms.CharField(label='Password', widget=forms.PasswordInput)

    class Meta:
        model = User
        fields = ('email', 'name',)

    def save(self, commit=True):
        # Save the provided password in hashed format
        user = super().save(commit=False)
        user.set_password(self.cleaned_data["password"])
        if commit:
            user.save()
        return user


class UserAdmin(BaseUserAdmin):
    # add_form = UserCreationForm

    list_display = ('id', 'email', 'name', 'team', 'plan', 'active', 'passive_from', 'selfhosted_enterprise', 'date_created', 'date_updated')
    fieldsets = (
        (None, {'fields': ('email', 'name', 'team', 'plan', 'active', 'passive_from' )}),
    )
    list_filter = ()
    search_fields = ('id', 'email', 'name')
    ordering = ('-date_created',)
    filter_horizontal = ()

    def has_change_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_add_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True

    def has_delete_permission(self, request, obj=None):
        return False if request.user.email == "readonly@getanteon.com" else True


class TeamAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'owner', 'date_created', 'date_updated')
    list_filter = ()
    search_fields = ('id', 'name', 'owner')
    ordering = ('-date_created',)
    filter_horizontal = ()


# Now register the new UserAdmin...
admin.site.register(User, UserAdmin)
admin.site.register(Team, TeamAdmin)
# ... and, since we're not using Django's built-in permissions,
# unregister the Group model from admin.
admin.site.unregister(Group)

# if settings.ANTEON_ENV != "onprem":
#     admin.site.register(WhitelistUser, WhitelistUserAdmin)
