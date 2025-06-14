import datetime
import uuid

from django.contrib.auth.models import AbstractBaseUser, BaseUserManager
from django.db import models
from django.utils import timezone


class UserManager(BaseUserManager):
    def create_user(self, id, email, password=None, name=None, auth_provider=None, is_email_confirmed=False):
        if not email:
            raise ValueError('Users must have an email address')

        user = self.model(
            email=self.normalize_email(email),
            name=name,
        )
        user.id = id
        user.plan = {}
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_superuser(self, id, email, password=None, name=None):
        user = self.create_user(
            id,
            email,
            password=password,
            name=name,
        )
        user.is_admin = True
        user.save(using=self._db)
        return user


class User(AbstractBaseUser):
    class AuthProviders(models.TextChoices):
        EMAIL = 'email'
        GOOGLE = 'google'
        GITHUB = 'github'

    id = models.UUIDField(primary_key=True, editable=False, serialize=False)
    email = models.EmailField(
        verbose_name='email address',
        max_length=255,
        unique=True,
    )
    name = models.CharField(max_length=250, null=False, blank=False)
    team = models.ForeignKey('Team', on_delete=models.SET_NULL, null=True, blank=True)
    plan = models.JSONField()
    is_admin = models.BooleanField(default=False)
    selfhosted_enterprise = models.BooleanField(default=False)  # Only used for onprem
    active = models.BooleanField(default=True)

    date_created = models.DateTimeField(default=timezone.now)
    date_updated = models.DateTimeField(auto_now=True)
    passive_from = models.DateTimeField(null=True, blank=True)

    objects = UserManager()

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = []

    def __str__(self):
        return f"{self.email}"

    def has_perm(self, perm, obj=None):
        "Does the user have a specific permission?"
        # Simplest possible answer: Yes, always
        return True

    def has_module_perms(self, app_label):
        "Does the user have permissions to view the app `app_label`?"
        # Simplest possible answer: Yes, always
        return True

    @property
    def is_staff(self):
        "Is the user a member of staff?"
        # Simplest possible answer: All admins are staff
        return self.is_admin
    
    def save(self, *args, **kwargs):
        if not self.active:
            self.passive_from = datetime.datetime.now(datetime.UTC)
        else:
            self.passive_from = None
        super(User, self).save(*args, **kwargs)


class Team(models.Model):
    
        id = models.UUIDField(primary_key=True, editable=False, serialize=False)
        name = models.CharField(max_length=250, null=False, blank=False)
        owner = models.ForeignKey(User, on_delete=models.CASCADE, related_name='team_owner')

        date_created = models.DateTimeField(auto_now_add=True)
        date_updated = models.DateTimeField(auto_now=True)
    
        def __str__(self):
            return f"{self.name}"
