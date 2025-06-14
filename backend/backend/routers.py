from django.conf import settings

class PrimaryReplicaRouter:
    def db_for_read(self, model, **hints):
        return 'read_replica' if 'read_replica' in settings.DATABASES else 'default'

    def db_for_write(self, model, **hints):
        return 'default'

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        return db == 'default'
