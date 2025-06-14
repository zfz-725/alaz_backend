from django.conf import settings
from django.contrib.auth.tokens import PasswordResetTokenGenerator
from django.utils.encoding import force_bytes, force_str
from django.utils.http import urlsafe_base64_decode, urlsafe_base64_encode
from django.utils.timezone import now
from accounts.serializers import TeamSerializer, UserSerializer
from accounts.models import Team, User
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)



def verbose_log(log):
    if settings.CELERY_VERBOSE_LOG:
        logger.info(log)

class PasswordResetTokenGenerator(PasswordResetTokenGenerator):
    def _make_hash_value(self, user, timestamp):
        login_timestamp = '' if user.last_login is None else user.last_login.replace(microsecond=0, tzinfo=None)
        return str(user.id) + str(user.password) + str(timestamp) + str(user.is_email_confirmed) + str(user.email) + str(login_timestamp)


default_token_generator = PasswordResetTokenGenerator()


def encode_uid(pk):
    return force_str(urlsafe_base64_encode(force_bytes(pk)))


def decode_uid(pk):
    return force_str(urlsafe_base64_decode(pk))


def generate_uid_token_for_user(user):
    uid = encode_uid(user.pk)
    token = default_token_generator.make_token(user)
    return uid, token


def check_uid_token_for_user(uid, token):
    try:
        uid = decode_uid(uid)
        user = User.objects.get(pk=uid)
    except (User.DoesNotExist, ValueError, TypeError, OverflowError):
        raise exceptions.ValidationError({"msg": "Invalid token for given user"})

    token_valid = default_token_generator.check_token(user=user, token=token)
    if not token_valid:
        raise exceptions.ValidationError({"msg": "Invalid token for given user"})

    return user


def get_error_detail(errors):
    key = list(errors.keys())[0]
    err_detail = "Invalid input"
    for err in errors[key]:
        if err.code is not None:
            err_detail = str(err)
            if err.code == 'blank':
                err_detail = f"{key}: {err_detail}"
            break
    return err_detail


def prepare_user_login_response(user):
    onboard = 0 if user.last_login else 1
    user.last_login = now()
    user.save()
    return {
        "tokens": user.tokens(),
        "email": user.email,
        "name": user.name,
        "auth_provider": user.auth_provider,
        "is_email_confirmed": user.is_email_confirmed,
        "id": user.id,
        "onboard": onboard
    }


def find_team_owner_if_exists(user):
    if user.team:
        return user.team.owner
    return user

def sync_users_and_teams(summary):
    start_time = now()
    fetched_users = summary.get('users', [])
    fetched_teams = summary.get('teams', [])
    users = User.objects.using('default').all()
    users = {str(user.id): user for user in users}
    teams = Team.objects.using('default').all()
    teams = {str(team.id): team for team in teams}
    verbose_log(f'Cached accounts from db in {now() - start_time}')

    if not fetched_users:
        logger.error('No user subscriptions found')
    if not fetched_teams:
        logger.error('No teams found')

    start_time = now()
    # Delete the nonexistent users and teams
    for user_id, user in users.items():
        if user.email == settings.ROOT_EMAIL:
            continue
        if str(user_id) not in fetched_users:
            user.delete()

    for team_id, team in teams.items():
        if str(team_id) not in fetched_teams:
            team.delete()
    verbose_log(f'Deleted nonexistent users and teams in {now() - start_time}')

    start_time = now()
    users_to_teams = {}
    for user_id, user_data in fetched_users.items():
        if user_data['email'] == settings.ROOT_EMAIL:
            if settings.ANTEON_ENV == 'onprem':
                root_user_qs = User.objects.filter(email=settings.ROOT_EMAIL)
                user_data['is_admin'] = True
                user_data['password'] = settings.ROOT_PASSWORD
                if not root_user_qs.exists():
                # 1- Root user does not exist -> Create
                    serializer = UserSerializer(data=user_data)
                    try:
                        serializer.is_valid(raise_exception=True)
                        serializer.save()
                    except Exception as exc:
                        logger.error(f'Could not create root user with id {user_id}. Error: {exc}')
                        continue
                else:
                    # 2- Root user exists -> Update
                    root_user = root_user_qs.first()
                    
                    serializer = UserSerializer(root_user, data=user_data)
                    try:
                        serializer.is_valid(raise_exception=True)
                        serializer.save()
                    except Exception as exc:
                        logger.error(f'Could not update root user with id {user_id}. Error: {exc}')
            continue

        if user_id not in users:
            # verbose_log(f'User with id {user_id} does not exist. Creating it now')
            users_to_teams[user_id] = user_data['team_id']
            user_data.pop('team_id', None)
            serializer = UserSerializer(data=user_data)
            try: 
                serializer.is_valid(raise_exception=True)
                serializer.save()
                # verbose_log(f'Created user with id {user_id}')
            except Exception as exc:
                logger.error(f'Could not create user with id {user_id}. Error: {exc}')
                continue
        else:
            user = users[user_id]
            serializer = UserSerializer(user, data=user_data)
            try:
                serializer.is_valid(raise_exception=True)
                serializer.save()
            except Exception as exc:
                logger.error(f'Could not update user with id {user_id}. Error: {exc}')
                continue
    verbose_log(f'Created/updated users in {now() - start_time}')
    
    start_time = now()
    for team_id, team_data in fetched_teams.items():
        if team_id not in teams:
            # verbose_log(f'Team with id {team_id} does not exist. Creating it now')
            team_data['id'] = team_id
            serializer = TeamSerializer(data=team_data)
            try:
                serializer.is_valid(raise_exception=True)
                serializer.save()
                # verbose_log(f'Created team with id {team_id}')
            except Exception as exc:
                logger.error(f'Could not create team with id {team_id}. Error: {exc}')
                continue
        else:
            team = teams[team_id]
            serializer = TeamSerializer(team, data=team_data)
            try:
                serializer.is_valid(raise_exception=True)
                serializer.save()
                # verbose_log(f'Updated team with id {team_id}')
            except Exception as exc:
                logger.error(f'Could not update team with id {team_id}. Error: {exc}')
                continue
    verbose_log(f'Created/updated teams in {now() - start_time}')

    # Update the teams of the fetched users
    start_time = now()
    users = User.objects.all()
    users = {str(user.id): user for user in users}
    teams = Team.objects.all()
    teams = {str(team.id): team for team in teams}
    verbose_log(f'Cached the updated accounts from db in {now() - start_time}')

    start_time = now()
    for user_id, user_data in fetched_users.items():
        if user_data['email'] == settings.ROOT_EMAIL:
            continue
        if user_id not in users:
            logger.error(f'User with id {user_id} does not exist. Skipping it')
            continue
        user = users[user_id]
        team_id = users_to_teams.get(user_id)
        if team_id is not None:
            if team_id not in teams:
                logger.error(f'Team with id {team_id} does not exist. Skipping it')
                continue
            team = teams[team_id]
            user.team = team
        else:
            user.team = None
        user.save()
        # verbose_log(f'Updated team of user with id {user_id} to {team_id}')
    verbose_log(f'Updated teams of users in {now() - start_time}')
