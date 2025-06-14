import logging
from rest_framework import serializers
from .models import Team, User
import secrets

logger = logging.getLogger(__name__)

class UserSerializer(serializers.ModelSerializer):
    # Since we use ModelSerializer, the field "id" is automatically not allowed to be written to.
    id = serializers.UUIDField(read_only=False)

    class Meta:
        model = User
        fields = '__all__'
        extra_kwargs = {'password': {'write_only': True, 'required': False}}

    def create(self, validated_data):
        # Check if user already exists
        user_qs = User.objects.filter(id=validated_data['id'])
        if user_qs.exists():
            raise serializers.ValidationError({'msg': 'User already exists'})
        
        try:
            user = User(**validated_data)
            password = validated_data.get('password', secrets.token_urlsafe())
            user.set_password(password)
            user.save()
        except Exception as exc:
            logger.error(f'Could not create user: {validated_data["id"]} - {validated_data["email"]}', exc_info=True)
            raise serializers.ValidationError({'msg': 'Could not create user'})
        return user

    def update(self, instance, validated_data):
        # print(f'Update user: {instance.id}')
        # Remove 'id' if present in validated_data to prevent its update
        validated_data.pop('id', None)
        return super(UserSerializer, self).update(instance, validated_data)

class RootUserSerializer(serializers.ModelSerializer):
    # Since we use ModelSerializer, the field "id" is automatically not allowed to be written to.
    id = serializers.UUIDField(read_only=False)

    class Meta:
        model = User
        fields = '__all__'

    def create(self, validated_data):
        user = User(**validated_data)
        user.set_password(validated_data['password'])
        user.save()
        return user


class TeamSerializer(serializers.ModelSerializer):
    # Since we use ModelSerializer, the field "id" is automatically not allowed to be written to.
    id = serializers.UUIDField(read_only=False)
    
    class Meta:
        model = Team
        fields = '__all__'

    def create(self, validated_data):
        # Check if team already exists
        team_qs = Team.objects.filter(id=validated_data['id'])
        if team_qs.exists():
            team_instance = team_qs.first()
            return team_instance
        
        # Create the Team instance
        try:
            team = Team.objects.create(**validated_data)
        except Exception as exc:
            logger.error(f'Could not create team: {validated_data["id"]} - {validated_data["name"]}', exc_info=True)
            raise serializers.ValidationError({'msg': 'Could not create team'})

        # Get the user from the request context
        try:
            user = User.objects.get(id=validated_data['owner'].id)
            # Set the team of the user who created the team
            user.team = team
            user.save()
        except Exception as exc:
            logger.error(f'Created team: {team.id} but could not set the owner: {validated_data["owner"]}')
            raise serializers.ValidationError({'msg': 'Could not set the owner of the team'})

        return team

    def update(self, instance, validated_data):
        # Remove 'id' if present in validated_data to prevent its update
        validated_data.pop('id', None)
        return super(TeamSerializer, self).update(instance, validated_data)

    def delete(self, instance, validated_data):
        # TODO: Test this
        # Get the user from the request context
        try:
            user = User.objects.get(id=validated_data['owner'].id)
            # Set the team of the user who created the team
            user.team = None
            user.save()
        except Exception as exc:
            logger.error(f'Deleted team: {instance.id} but could not remove the owner: {validated_data["owner"]}')

        return super(TeamSerializer, self).delete(instance, validated_data)