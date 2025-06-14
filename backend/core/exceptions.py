
from rest_framework import status
from rest_framework.exceptions import APIException


class ValidationError(APIException):
    status_code = status.HTTP_400_BAD_REQUEST
    default_detail = {"msg": "invalid input"}
    default_code = 'invalid'


# class BalanceNotSufficientError(APIException):
#     status_code = status.HTTP_400_BAD_REQUEST
#     default_detail = {"msg": "Your balance is not sufficient"}
#     default_code = 'balance_not_sufficient'


class AuthenticationFailed(APIException):
    status_code = status.HTTP_401_UNAUTHORIZED
    default_detail = {"msg": "Incorrect authentication credentials."}
    default_code = 'authentication_failed'


class PermissionDenied(APIException):
    status_code = status.HTTP_403_FORBIDDEN
    default_detail = {"msg": "You do not have permission to perform this action."}
    default_code = 'permission_denied'


class NotFoundError(APIException):
    status_code = status.HTTP_404_NOT_FOUND
    default_detail = {"msg": "not found"}
    default_code = 'notfound'


class NoDataFound(APIException):
    status_code = status.HTTP_204_NO_CONTENT
    default_detail = {"msg": "no data found"}
    default_code = 'notdatafound'


class ServerError(APIException):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    default_detail = {"msg": "server error"}
    default_code = 'servererror'


# class InvalidTestReportError(APIException):
#     status_code = status.HTTP_400_BAD_REQUEST
#     default_detail = {"msg": "invalid test report"}
#     default_code = 'invalidtestreport'


class InvalidRequestError(APIException):
    status_code = status.HTTP_400_BAD_REQUEST
    default_detail = {"msg": "invalid request"}
    default_code = 'invalidrequest'