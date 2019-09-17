import hashlib
import hmac
import base64
import time
import six
import json
import requests
import os
import warnings
from requests.auth import AuthBase
from urllib.parse import quote, urljoin, urlsplit, urlparse
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5


imap = map


class CBProAuth(AuthBase):
    URL = 'https://api.pro.coinbase.com/'
    BTC_ACCTID = 'a19c1a3c-2255-49e5-a912-c9dd5c783343'
    MAIN_ID = 'db060343-88d3-4360-9659-fb38a3cdb998'


    def __init__(self):
        self.api_key = 'a768f040572dbc73694ecde51682313a'
        self.secret_key = 'YTTOGlL7fgio5OWXSnGLWwoIgpj3uzYq9iy6WKJ2lgSaDoKZYa9GHMvtRHEmhTmxONDEQuaX8Qy+GExmL4UX1w=='
        self.passphrase = '7rq8c81dhav'


    def __call__(self, request):
        self.timestamp = str(time.time())
        self.message = ''.join([self.timestamp, request.method, request.path_url, (request.body or '')])
        request.headers.update(self.GetHeaders())

        return request


    def GetHeaders(self):
        message = self.message.encode('ascii')
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode('utf-8')
        return {
            'Content-Type': 'Application/JSON',
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': self.timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase}



from requests.compat import json


class CoinbaseError(Exception):
    """Base error class for all exceptions raised in this library.
    Will never be raised naked; more specific subclasses of this exception will
    be raised when appropriate."""


class APIError(CoinbaseError):
    """Raised for errors related to interacting with the Coinbase API server."""

    def __init__(self, response, id, message, errors=None):
        self.status_code = response.status_code
        self.response = response
        self.id = id or ''
        self.message = message or ''
        self.request = getattr(response, 'request', None)
        self.errors = errors or []

    def __str__(self):  # pragma: no cover
        return 'APIError(id=%s): %s' % (self.id, self.message)


class TwoFactorRequiredError(APIError):
    pass


class ParamRequiredError(APIError):
    pass


class ValidationError(APIError):
    pass


class InvalidRequestError(APIError):
    pass


class PersonalDetailsRequiredError(APIError):
    pass


class AuthenticationError(APIError):
    pass


class UnverifiedEmailError(APIError):
    pass


class InvalidTokenError(APIError):
    pass


class RevokedTokenError(APIError):
    pass


class ExpiredTokenError(APIError):
    pass


class InvalidScopeError(APIError):
    pass


class NotFoundError(APIError):
    pass


class RateLimitExceededError(APIError):
    pass


class InternalServerError(APIError):
    pass


class BadGatewayError(APIError):
    pass


class ServiceUnavailableError(APIError):
    pass


def build_api_error(response, blob=None):
    """Helper method for creating errors and attaching HTTP response/request
    details to them.
    """
    try:
        blob = blob or response.json()
    except json.JSONDecodeError:
        blob = {}
    error_list = blob.get('errors', None)
    error = (error_list[0] if error_list else {})
    if error:
        error_id = error.get('id', '')
        error_message = error.get('message', '')
    else:
        # In the case of an OAuth-specific error, the response data is the error
        # blob, and the keys are slightly different. See
        # https://developers.coinbase.com/api/v2#error-response
        error_id = blob.get('error')
        error_message = blob.get('error_description')
    error_class = (
        _error_id_to_class.get(error_id, None) or
        _status_code_to_class.get(response.status_code, APIError))
    return error_class(response, error_id, error_message, error_list)


_error_id_to_class = {
    'two_factor_required': TwoFactorRequiredError,
    'param_required': ParamRequiredError,
    'validation_error': ValidationError,
    'invalid_request': InvalidRequestError,
    'personal_details_required': PersonalDetailsRequiredError,
    'authentication_error': AuthenticationError,
    'unverified_email': UnverifiedEmailError,
    'invalid_token': InvalidTokenError,
    'revoked_token': RevokedTokenError,
    'expired_token': ExpiredTokenError,
    'invalid_scope': InvalidScopeError,
    'not_found': NotFoundError,
    'rate_limit_exceeded': RateLimitExceededError,
    'internal_server_error': InternalServerError,
}

_status_code_to_class = {
    400: InvalidRequestError,
    401: AuthenticationError,
    402: TwoFactorRequiredError,
    403: InvalidScopeError,
    404: NotFoundError,
    422: ValidationError,
    429: RateLimitExceededError,
    500: InternalServerError,
    502: BadGatewayError,
    503: ServiceUnavailableError,}




def new_api_object(client, obj, cls=None, **kwargs):
    if isinstance(obj, dict):
        if not cls:
            resource = obj.get('resource', None)
            cls = _resource_to_model.get(resource, None)
        if not cls:
            obj_keys = set(six.iterkeys(obj))
            for keys, model in six.iteritems(_obj_keys_to_model):
                if keys <= obj_keys:
                    cls = model
                    break
        cls = cls or APIObject
        result = cls(client, **kwargs)
        for k, v in six.iteritems(obj):
            result[k] = new_api_object(client, v)
        return result
    if isinstance(obj, list):
        return [new_api_object(client, v, cls) for v in obj]
    return obj


class APIObject(dict):
    """Generic class used to represent a JSON response from the Coinbase API.
    If you're a consumer of the API, you shouldn't be using this class directly.
    This exists to make it easier to consume our API by allowing dot-notation
    access to the responses, as well as automatically parsing the responses into
    the appropriate Python models.
    """
    __api_client = None
    __resource_path = None
    __response = None
    __pagination = None
    __warnings = None

    def __init__(self, api_client, response=None, pagination=None, warnings=None):
        self.__api_client = api_client
        if response:
            self.__resource_path = urlsplit(response.url).path
        self.__response = response
        self.__pagination = pagination
        self.__warnings = warnings

    @property
    def api_client(self):
        return self.__api_client

    @property
    def resource_path(self):
        return self.__resource_path

    @property
    def response(self):
        return self.__response

    @property
    def warnings(self):
        return self.__warnings

    @property
    def pagination(self):
        return self.__pagination

    def refresh(self, **params):
        url = getattr(self, 'resource_path', None)
        if not url:
            raise ValueError("Unable to refresh: missing 'resource_path' attribute.")
        response = self.api_client._get(url, data=params)
        data = self.api_client._make_api_object(response, type(self))
        self.update(data)
        return data

    # The following three method definitions allow dot-notation access to member
    # objects for convenience.
    def __getattr__(self, *args, **kwargs):
        try:
            return dict.__getitem__(self, *args, **kwargs)
        except KeyError as key_error:
            attribute_error = AttributeError(*key_error.args)
            attribute_error.message = getattr(key_error, 'message', '')
            raise attribute_error

    def __delattr__(self, *args, **kwargs):
        try:
            return dict.__delitem__(self, *args, **kwargs)
        except KeyError as key_error:
            attribute_error = AttributeError(*key_error.args)
            attribute_error.message = getattr(key_error, 'message', '')
            raise attribute_error

    def __setattr__(self, key, value):
        # All attributes that start with '_' will not be accessible via item-getter
        # syntax, which means that they won't be included in conversion to a
        # vanilla dict, which means that APIObjects can be treated as equivalent to
        # dicts. This is nice because it allows direct JSON-serialization of any
        # APIObject.
        if key.startswith('_') or key in self.__dict__:
            return dict.__setattr__(self, key, value)
        return dict.__setitem__(self, key, value)

    # When an API response includes multiple items, allow direct accessing that
    # data instead of forcing additional attribute access. This works for
    # slicing and index reference only.
    def __getitem__(self, key):
        data = getattr(self, 'data', None)
        if isinstance(data, list) and isinstance(key, (int, slice)):
            return data[key]
        return dict.__getitem__(self, key)

    def __dir__(self):  # pragma: no cover
        # This makes tab completion work in interactive shells like IPython for all
        # attributes, items, and methods.
        return list(self.keys())

    def __str__(self):
        try:
            return json.dumps(self, sort_keys=True, indent=2)
        except TypeError:
            return '(invalid JSON)'

    def __name__(self):
        return '<{} @ {}>'.format(type(self).__name__, hex(id(self)))  # pragma: no cover

    def __repr__(self):
        return '{} {}'.format(self.__name__(), str(self))  # pragma: no cover


class Account(APIObject):
    def set_primary(self, **params):
        """https://developers.coinbase.com/api/v2#set-account-as-primary"""
        data = self.api_client.set_primary_account(self.id, **params)
        self.update(data)
        return data

    def modify(self, **params):
        """https://developers.coinbase.com/api#modify-an-account"""
        data = self.api_client.update_account(self.id, **params)
        self.update(data)
        return data

    def delete(self, **params):
        """https://developers.coinbase.com/api#delete-an-account"""
        return self.api_client.delete_account(self.id, **params)

    # Addresses API
    # -----------------------------------------------------------
    def get_addresses(self, **params):
        """https://developers.coinbase.com/api/v2#list-addresses"""
        return self.api_client.get_addresses(self.id, **params)

    def get_address(self, address_id, **params):
        """https://developers.coinbase.com/api/v2#show-addresss"""
        return self.api_client.get_address(self.id, address_id, **params)

    def get_address_transactions(self, address_id, **params):
        """https://developers.coinbase.com/api/v2#list-address39s-transactions"""
        return self.api_client.get_address_transactions(self.id, address_id, **params)

    def create_address(self, **params):
        """https://developers.coinbase.com/api/v2#show-addresss"""
        return self.api_client.create_address(self.id, **params)

    # Transactions API
    # -----------------------------------------------------------
    def get_transactions(self, **params):
        """https://developers.coinbase.com/api/v2#list-transactions"""
        return self.api_client.get_transactions(self.id, **params)

    def get_transaction(self, transaction_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-transaction"""
        return self.api_client.get_transaction(self.id, transaction_id, **params)

    def send_money(self, **params):
        """https://developers.coinbase.com/api/v2#send-money"""
        return self.api_client.send_money(self.id, **params)

    def transfer_money(self, **params):
        """https://developers.coinbase.com/api/v2#transfer-money-between-accounts"""
        return self.api_client.transfer_money(self.id, **params)

    def request_money(self, **params):
        """https://developers.coinbase.com/api/v2#request-money"""
        return self.api_client.request_money(self.id, **params)

    # Reports API
    # -----------------------------------------------------------
    def get_reports(self, **params):
        """https://developers.coinbase.com/api/v2#list-all-reports"""
        return self.api_client.get_reports(**params)

    def get_report(self, report_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-report"""
        return self.api_client.get_report(report_id, **params)

    def create_report(self, **params):
        """https://developers.coinbase.com/api/v2#generate-a-new-report"""
        return self.api_client.create_report(**params)

    # Buys API
    # -----------------------------------------------------------
    def get_buys(self, **params):
        """https://developers.coinbase.com/api/v2#list-buys"""
        return self.api_client.get_buys(self.id, **params)

    def get_buy(self, buy_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-buy"""
        return self.api_client.get_buy(self.id, buy_id, **params)

    def buy(self, **params):
        """https://developers.coinbase.com/api/v2#buy-bitcoin"""
        return self.api_client.buy(self.id, **params)

    def commit_buy(self, buy_id, **params):
        """https://developers.coinbase.com/api/v2#commit-a-buy"""
        return self.api_client.commit_buy(self.id, buy_id, **params)

    # Sells API
    # -----------------------------------------------------------
    def get_sells(self, **params):
        """https://developers.coinbase.com/api/v2#list-sells"""
        return self.api_client.get_sells(self.id, **params)

    def get_sell(self, sell_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-sell"""
        return self.api_client.get_sell(self.id, sell_id, **params)

    def sell(self, **params):
        """https://developers.coinbase.com/api/v2#sell-bitcoin"""
        return self.api_client.sell(self.id, **params)

    def commit_sell(self, sell_id, **params):
        """https://developers.coinbase.com/api/v2#commit-a-sell"""
        return self.api_client.commit_sell(self.id, sell_id, **params)

    # Deposits API
    # -----------------------------------------------------------
    def get_deposits(self, **params):
        """https://developers.coinbase.com/api/v2#list-deposits"""
        return self.api_client.get_deposits(self.id, **params)

    def get_deposit(self, deposit_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-deposit"""
        return self.api_client.get_deposit(self.id, deposit_id, **params)

    def deposit(self, **params):
        """https://developers.coinbase.com/api/v2#deposit-funds"""
        return self.api_client.deposit(self.id, **params)

    def commit_deposit(self, deposit_id, **params):
        """https://developers.coinbase.com/api/v2#commit-a-deposit"""
        return self.api_client.commit_deposit(self.id, deposit_id, **params)

    # Withdrawals API
    # -----------------------------------------------------------
    def get_withdrawals(self, **params):
        """https://developers.coinbase.com/api/v2#list-withdrawals"""
        return self.api_client.get_withdrawals(self.id, **params)

    def get_withdrawal(self, withdrawal_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-withdrawal"""
        return self.api_client.get_withdrawal(self.id, withdrawal_id, **params)

    def withdraw(self, **params):
        """https://developers.coinbase.com/api/v2#withdraw-funds"""
        return self.api_client.withdraw(self.id, **params)

    def commit_withdrawal(self, withdrawal_id, **params):
        """https://developers.coinbase.com/api/v2#commit-a-withdrawal"""
        return self.api_client.commit_withdrawal(self.id, withdrawal_id, **params)


class Notification(APIObject):
    pass


class Address(APIObject):
    pass


class Checkout(APIObject):
    def get_orders(self, **params):
        """https://developers.coinbase.com/api/v2#list-checkout39s-orders"""
        return self.api_client.get_checkout_orders(self.id, **params)

    def create_order(self, **params):
        """https://developers.coinbase.com/api/v2#create-a-new-order-for-a-checkout"""
        return self.api_client.create_checkout_order(self.id, **params)


class Merchant(APIObject):
    pass


class Money(APIObject):
    def __str__(self):
        currency_str = '%s %s' % (self.currency, self.amount)
        # Some API responses return mappings that look like Money objects (with
        # 'amount' and 'currency' keys) but with additional information. In those
        # cases, the string representation also includes a full dump of the keys of
        # the object.
        if set(dir(self)) > set(('amount', 'currency')):
            return '{} {}'.format(
                currency_str, json.dumps(self, sort_keys=True, indent=2))
        return currency_str


class Order(APIObject):
    def refund(self, **params):
        data = self.api_client.refund_order(self.id, **params)
        self.update(data)
        return data


class PaymentMethod(APIObject):
    pass


class Transaction(APIObject):
    def complete(self):
        """https://developers.coinbase.com/api/v2#complete-request-money"""
        response = self.api_client._post(self.resource_path, 'complete')
        return self.api_client._make_api_object(response, APIObject)

    def resend(self):
        """https://developers.coinbase.com/api/v2#re-send-request-money"""
        response = self.api_client._post(self.resource_path, 'resend')
        return self.api_client._make_api_object(response, APIObject)

    def cancel(self):
        """https://developers.coinbase.com/api/v2#cancel-request-money"""
        response = self.api_client._post(self.resource_path, 'cancel')
        return self.api_client._make_api_object(response, APIObject)


class Report(APIObject):
    pass


class Transfer(APIObject):
    def commit(self, **params):
        response = self.api_client._post(self.resource_path, 'commit')
        data = self.api_client._make_api_object(response, type(self))
        self.update(data)
        return data


class Buy(Transfer):
    pass


class Sell(Transfer):
    pass


class Deposit(Transfer):
    pass


class Withdrawal(Transfer):
    pass


class User(APIObject):
    pass


class CurrentUser(User):
    def modify(self, **params):
        """https://developers.coinbase.com/api/v2#update-current-user"""
        data = self.api_client.update_current_user(**params)
        self.update(data)
        return data


# The following dicts are used to automatically parse API responses into the
# appropriate Python models. See `new_api_object` for more details.
_resource_to_model = {
    'account': Account,
    'balance': Money,
    'buy': Buy,
    'checkout': Checkout,
    'deposit': Transfer,
    'merchant': Merchant,
    'notification': Notification,
    'order': Order,
    'payment_method': PaymentMethod,
    'report': Report,
    'sell': Sell,
    'transaction': Transaction,
    'transfer': Transfer,
    'user': User,
    'withdrawal': Withdrawal,
}
_obj_keys_to_model = {
    frozenset(('amount', 'currency')): Money,
}


COINBASE_CRT_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'ca-coinbase.crt')

COINBASE_CALLBACK_PUBLIC_KEY_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'coinbase-callback.pub')



def clean_params(params, drop_nones=True, recursive=True):
    """Clean up a dict of API parameters to be sent to the Coinbase API.
    Some endpoints require boolean options to be represented as integers. By
    default, will remove all keys whose value is None, so that they will not be
    sent to the API endpoint at all.
    """
    cleaned = {}
    for key, value in six.iteritems(params):
        if drop_nones and value is None:
            continue
        if recursive and isinstance(value, dict):
            value = clean_params(value, drop_nones, recursive)
        cleaned[key] = value
    return cleaned


def encode_params(params, **kwargs):
    """Clean and JSON-encode a dict of parameters."""
    cleaned = clean_params(params, **kwargs)
    return json.dumps(cleaned)


def check_uri_security(uri):
    """Warns if the URL is insecure."""
    if urlparse(uri).scheme != 'https':
        warning_message = (
            'WARNING: this client is sending a request to an insecure'
            ' API endpoint. Any API request you make may expose your API key and'
            ' secret to third parties. Consider using the default endpoint:\n\n'
            '  %s\n') % uri
        warnings.warn(warning_message, UserWarning)
    return uri


class Client(object):
    """API Client for the Coinbase API.
    Entry point for making requests to the Coinbase API. Provides helper methods
    for common API endpoints, as well as niceties around response verification
    and formatting.
    Any errors will be raised as exceptions. These exceptions will always be
    subclasses of `coinbase.error.APIError`. HTTP-related errors will also be
    subclasses of `requests.HTTPError`.
    Full API docs, including descriptions of each API and its paramters, are
    available here: https://developers.coinbase.com/api
    """

    VERIFY_SSL = True

    BASE_API_URI = 'https://api.coinbase.com/'
    API_VERSION = '2016-02-18'

    cached_callback_public_key = None

    def __init__(self, api_key, api_secret, base_api_uri=None, api_version=None):
        if not api_key:
            raise ValueError('Missing `api_key`.')
        if not api_secret:
            raise ValueError('Missing `api_secret`.')

        # Allow passing in a different API base.
        self.BASE_API_URI = check_uri_security(base_api_uri or self.BASE_API_URI)

        self.API_VERSION = api_version or self.API_VERSION

        # Set up a requests session for interacting with the API.
        self.session = self._build_session(CBProAuth, api_key, api_secret, self.API_VERSION)

    def _build_session(self, auth_class, *args, **kwargs):
        """Internal helper for creating a requests `session` with the correct
        authentication handling.
        """
        session = requests.session()
        session.auth = auth_class(*args, **kwargs)
        session.headers.update({'CB-VERSION': self.API_VERSION,
                                'Accept': 'application/json',
                                'Content-Type': 'application/json',
                                'User-Agent': 'coinbase/python/2.0'})
        return session

    def _create_api_uri(self, *parts):
        """Internal helper for creating fully qualified endpoint URIs."""
        return urljoin(self.BASE_API_URI, '/'.join(imap(quote, parts)))

    def _request(self, method, *relative_path_parts, **kwargs):
        """Internal helper for creating HTTP requests to the Coinbase API.
        Raises an APIError if the response is not 20X. Otherwise, returns the
        response object. Not intended for direct use by API consumers.
        """
        uri = self._create_api_uri(*relative_path_parts)
        data = kwargs.get('data', None)
        if data and isinstance(data, dict):
            kwargs['data'] = encode_params(data)
        if self.VERIFY_SSL:
            kwargs.setdefault('verify', COINBASE_CRT_PATH)
        else:
            kwargs.setdefault('verify', False)
        kwargs.update(verify=self.VERIFY_SSL)
        response = getattr(self.session, method)(uri, **kwargs)
        return self._handle_response(response)

    def _handle_response(self, response):
        """Internal helper for handling API responses from the Coinbase server.
        Raises the appropriate exceptions when necessary; otherwise, returns the
        response.
        """
        if not str(response.status_code).startswith('2'):
            raise build_api_error(response)
        return response

    def _get(self, *args, **kwargs):
        """Get requests can be paginated, ensure we iterate through all the pages."""
        prev_data = kwargs.pop('prev_data', [])
        resp = self._request('get', *args, **kwargs)
        resp_content = resp._content
        if not resp_content:
            # No content so its obviously not paginated
            return resp

        # if resp._content is a bytes object, decode it so we can loads it as json
        if isinstance(resp_content, bytes):
            resp_content = resp_content.decode('utf-8')

        # Load the json so we can use the data as python objects
        content = json.loads(resp_content)
        if 'pagination' not in content:
            # Result is not paginated
            return resp

        page_info = content['pagination']
        if not page_info['next_uri']:
            # next_uri is None when the cursor has been iterated to the last element
            content['data'].extend(prev_data)
            # If resp._content was is a bytes object, only set it as a bytes object
            if isinstance(resp_content, bytes):
                resp._content = json.dumps(content).decode('utf-8')
            else:
                resp._content = json.dumps(content)
            return resp

        prev_data.extend(content['data'])
        next_page_id = page_info['next_uri'].split('=')[-1]
        kwargs.update({
            'prev_data': prev_data,
            'params': {'starting_after': next_page_id}
        })
        return self._get(*args, **kwargs)

    def _post(self, *args, **kwargs):
        return self._request('post', *args, **kwargs)

    def _put(self, *args, **kwargs):
        return self._request('put', *args, **kwargs)

    def _delete(self, *args, **kwargs):
        return self._request('delete', *args, **kwargs)

    def _make_api_object(self, response, model_type=None):
        blob = response.json()
        data = blob.get('data', None)
        # All valid responses have a "data" key.
        if data is None:
            raise build_api_error(response, blob)
        # Warn the user about each warning that was returned.
        warnings_data = blob.get('warnings', None)
        for warning_blob in warnings_data or []:
            message = "%s (%s)" % (
                warning_blob.get('message', ''),
                warning_blob.get('url', ''))
            warnings.warn(message, UserWarning)

        pagination = blob.get('pagination', None)
        kwargs = {
            'response': response,
            'pagination': pagination and new_api_object(None, pagination, APIObject),
            'warnings': warnings_data and new_api_object(None, warnings_data, APIObject)
        }
        if isinstance(data, dict):
            obj = new_api_object(self, data, model_type, **kwargs)
        else:
            obj = APIObject(self, **kwargs)
            obj.data = new_api_object(self, data, model_type)
        return obj

    # Data API
    # -----------------------------------------------------------
    def get_currencies(self, **params):
        """https://developers.coinbase.com/api/v2#currencies"""
        response = self._get('v2', 'currencies', params=params)
        return self._make_api_object(response, APIObject)

    def get_exchange_rates(self, **params):
        """https://developers.coinbase.com/api/v2#exchange-rates"""
        response = self._get('v2', 'exchange-rates', params=params)
        return self._make_api_object(response, APIObject)

    def get_buy_price(self, **params):
        """https://developers.coinbase.com/api/v2#get-buy-price"""
        currency_pair = params.get('currency_pair', 'BTC-USD')
        response = self._get('v2', 'prices', currency_pair, 'buy', params=params)
        return self._make_api_object(response, APIObject)

    def get_sell_price(self, **params):
        """https://developers.coinbase.com/api/v2#get-sell-price"""
        currency_pair = params.get('currency_pair', 'BTC-USD')
        response = self._get('v2', 'prices', currency_pair, 'sell', params=params)
        return self._make_api_object(response, APIObject)

    def get_spot_price(self, **params):
        """https://developers.coinbase.com/api/v2#get-spot-price"""
        currency_pair = params.get('currency_pair', 'BTC-USD')
        response = self._get('v2', 'prices', currency_pair, 'spot', params=params)
        return self._make_api_object(response, APIObject)

    def get_historic_prices(self, **params):
        """https://developers.coinbase.com/api/v2#get-historic-prices"""
        if 'currency_pair' in params:
            currency_pair = params['currency_pair']
        else:
            currency_pair = 'BTC-USD'
        response = self._get('v2', 'prices', currency_pair, 'historic', params=params)
        return self._make_api_object(response, APIObject)

    def get_time(self, **params):
        """https://developers.coinbase.com/api/v2#time"""
        response = self._get('v2', 'time', params=params)
        return self._make_api_object(response, APIObject)

    # User API
    # -----------------------------------------------------------
    def get_user(self, user_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-user"""
        response = self._get('v2', 'users', user_id, params=params)
        return self._make_api_object(response, User)

    def get_current_user(self, **params):
        """https://developers.coinbase.com/api/v2#show-current-user"""
        response = self._get('v2', 'user', params=params)
        return self._make_api_object(response, CurrentUser)

    def get_auth_info(self, **params):
        """https://developers.coinbase.com/api/v2#show-authorization-information"""
        response = self._get('v2', 'user', 'auth', params=params)
        return self._make_api_object(response, APIObject)

    def update_current_user(self, **params):
        """https://developers.coinbase.com/api/v2#update-current-user"""
        response = self._put('v2', 'user', data=params)
        return self._make_api_object(response, CurrentUser)

    # Accounts API
    # -----------------------------------------------------------
    def get_accounts(self, **params):
        """https://developers.coinbase.com/api/v2#list-accounts"""
        response = self._get('v2', 'accounts', params=params)
        return self._make_api_object(response, Account)

    def get_account(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#show-an-account"""
        response = self._get('v2', 'accounts', account_id, params=params)
        return self._make_api_object(response, Account)

    def get_primary_account(self, **params):
        """https://developers.coinbase.com/api/v2#show-an-account"""
        return self.get_account('primary', **params)

    def create_account(self, **params):
        """https://developers.coinbase.com/api/v2#create-account"""
        response = self._post('v2', 'accounts', data=params)
        return self._make_api_object(response, Account)

    def set_primary_account(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#set-account-as-primary"""
        response = self._post('v2', 'accounts', account_id, 'primary', data=params)
        return self._make_api_object(response, Account)

    def update_account(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#update-account"""
        response = self._put('v2', 'accounts', account_id, data=params)
        return self._make_api_object(response, Account)

    def delete_account(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#delete-account"""
        self._delete('v2', 'accounts', account_id, data=params)
        return None

    # Notifications API
    # -----------------------------------------------------------
    def get_notifications(self, **params):
        """https://developers.coinbase.com/api/v2#list-notifications"""
        response = self._get('v2', 'notifications', params=params)
        return self._make_api_object(response, Notification)

    def get_notification(self, notification_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-notification"""
        response = self._get('v2', 'notifications', notification_id, params=params)
        return self._make_api_object(response, Notification)

    # Addresses API
    # -----------------------------------------------------------
    def get_addresses(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#list-addresses"""
        response = self._get('v2', 'accounts', account_id, 'addresses', params=params)
        return self._make_api_object(response, Address)

    def get_address(self, account_id, address_id, **params):
        """https://developers.coinbase.com/api/v2#show-addresss"""
        response = self._get('v2', 'accounts', account_id, 'addresses', address_id, params=params)
        return self._make_api_object(response, Address)

    def get_address_transactions(self, account_id, address_id, **params):
        """https://developers.coinbase.com/api/v2#list-address39s-transactions"""
        response = self._get(
            'v2',
            'accounts',
            account_id,
            'addresses',
            address_id,
            'transactions',
            params=params)
        return self._make_api_object(response, Transaction)

    def create_address(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#create-address"""
        response = self._post('v2', 'accounts', account_id, 'addresses', data=params)
        return self._make_api_object(response, Address)

    # Transactions API
    # -----------------------------------------------------------
    def get_transactions(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#list-transactions"""
        response = self._get('v2', 'accounts', account_id, 'transactions', params=params)
        return self._make_api_object(response, Transaction)

    def get_transaction(self, account_id, transaction_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-transaction"""
        response = self._get(
            'v2', 'accounts', account_id, 'transactions', transaction_id, params=params)
        return self._make_api_object(response, Transaction)

    def send_money(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#send-money"""
        for required in ['to', 'amount', 'currency']:
            if required not in params:
                raise ValueError("Missing required parameter: %s" % required)
        params['type'] = 'send'
        response = self._post('v2', 'accounts', account_id, 'transactions', data=params)
        return self._make_api_object(response, Transaction)

    def transfer_money(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#transfer-money-between-accounts"""
        for required in ['to', 'amount', 'currency']:
            if required not in params:
                raise ValueError("Missing required parameter: %s" % required)
        params['type'] = 'transfer'
        response = self._post('v2', 'accounts', account_id, 'transactions', data=params)
        return self._make_api_object(response, Transaction)

    def request_money(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#request-money"""
        for required in ['to', 'amount', 'currency']:
            if required not in params:
                raise ValueError("Missing required parameter: %s" % required)
        params['type'] = 'request'
        response = self._post('v2', 'accounts', account_id, 'transactions', data=params)
        return self._make_api_object(response, Transaction)

    def complete_request(self, account_id, request_id, **params):
        """https://developers.coinbase.com/api/v2#complete-request-money"""
        response = self._post(
            'v2', 'accounts', account_id, 'transactions', request_id,
            'complete', data=params)
        return self._make_api_object(response, APIObject)

    def resend_request(self, account_id, request_id, **params):
        """https://developers.coinbase.com/api/v2#re-send-request-money"""
        response = self._post(
            'v2', 'accounts', account_id, 'transactions', request_id, 'resend',
            data=params)
        return self._make_api_object(response, APIObject)

    def cancel_request(self, account_id, request_id, **params):
        """https://developers.coinbase.com/api/v2#cancel-request-money"""
        response = self._post(
            'v2', 'accounts', account_id, 'transactions', request_id, 'cancel',
            data=params)
        return self._make_api_object(response, APIObject)

    # Reports API
    # -----------------------------------------------------------
    def get_reports(self, **params):
        """https://developers.coinbase.com/api/v2#list-all-reports"""
        response = self._get('v2', 'reports', data=params)
        return self._make_api_object(response, Report)

    def get_report(self, report_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-report"""
        response = self._get('v2', 'reports', report_id, data=params)
        return self._make_api_object(response, Report)

    def create_report(self, **params):
        """https://developers.coinbase.com/api/v2#generate-a-new-report"""
        if 'type' not in params and 'email' not in params:
            raise ValueError("Missing required parameter: 'type' or 'email'")
        response = self._post('v2', 'reports', data=params)
        return self._make_api_object(response, Report)

    # Buys API
    # -----------------------------------------------------------
    def get_buys(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#list-buys"""
        response = self._get('v2', 'accounts', account_id, 'buys', params=params)
        return self._make_api_object(response, Buy)

    def get_buy(self, account_id, buy_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-buy"""
        response = self._get('v2', 'accounts', account_id, 'buys', buy_id, params=params)
        return self._make_api_object(response, Buy)

    def buy(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#buy-bitcoin"""
        if 'amount' not in params and 'total' not in params:
            raise ValueError("Missing required parameter: 'amount' or 'total'")
        for required in ['currency', 'payment_method']:
            if required not in params:
                raise ValueError("Missing required parameter: %s" % required)
        response = self._post('v2', 'accounts', account_id, 'buys', data=params)
        return self._make_api_object(response, Buy)

    def commit_buy(self, account_id, buy_id, **params):
        """https://developers.coinbase.com/api/v2#commit-a-buy"""
        response = self._post(
            'v2', 'accounts', account_id, 'buys', buy_id, 'commit', data=params)
        return self._make_api_object(response, Buy)

    # Sells API
    # -----------------------------------------------------------
    def get_sells(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#list-sells"""
        response = self._get('v2', 'accounts', account_id, 'sells', params=params)
        return self._make_api_object(response, Sell)

    def get_sell(self, account_id, sell_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-sell"""
        response = self._get(
            'v2', 'accounts', account_id, 'sells', sell_id, params=params)
        return self._make_api_object(response, Sell)

    def sell(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#sell-bitcoin"""
        if 'amount' not in params and 'total' not in params:
            raise ValueError("Missing required parameter: 'amount' or 'total'")
        for required in ['currency']:
            if required not in params:
                raise ValueError("Missing required parameter: %s" % required)
        response = self._post('v2', 'accounts', account_id, 'sells', data=params)
        return self._make_api_object(response, Sell)

    def commit_sell(self, account_id, sell_id, **params):
        """https://developers.coinbase.com/api/v2#commit-a-sell"""
        response = self._post(
            'v2', 'accounts', account_id, 'sells', sell_id, 'commit', data=params)
        return self._make_api_object(response, Sell)

    # Deposits API
    # -----------------------------------------------------------
    def get_deposits(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#list-deposits"""
        response = self._get('v2', 'accounts', account_id, 'deposits', params=params)
        return self._make_api_object(response, Deposit)

    def get_deposit(self, account_id, deposit_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-deposit"""
        response = self._get(
            'v2', 'accounts', account_id, 'deposits', deposit_id, params=params)
        return self._make_api_object(response, Deposit)

    def deposit(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#deposit-funds"""
        for required in ['payment_method', 'amount', 'currency']:
            if required not in params:
                raise ValueError("Missing required parameter: %s" % required)
        response = self._post('v2', 'accounts', account_id, 'deposits', data=params)
        return self._make_api_object(response, Deposit)

    def commit_deposit(self, account_id, deposit_id, **params):
        """https://developers.coinbase.com/api/v2#commit-a-deposit"""
        response = self._post(
            'v2', 'accounts', account_id, 'deposits', deposit_id, 'commit',
            data=params)
        return self._make_api_object(response, Deposit)

    # Withdrawals API
    # -----------------------------------------------------------
    def get_withdrawals(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#list-withdrawals"""
        response = self._get('v2', 'accounts', account_id, 'withdrawals', params=params)
        return self._make_api_object(response, Withdrawal)

    def get_withdrawal(self, account_id, withdrawal_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-withdrawal"""
        response = self._get(
            'v2', 'accounts', account_id, 'withdrawals', withdrawal_id, params=params)
        return self._make_api_object(response, Withdrawal)

    def withdraw(self, account_id, **params):
        """https://developers.coinbase.com/api/v2#withdraw-funds"""
        for required in ['payment_method', 'amount', 'currency']:
            if required not in params:
                raise ValueError("Missing required parameter: %s" % required)
        response = self._post('v2', 'accounts', account_id, 'withdrawals', data=params)
        return self._make_api_object(response, Withdrawal)

    def commit_withdrawal(self, account_id, withdrawal_id, **params):
        """https://developers.coinbase.com/api/v2#commit-a-withdrawal"""
        response = self._post(
            'v2', 'accounts', account_id, 'withdrawals', withdrawal_id, 'commit',
            data=params)
        return self._make_api_object(response, Withdrawal)

    # Payment Methods API
    # -----------------------------------------------------------
    def get_payment_methods(self, **params):
        """https://developers.coinbase.com/api/v2#list-payment-methods"""
        response = self._get('v2', 'payment-methods', params=params)
        return self._make_api_object(response, PaymentMethod)

    def get_payment_method(self, payment_method_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-payment-method"""
        response = self._get('v2', 'payment-methods', payment_method_id, params=params)
        return self._make_api_object(response, PaymentMethod)

    # Merchants API
    # -----------------------------------------------------------
    def get_merchant(self, merchant_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-merchant"""
        response = self._get('v2', 'merchants', merchant_id, params=params)
        return self._make_api_object(response, Merchant)

    # Orders API
    # -----------------------------------------------------------
    def get_orders(self, **params):
        """https://developers.coinbase.com/api/v2#list-orders"""
        response = self._get('v2', 'orders', params=params)
        return self._make_api_object(response, Order)

    def get_order(self, order_id, **params):
        """https://developers.coinbase.com/api/v2#show-an-order"""
        response = self._get('v2', 'orders', order_id, params=params)
        return self._make_api_object(response, Order)

    def create_order(self, **params):
        """https://developers.coinbase.com/api/v2#create-an-order"""
        for required in ['amount', 'currency', 'name']:
            if required not in params:
                raise ValueError("Missing required parameter: %s" % required)
        response = self._post('v2', 'orders', data=params)
        return self._make_api_object(response, Order)

    def refund_order(self, order_id, **params):
        """https://developers.coinbase.com/api/v2#refund-an-order"""
        for required in ['currency']:
            if required not in params:
                raise ValueError("Missing required parameter: %s" % required)
        response = self._post('v2', 'orders', order_id, 'refund', data=params)
        return self._make_api_object(response, Order)

    # Checkouts API
    # -----------------------------------------------------------
    def get_checkouts(self, **params):
        """https://developers.coinbase.com/api/v2#list-checkouts"""
        response = self._get('v2', 'checkouts', params=params)
        return self._make_api_object(response, Checkout)

    def get_checkout(self, checkout_id, **params):
        """https://developers.coinbase.com/api/v2#show-a-checkout"""
        response = self._get('v2', 'checkouts', checkout_id, params=params)
        return self._make_api_object(response, Checkout)

    def create_checkout(self, **params):
        """https://developers.coinbase.com/api/v2#create-checkout"""
        for required in ['amount', 'currency', 'name']:
            if required not in params:
                raise ValueError("Missing required parameter: %s" % required)
        response = self._post('v2', 'checkouts', data=params)
        return self._make_api_object(response, Checkout)

    def get_checkout_orders(self, checkout_id, **params):
        """https://developers.coinbase.com/api/v2#list-checkout39s-orders"""
        response = self._get('v2', 'checkouts', checkout_id, 'orders', params=params)
        return self._make_api_object(response, Order)

    def create_checkout_order(self, checkout_id, **params):
        """https://developers.coinbase.com/api/v2#create-a-new-order-for-a-checkout"""
        response = self._post('v2', 'checkouts', checkout_id, 'orders', data=params)
        return self._make_api_object(response, Order)

    def verify_callback(self, body, signature):
        h = SHA256.new()
        h.update(body)
        key = Client.callback_public_key()
        verifier = PKCS1_v1_5.new(key)
        signature = bytes(signature, 'utf-8') if six.PY3 else bytes(signature)
        signature_buffer = base64.b64decode(signature)
        return verifier.verify(h, signature_buffer)

    @staticmethod
    def callback_public_key():
        if Client.cached_callback_public_key is None:
            f = open(COINBASE_CALLBACK_PUBLIC_KEY_PATH, 'r')
            Client.cached_callback_public_key = RSA.importKey(f.read())
        return Client.cached_callback_public_key


class OAuthClient(Client):
    def __init__(self, access_token, refresh_token, api_key, api_secret, base_api_uri=None, api_version=None):
        super().__init__(api_key, api_secret, base_api_uri, api_version)
        if not access_token:
            raise ValueError("Missing `access_token`.")
        if not refresh_token:
            raise ValueError("Missing `refresh_token`.")

        self.access_token = access_token
        self.refresh_token = refresh_token

        # Allow passing in a different API base.
        self.BASE_API_URI = check_uri_security(base_api_uri or self.BASE_API_URI)

        self.API_VERSION = api_version or self.API_VERSION

        # Set up a requests session for interacting with the API.
        self.session = self._build_session(CBProAuth, lambda: self.access_token, self.API_VERSION)

    def revoke(self):
        """https://developers.coinbase.com/docs/wallet/coinbase-connect#revoking-an-access-token"""
        response = self._post('oauth', 'revoke', data={'token': self.access_token})
        return None

    def refresh(self):
        """Attempt to refresh the current access token / refresh token pair.
        If successful, the relevant attributes of this client will be updated
        automatically and the dict of token values and information given  by the
        Coinbase OAuth server will be returned to the caller.
        """
        params = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token
        }
        response = self._post('oauth', 'token', params=params)
        response = self._handle_response(response)
        blob = response.json()
        self.access_token = blob.get('access_token', None)
        self.refresh_token = blob.get('refresh_token', None)
        if not (self.access_token and self.refresh_token):
            raise build_api_error(response, blob)
        return blob


