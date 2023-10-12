import logging
import os
import requests
import time
import re

from datetime import datetime
from cloudforet.cost_analysis.conf.cost_conf import *
from spaceone.core.connector import BaseConnector
from spaceone.core.error import *
from cloudforet.cost_analysis.error.cost import *
from azure.identity import DefaultAzureCredential
from azure.mgmt.billing import BillingManagementClient
from azure.mgmt.costmanagement import CostManagementClient

__all__ = ['AzureCostMgmtConnector']

_LOGGER = logging.getLogger(__name__)


class AzureCostMgmtConnector(BaseConnector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.billing_client = None
        self.cost_mgmt_client = None
        self.billing_account_id = None
        self.next_link = None

    def create_session(self, options: dict, secret_data: dict, schema: str):
        self._check_secret_data(secret_data)

        subscription_id = secret_data.get('subscription_id', '')

        os.environ["AZURE_SUBSCRIPTION_ID"] = subscription_id
        os.environ["AZURE_TENANT_ID"] = secret_data['tenant_id']
        os.environ["AZURE_CLIENT_ID"] = secret_data['client_id']
        os.environ["AZURE_CLIENT_SECRET"] = secret_data['client_secret']

        credential = DefaultAzureCredential()

        self.billing_account_id = secret_data.get('billing_account_id')
        self.billing_client = BillingManagementClient(credential=credential, subscription_id=subscription_id)
        self.cost_mgmt_client = CostManagementClient(credential=credential, subscription_id=subscription_id)

    def list_billing_accounts(self):
        billing_accounts_info = []

        billing_account_name = self.billing_account_id
        billing_accounts = self.billing_client.customers.list_by_billing_account(
            billing_account_name=billing_account_name)
        for billing_account in billing_accounts:
            billing_accounts_info.append({
                'customer_id': billing_account.name
            })

        return billing_accounts_info

    def get_billing_account(self):
        billing_account_name = self.billing_account_id
        billing_account_info = self.billing_client.billing_accounts.get(billing_account_name=billing_account_name)
        return billing_account_info

    def query(self, customer_id, start, end):
        billing_account_id = self.billing_account_id
        scope = f'providers/Microsoft.Billing/billingAccounts/{billing_account_id}/customers/{customer_id}'
        parameters = {
            'type': TYPE,
            'timeframe': TIMEFRAME,
            'timePeriod': {
                'from': start.isoformat(),
                'to': end.isoformat()
            },
            'dataset': {
                'granularity': GRANULARITY,
                'aggregation': AGGREGATION,
                'grouping': GROUPING
            }
        }
        return self.cost_mgmt_client.query.usage(scope=scope, parameters=parameters)

    def query_http(self, scope, secret_data, parameters, **kwargs):
        try:
            api_version = '2023-03-01'
            self.next_link = f'https://management.azure.com/{scope}/providers/Microsoft.CostManagement/query?api-version={api_version}'

            while self.next_link:
                url = self.next_link

                headers = self._make_request_headers(client_type=secret_data.get('client_id'))
                response = requests.post(url=url, headers=headers, json=parameters)
                response_json = response.json()

                if response_json.get('error'):
                    response_json = self._retry_request(response=response, url=url, headers=headers,
                                                        json=parameters, retry_count=RETRY_COUNT, method='post', **kwargs)

                self.next_link = response_json.get('properties').get('nextLink', None)
                yield response_json
        except Exception as e:
            raise ERROR_UNKNOWN(message=f'[ERROR] query_http {e}')

    def list_by_billing_account(self):
        billing_account_name = self.billing_account_id
        return self.billing_client.billing_subscriptions.list_by_billing_account(billing_account_name=billing_account_name)

    def _make_request_headers(self, client_type=None):
        access_token = self._get_access_token()
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
        }
        if client_type:
            headers['ClientType'] = client_type

        return headers

    def _retry_request(self, response, url, headers, json, retry_count, method='post', **kwargs):
        try:
            print(f'{datetime.utcnow()}[INFO] retry_request {response.headers}')
            if retry_count == 0:
                raise ERROR_UNKNOWN(message=f'[ERROR] retry_request failed {response.json()}')
            elif response.status_code == 400:
                raise ERROR_UNKNOWN(message=f'[ERROR] retry_request failed {response.json()}')

            _sleep_time = self._get_sleep_time(response.headers)
            time.sleep(_sleep_time)

            if method == 'post':
                response = requests.post(url=url, headers=headers, json=json)
            else:
                response = requests.get(url=url, headers=headers, json=json)
            response_json = response.json()

            if response_json.get('error'):
                response_json = self._retry_request(response=response, url=url, headers=headers,
                                                    json=json, retry_count=retry_count - 1, method=method)
            return response_json
        except Exception as e:
            _LOGGER.error(f'[ERROR] retry_request failed {e}')
            raise e

    def _get_latest_api_version(self, secret_data, scope):
        try:
            url = f'https://management.azure.com/{scope}/providers/Microsoft.CostManagement/query?api-version=""'
            headers = self._make_request_headers(secret_data)
            response = requests.post(url=url, headers=headers)

            response_json = response.json()
            if error_json := response_json.get('error'):
                error_msg = error_json.get('message', '')
                api_versions = re.findall(r"'([^\']+)'", error_msg.split('.')[2].strip().strip("'")+"'")[0].split(',')
                for api_version in reversed(api_versions):
                    if 'preview' not in api_version:
                        return api_version

        except Exception as e:
            _LOGGER.error(f'[ERROR] _get_latest_api_version {e}')
            raise e

    @staticmethod
    def _get_sleep_time(response_headers):
        sleep_time = 0
        for key, value in response_headers.items():
            if 'retry' in key.lower():
                if isinstance(value, str) is False:
                    _retry_time = 0
                _retry_time = int(value)
                sleep_time = max(sleep_time, _retry_time)
        return sleep_time + 1

    @staticmethod
    def _get_access_token():
        try:
            credential = DefaultAzureCredential(logging_enable=True)
            scopes = ["https://management.azure.com/.default"]
            token_info = credential.get_token(*scopes)
            return token_info.token
        except Exception as e:
            _LOGGER.error(f'[ERROR] _get_access_token :{e}')
            raise ERROR_INVALID_TOKEN(token=e)

    @staticmethod
    def _check_secret_data(secret_data):
        if 'billing_account_id' not in secret_data and 'subscription_id' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.billing_account_id or secret_data.subscription_id')

        if 'tenant_id' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.tenant_id')

        if 'client_id' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.client_id')

        if 'client_secret' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.client_secret')