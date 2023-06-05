import logging
import os
import requests
import time

from cloudforet.cost_analysis.conf.cost_conf import *
from spaceone.core import utils
from spaceone.core.connector import BaseConnector
from spaceone.core.error import *
from azure.identity import DefaultAzureCredential
from azure.mgmt.billing import BillingManagementClient
from azure.mgmt.costmanagement import CostManagementClient

__all__ = ['AzureCostMgmtConnector']

_LOGGER = logging.getLogger(__name__)


class AzureCostMgmtConnector(BaseConnector):

    def __init_(self, *args, **kwargs):
        super().__init_(*args, **kwargs)
        self.session = None
        self.billing_client = None
        self.cost_mgmt_client = None
        self.billing_account_name = None

    def create_session(self, options: dict, secret_data: dict, schema: str):
        self._check_secret_data(secret_data)

        subscription_id = secret_data.get('subscription_id', '')

        os.environ["AZURE_SUBSCRIPTION_ID"] = subscription_id
        os.environ["AZURE_TENANT_ID"] = secret_data['tenant_id']
        os.environ["AZURE_CLIENT_ID"] = secret_data['client_id']
        os.environ["AZURE_CLIENT_SECRET"] = secret_data['client_secret']

        credential = DefaultAzureCredential()

        self.billing_account_name = secret_data['billing_account_name']
        self.billing_client = BillingManagementClient(credential=credential, subscription_id=subscription_id)
        self.cost_mgmt_client = CostManagementClient(credential=credential, subscription_id=subscription_id)

    def list_billing_accounts(self):
        billing_accounts_info = []

        billing_account_name = self.billing_account_name
        billing_accounts = self.billing_client.customers.list_by_billing_account(billing_account_name=billing_account_name)
        for billing_account in billing_accounts:
            billing_accounts_info.append({
                'customer_id': billing_account.name
            })

        return billing_accounts_info

    def get_cost_and_usage(self, customer_id, start, end):
        billing_account_name = self.billing_account_name
        scope = f'providers/Microsoft.Billing/billingAccounts/{billing_account_name}/customers/{customer_id}'
        parameters = PARAMETERS
        parameters.update({
            'time_period': {
                'from': start,
                'to': end
            }
        })
        return self.cost_mgmt_client.query.usage(scope=scope, parameters=parameters)

    def get_cost_and_usage_http(self, secret_data, customer_id, start, end, next_link=None):
        billing_account_name = self.billing_account_name
        api_version = '2022-10-01'
        url = f'https://management.azure.com/providers/Microsoft.Billing/billingAccounts/{billing_account_name}/customers/{customer_id}/providers/Microsoft.CostManagement/query?api-version={api_version}'
        if next_link:
            url = next_link

        parameters = PARAMETERS
        parameters.update({
            'timePeriod': {
                'from': start.isoformat(),
                'to': end.isoformat()
            }
        })

        header = self._make_request_header(secret_data)

        # for request limit
        time.sleep(2)
        response = requests.post(url=url, headers=header, json=parameters)
        return response.json()

    def get_usd_cost_and_tag_http(self, secret_data, customer_id, start, end, next_link=None):
        billing_account_name = self.billing_account_name
        api_version = '2022-10-01'
        url = f'https://management.azure.com/providers/Microsoft.Billing/billingAccounts/{billing_account_name}/customers/{customer_id}/providers/Microsoft.CostManagement/query?api-version={api_version}'
        if next_link:
            url = next_link

        parameters = PARAMETERS_WITH_USD_AND_TAG
        parameters.update({
            'timePeriod': {
                'from': start.isoformat(),
                'to': end.isoformat()
            }
        })

        header = self._make_request_header(secret_data)

        # for request limit
        time.sleep(2)
        response = requests.post(url=url, headers=header, json=parameters)
        return response.json()


    def _make_request_header(self, secret_data):
        access_token = self._get_access_token(secret_data)
        return {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

    @staticmethod
    def _get_access_token(secret_data):
        tenant_id = secret_data.get('tenant_id', '')

        get_token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
        get_token_data = {
            'client_id': secret_data.get('client_id', ''),
            'client_secret': secret_data.get('client_secret', ''),
            'grant_type': 'client_credentials',
            'resource': 'https://management.azure.com/'
        }
        get_token_response = requests.post(url=get_token_url, data=get_token_data)
        return get_token_response.json()['access_token']

    @staticmethod
    def _check_secret_data(secret_data):
        if 'billing_account_name' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.billing_account_name')

        if 'tenant_id' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.tenant_id')

        if 'client_id' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.client_id')

        if 'client_secret' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.client_secret')
