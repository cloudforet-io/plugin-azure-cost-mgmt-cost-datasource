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
        billing_accounts = self.billing_client.customers.list_by_billing_account(
            billing_account_name=billing_account_name)
        for billing_account in billing_accounts:
            billing_accounts_info.append({
                'customer_id': billing_account.name
            })

        return billing_accounts_info

    def get_cost_and_usage(self, customer_id, start, end):
        billing_account_name = self.billing_account_name
        scope = f'providers/Microsoft.Billing/billingAccounts/{billing_account_name}/customers/{customer_id}'
        parameters = {
            'type': TYPE,
            'timeframe': TIMEFRAME,
            'timePeriod': {
                'from': start.isoformat(),
                'to': end.isoformat()
            },
            'dataset': {
                'granularity': GRANULARITY,
                'aggregation': dict(AGGREGATION_USAGE_QUANTITY, **AGGREGATION_COST),
                'grouping': GROUPING
            }
        }
        return self.cost_mgmt_client.query.usage(scope=scope, parameters=parameters)

    def get_cost_and_usage_http(self, secret_data, customer_id, start, end):
        billing_account_name = self.billing_account_name
        api_version = '2022-10-01'
        url = f'https://management.azure.com/providers/Microsoft.Billing/billingAccounts/{billing_account_name}/customers/{customer_id}/providers/Microsoft.CostManagement/query?api-version={api_version}'


        parameters = self._make_parameters(start, end)
        headers = self._make_request_headers(secret_data)
        response = requests.post(url=url, headers=headers, json=parameters)
        response_json = response.json()
        if response_json.get('error'):
            response_json = self._retry_request(response=response, url=url, headers=headers,
                                                json=parameters, retry_count=RETRY_COUNT, method='post')
        return response_json

    def get_usd_cost_and_tag_http(self, secret_data, customer_id, start, end, next_link=None):
        try:
            billing_account_name = self.billing_account_name
            api_version = '2022-10-01'
            url = f'https://management.azure.com/providers/Microsoft.Billing/billingAccounts/{billing_account_name}/customers/{customer_id}/providers/Microsoft.CostManagement/query?api-version={api_version}'

            if next_link:
                url = next_link

            options = {
                'aggregation': 'usd_cost',
            }

            parameters = self._make_parameters(start, end, options)
            headers = self._make_request_headers(secret_data)
            response = requests.post(url=url, headers=headers, json=parameters)
            response_json = response.json()

            if response_json.get('error'):
                response_json = self._retry_request(response=response, url=url, headers=headers,
                                                    json=parameters, retry_count=RETRY_COUNT, method='post')
            return response_json
        except Exception as e:
            raise ERROR_UNKNOWN(message=f'[ERROR] get_usd_cost_and_tag_http {e}')

    def _make_request_headers(self, secret_data):
        access_token = self._get_access_token(secret_data)
        return {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

    def _retry_request(self, response, url, headers, json, retry_count, method='post'):
        try:
            if retry_count == 0:
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

    @staticmethod
    def _make_parameters(start, end, options=None):
        parameters = {}
        aggregation = AGGREGATION_USAGE_QUANTITY
        grouping = GROUPING

        if options.get('aggregation') == 'usd_cost':
            aggregation = dict(aggregation, **AGGREGATION_USD_COST)
        else:
            aggregation = dict(aggregation, **AGGREGATION_COST)

        if options.get('grouping') == 'tag':
            grouping = grouping + [GROUPING_TAG_OPTION]

        if options.get('granularity'):
            parameters.update({'dataset': {'granularity': options.get('granularity')}})

        parameters.update({
            'type': TYPE,
            'timeframe': TIMEFRAME,
            'timePeriod': {
                'from': start.isoformat(),
                'to': end.isoformat()
            },
            'dataset': {
                'aggregation': aggregation,
                'grouping': grouping
            }
        })

        return parameters

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

    # def _make_cost_data(self, secret_data, results, customer_id, start, next_link=None):
    #     costs_data = []
    #     last_billed_at = ''
    #
    #     try:
    #         combined_results = self._combine_rows_and_columns_from_results(results.get('properties').get('rows'),
    #                                                                        results.get('properties').get('columns'))
    #         for idx, cb_result in enumerate(combined_results):
    #             if idx > 0 and cb_result.get('Tag') != '':
    #                 if self._check_prev_and_current_result(combined_results[idx-1], cb_result):
    #                     costs_data[-1]['tags'].update(self._convert_tag_str_to_dict(cb_result.get('Tag')))
    #                     continue
    #
    #             billed_at = self._set_billed_at(cb_result.get('UsageDate'))
    #             if not billed_at:
    #                 continue
    #
    #             data = self._make_data_info(cb_result, billed_at, customer_id)
    #             costs_data.append(data)
    #             last_billed_at = billed_at.replace(hour=0, minute=0, second=0)
    #
    #         if next_link:
    #             costs_data = self._remove_cost_data_start_from_last_billed_at(costs_data, last_billed_at)
    #
    #         _end = self._set_end_date(last_billed_at, next_link)
    #         response_stream = self.azure_cm_connector.get_cost_and_usage_http(secret_data, customer_id, start, _end)
    #         costs_data = self._combine_make_data(costs_data=costs_data, results=response_stream)
    #
    #     except Exception as e:
    #         _LOGGER.error(f'[_make_cost_data] make data error: {e}', exc_info=True)
    #         raise e
    #
    #     return costs_data