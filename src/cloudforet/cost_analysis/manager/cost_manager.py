import logging

from datetime import datetime, timedelta, timezone
from spaceone.core.error import *
from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.connector.azure_cost_mgmt_connector import AzureCostMgmtConnector
from cloudforet.cost_analysis.conf.cost_conf import *

_LOGGER = logging.getLogger(__name__)


class CostManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.azure_cm_connector: AzureCostMgmtConnector = self.locator.get_connector('AzureCostMgmtConnector')

    def get_data(self, options, secret_data, schema, task_options):
        self.azure_cm_connector.create_session(options, secret_data, schema)
        self._check_task_options(task_options)

        # task unit is tenant(customer_id) not a subscription
        customer_id = task_options['account_id']
        start = self._convert_date_format_to_utc(task_options['start'])
        end = datetime.utcnow().replace(tzinfo=timezone.utc)
        parameters = PARAMETERS
        parameters.update({
            'timePeriod': {
                'from': start,
                'to': end
            },
        })

        response_stream = self.azure_cm_connector.get_cost_and_usage_http(secret_data, customer_id, start, end)

        while response_stream is not None:
            yield self._make_cost_data(response_stream, customer_id)
            if next_link := response_stream['properties'].get('nextLink'):
                response_stream = self.azure_cm_connector.get_cost_and_usage_http(secret_data, customer_id, start, end, next_link)
            else:
                break

        yield []

    def _make_cost_data(self, results, customer_id):
        costs_data = []

        try:
            combined_results = self._combine_rows_and_columns_from_results(results.get('properties').get('rows'), results.get('properties').get('columns'))

            for result in combined_results:
                cost = self._convert_str_to_float_format(result.get('Cost', 0))
                usd_cost = self._convert_str_to_float_format(result.get('CostUSD', 0))
                currency = result.get('Currency', 'USD')
                usage_quantity = self._convert_str_to_float_format(result.get('UsageQuantity', 0))
                usage_type = result.get('Meter', '')
                usage_unit = result.get('UnitOfMeasure', '')
                subscription_id = result.get('SubscriptionId', '')
                region_code = result.get('ResourceLocation', '')
                product = result.get('MeterCategory', '')
                resource = result.get('ResourceId', '')
                billed_at = self._set_billed_at(result.get('UsageDate'))
                additional_info = self._get_additional_info(result, customer_id)

                if not billed_at:
                    continue

                if currency == 'USD':
                    usd_cost = cost

                data = {
                    'cost': cost,
                    'usd_cost': usd_cost,
                    'currency': currency,
                    'usage_quantity': usage_quantity,
                    'usage_type': usage_type,
                    'usage_unit': usage_unit,
                    'provider': 'azure',
                    'region_code': REGION_MAP.get(region_code, region_code),
                    'account': subscription_id,
                    'product': product,
                    'resource': resource,
                    'billed_at': billed_at,
                    'additional_info': additional_info,
                    'tags': {}
                }
                costs_data.append(data)

        except Exception as e:
            _LOGGER.error(f'[_make_cost_data] make data error: {e}', exc_info=True)
            raise e

        return costs_data

    @staticmethod
    def _combine_rows_and_columns_from_results(rows, columns):
        combined_results = []
        for row in rows:
            temp_result = {}
            for idx, column in enumerate(columns):
                temp_result.update({column.get('name'): row[idx]})
            combined_results.append(temp_result)

        return combined_results

    @staticmethod
    def _convert_str_to_float_format(num_str: str):
        return format(float(num_str), 'f')

    @staticmethod
    def _set_billed_at(start: int):
        try:
            start = str(start)
            formatted_start = f"{start[:4]}-{start[4:6]}-{start[6:]}"
            return datetime.strptime(formatted_start, "%Y-%m-%d")
        except Exception as e:
            _LOGGER.error(f'[_set_billed_at] set billed_at error: {e}', exc_info=True)
            return None

    @staticmethod
    def _get_additional_info(result, customer_id):
        additional_info = {'Azure Tenant ID': customer_id}
        meter_category = result.get('MeterCategory', '')

        if 'SubscriptionId' in result:
            additional_info['Azure Subscription ID'] = result['SubscriptionId']

        if 'ResourceGroup' in result:
            additional_info['Azure Resource Group'] = result['ResourceGroup']

        if 'ResourceType' in result:
            additional_info['Azure Resource Type'] = result['ResourceType']

        if meter_category == 'Virtual Machines' and 'Meter' in result:
            additional_info['Azure Instance Type'] = result['Meter']

        return additional_info

    @staticmethod
    def _convert_date_format_to_utc(date_format):
        return datetime.strptime(date_format, '%Y-%m-%d').replace(tzinfo=timezone.utc)

    @staticmethod
    def _check_task_options(task_options):
        if 'account_id' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.account_id')

        if 'start' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.start')
