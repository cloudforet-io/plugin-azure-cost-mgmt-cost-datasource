import logging
import pandas as pd

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

        response_stream = self.azure_cm_connector.get_usd_cost_and_tag_http(secret_data, customer_id, start, end)
        while len(response_stream.get('properties', {}).get('rows', [])) > 0:
            next_link = response_stream.get('properties', {}).get('nextLink', None)
            yield self._make_cost_data(secret_data=secret_data, results=response_stream, customer_id=customer_id
                                       , start=start, next_link=next_link)

            if next_link:
                start = self._set_billed_at(response_stream.get('properties').get('rows')[-1][2])
                response_stream = self.azure_cm_connector.get_usd_cost_and_tag_http(secret_data, customer_id, start, end)
            else:
                break
        yield []

    def _make_cost_data(self, secret_data, results, customer_id, start, next_link=None):
        costs_data = []
        last_billed_at = ''

        try:
            combined_results = self._combine_rows_and_columns_from_results(results.get('properties').get('rows'),
                                                                           results.get('properties').get('columns'))
            for idx, cb_result in enumerate(combined_results):
                if idx > 0 and cb_result.get('Tag') != '':
                    if self._check_prev_and_current_result(combined_results[idx-1], cb_result):
                        costs_data[-1]['tags'].update(self._convert_tag_str_to_dict(cb_result.get('Tag')))
                        continue

                billed_at = self._set_billed_at(cb_result.get('UsageDate'))
                if not billed_at:
                    continue

                data = self._make_data_info(cb_result, billed_at, customer_id)
                costs_data.append(data)
                last_billed_at = billed_at.replace(hour=0, minute=0, second=0)

            if next_link:
                costs_data = self._remove_cost_data_start_from_last_billed_at(costs_data, last_billed_at)

            _end = self._set_end_date(last_billed_at, next_link)
            response_stream = self.azure_cm_connector.get_cost_and_usage_http(secret_data, customer_id, start, _end)
            costs_data = self._combine_make_data(costs_data=costs_data, results=response_stream)

        except Exception as e:
            _LOGGER.error(f'[_make_cost_data] make data error: {e}', exc_info=True)
            raise e

        return costs_data

    def _combine_make_data(self, costs_data, results):
        try:
            costs_data_without_tag = []
            combined_results = self._combine_rows_and_columns_from_results(results.get('properties').get('rows'),
                                                                           results.get('properties').get('columns'))
            for cb_result in combined_results:
                billed_at = self._set_billed_at(cb_result.get('UsageDate'))
                if not billed_at:
                    continue

                data = self._make_data_info(cb_result, billed_at)
                costs_data_without_tag.append(data)

            for idx, cost_data in enumerate(costs_data):
                if self._check_prev_and_current_result(cost_data, costs_data_without_tag[idx]):
                    cost_data.update({'usd_cost': costs_data_without_tag[idx]['usd_cost']})
            return costs_data
        except Exception as e:
            _LOGGER.error(f'[_combine_make_data] make data error: {e}', exc_info=True)
            raise e

    def _make_data_info(self, result, billed_at, customer_id=None):
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
        tags = self._convert_tag_str_to_dict(result.get('Tag'))
        additional_info = self._get_additional_info(result, customer_id)

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
            'tags': tags,
            'billed_at': billed_at,
            'additional_info': additional_info,
        }

        return data

    @staticmethod
    def _set_end_date(last_billed_at, next_link=None):
        if next_link:
            return last_billed_at - timedelta(seconds=1)
        else:
            return last_billed_at.replace(hour=23, minute=59, second=59)

    @staticmethod
    def _convert_tag_str_to_dict(tag: str):
        if tag is None:
            return {}

        tag_dict = {}
        if ":" in tag:
            tag = tag.split(':')
            _key = tag[0]
            _value = tag[1]
            tag_dict[_key] = _value
        elif tag:
            tag_dict[tag] = ''
        return tag_dict

    @staticmethod
    def _remove_cost_data_start_from_last_billed_at(costs_data, last_billed_at):
        return [cost_data for cost_data in costs_data if cost_data.get('billed_at') < last_billed_at]

    @staticmethod
    def _combine_rows_and_columns_from_results(rows, columns):
        _columns = [column.get('name') for column in columns]
        return pd.DataFrame(data=rows, columns=_columns).to_dict(orient='records')

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
        additional_info = {}
        meter_category = result.get('MeterCategory', '')

        if customer_id:
            additional_info = {'Azure Tenant ID': customer_id}

        if 'ResourceGroup' in result:
            additional_info['Azure Resource Group'] = result['ResourceGroup']

        if 'ResourceType' in result:
            additional_info['Azure Resource Type'] = result['ResourceType']

        if 'SubscriptionName' in result:
            additional_info['Azure Subscription Name'] = result['SubscriptionName']

        if meter_category == 'Virtual Machines' and 'Meter' in result:
            additional_info['Azure Instance Type'] = result['Meter']

        return additional_info

    @staticmethod
    def _check_prev_and_current_result(prev_result, cur_result):
        if cur_result.get('UsageDate') != prev_result.get('UsageDate'):
            return False
        if cur_result.get('Meter') != prev_result.get('Meter'):
            return False
        if cur_result.get('MeterCategory') != prev_result.get('MeterCategory'):
            return False
        if cur_result.get('ResourceId') != prev_result.get('ResourceId'):
            return False

        return True

    @staticmethod
    def _convert_date_format_to_utc(date_format: str):
        return datetime.strptime(date_format, '%Y-%m-%d').replace(tzinfo=timezone.utc)

    @staticmethod
    def _check_task_options(task_options):
        if 'account_id' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.account_id')

        if 'start' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.start')
