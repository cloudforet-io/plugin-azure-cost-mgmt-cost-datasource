import logging
import pandas as pd
import time

from datetime import datetime, timedelta, timezone
from spaceone.core.error import *
from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.connector.azure_cost_mgmt_connector import AzureCostMgmtConnector
from cloudforet.cost_analysis.conf.cost_conf import *

_LOGGER = logging.getLogger(__name__)


class CostManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.azure_cm_connector: AzureCostMgmtConnector = self.locator.get_connector('AzureCostMgmtConnector')

    def get_data(self, options, secret_data, schema, task_options):
        self.azure_cm_connector.create_session(options, secret_data, schema)
        self._check_task_options(task_options)

        collect_scope = task_options['collect_scope']
        account_agreement_type = task_options.get('account_agreement_type')
        customer_tenants = self._get_customer_tenant_id(task_options, collect_scope)
        start = self._convert_date_format_to_utc(task_options['start'])
        end = datetime.utcnow().replace(tzinfo=timezone.utc)

        monthly_time_period = self._make_monthly_time_period(start, end)
        for time_period in monthly_time_period:
            _start = time_period['start']
            _end = time_period['end']
            parameters = self._make_parameters(_start, _end, account_agreement_type, options)
            start_time = time.time()
            print(f"{datetime.utcnow()} [INFO][get_data] all tenant is start to collect data from {_start} to {_end}")
            for idx, customer_tenant_id in enumerate(customer_tenants):
                scope = self._make_scope(secret_data, task_options, collect_scope, customer_tenant_id)

                for response_stream in self.azure_cm_connector.query_http(scope, secret_data, parameters):
                    yield self._make_cost_data(results=response_stream, end=_end, tenant_id=customer_tenant_id)
                end_time = time.time()
                print(f"{datetime.utcnow()} [INFO][get_data] #{idx+1} {customer_tenant_id} tenant collect is done")
            print(f"{datetime.utcnow()} [INFO][get_data] all collect is done in {int(end_time - start_time)} seconds")

        yield []

    def _make_cost_data(self, results, end, tenant_id=None):
        """ Source Data Model
        class QueryResult Class
            id: str,
            name: str,
            type: str,
            location: str,
            sku: str,
            eTag: str,
            tags: dict,
            properties: {
                nextLink: str,
                columns: list,
                rows: list
            }
        """

        costs_data = []
        try:
            combined_results = self._combine_rows_and_columns_from_results(results.get('properties').get('rows'),
                                                                           results.get('properties').get('columns'))
            for cb_result in combined_results:
                billed_date = self._set_billed_at(cb_result.get('UsageDate', end))
                if not billed_date:
                    continue

                data = self._make_data_info(cb_result, billed_date, tenant_id)
                costs_data.append(data)

        except Exception as e:
            _LOGGER.error(f'[_make_cost_data] make data error: {e}', exc_info=True)
            raise e

        return costs_data

    def _make_data_info(self, result, billed_date, tenant_id=None):
        additional_info = self._get_additional_info(result, tenant_id)
        cost = self._convert_str_to_float_format(result.get('Cost', 0))
        usage_quantity = self._convert_str_to_float_format(result.get('UsageQuantity', 0))
        usage_type = result.get('Meter', '')
        usage_unit = result.get('UnitOfMeasure', '')
        region_code = result.get('ResourceLocation', '').lower()
        product = result.get('MeterCategory', '')
        tags = {}  # self._convert_tag_str_to_dict(result.get('Tag'))

        data = {
            'cost': cost,
            'usage_quantity': usage_quantity,
            'usage_type': usage_type,
            'usage_unit': usage_unit,
            'provider': 'azure',
            'region_code': REGION_MAP.get(region_code, region_code),
            'product': product,
            'tags': tags,
            'billed_date': billed_date,
            'additional_info': additional_info,
        }

        return data

    def _get_additional_info(self, result, tenant_id=None):
        additional_info = {}

        meter_category = result.get('MeterCategory', '')
        tenant_id = result.get('CustomerTenantId') if result.get('CustomerTenantId') else tenant_id

        additional_info['Tenant ID'] = tenant_id
        additional_info['Currency'] = result.get('Currency', 'USD')
        additional_info['Subscription Id'] = result.get('SubscriptionId', 'Shared')

        if meter_category == 'Virtual Machines' and 'Meter' in result:
            additional_info['Instance Type'] = result['Meter']

        if result.get('ResourceLocation') != '' and result.get('ResourceGroup'):
            additional_info['Resource Group'] = result['ResourceGroup']

        if result.get('ResourceType') != '' and result.get('ResourceType'):
            additional_info['Resource Type'] = result['ResourceType']

        if result.get('SubscriptionName') != '' and result.get('SubscriptionName'):
            additional_info['Subscription Name'] = result['SubscriptionName']

        if result.get('PricingModel') != '' and result.get('PricingModel'):
            additional_info['Pricing Model'] = result['PricingModel']

        if result.get('BenefitName') != '' and result.get('BenefitName'):
            benefit_name = result['BenefitName']
            additional_info['Benefit Name'] = benefit_name

            if result.get('PricingModel') == 'Reservation' and result['MeterCategory'] == '':
                result['MeterCategory'] = self._set_product_from_benefit_name(benefit_name)

        if result.get('MeterSubcategory') != '' and result.get('MeterSubcategory'):
            additional_info['Meter SubCategory'] = result.get('MeterSubcategory')
            if result.get('PricingModel') == 'OnDemand' and result.get('MeterCategory') == '':
                result['MeterCategory'] = result.get('MeterSubcategory')

        if result.get('DepartmentName') != '' and result.get('DepartmentName'):
            additional_info['Department Name'] = result['DepartmentName']

        if result.get('EnrollmentAccountName') != '' and result.get('EnrollmentAccountName'):
            additional_info['Enrollment Account Name'] = result['EnrollmentAccountName']

        if result.get('ResourceId') != '' and result.get('ResourceId'):
            additional_info['Resource ID'] = result['ResourceId']
            additional_info['Resource Name'] = result['ResourceId'].split('/')[-1]

        return additional_info

    def _combine_make_data(self, costs_data, results):
        try:
            costs_data_without_tag = []
            combined_results = self._combine_rows_and_columns_from_results(results.get('properties').get('rows'),
                                                                           results.get('properties').get('columns'))
            for cb_result in combined_results:
                billed_date = self._set_billed_at(cb_result.get('UsageDate'))
                if not billed_date:
                    continue

                data = self._make_data_info(cb_result, billed_date)
                costs_data_without_tag.append(data)

            for idx, cost_data in enumerate(costs_data):
                if self._check_prev_and_current_result(cost_data, costs_data_without_tag[idx]):
                    cost_data.update({'usd_cost': costs_data_without_tag[idx]['usd_cost']})
            return costs_data
        except Exception as e:
            _LOGGER.error(f'[_combine_make_data] make data error: {e}', exc_info=True)
            raise e

    @staticmethod
    def _get_customer_tenant_id(task_options, collect_scope):
        customer_tenants = []
        if 'tenant_id' in task_options:
            customer_tenants.append(task_options['tenant_id'])
        elif collect_scope == 'customer_tenant_id':
            customer_tenants.extend(task_options['customer_tenants'])
        else:
            customer_tenants.append('EA Agreement')
        return customer_tenants

    @staticmethod
    def _make_scope(secret_data, task_options, collect_scope, customer_tenant_id=None):
        if collect_scope == 'subscription_id':
            subscription_id = task_options['subscription_id']
            scope = SCOPE_MAP[collect_scope].format(subscription_id=subscription_id)
        elif collect_scope == 'customer_tenant_id':
            billing_account_id = secret_data.get('billing_account_id')
            scope = SCOPE_MAP[collect_scope].format(billing_account_id=billing_account_id,
                                                    customer_tenant_id=customer_tenant_id)
        else:
            billing_account_id = secret_data.get('billing_account_id')
            scope = SCOPE_MAP[collect_scope].format(billing_account_id=billing_account_id)
        return scope

    @staticmethod
    def _make_parameters(start, end, account_agreement_type, options=None):
        parameters = {}
        aggregation = AGGREGATION
        grouping = GROUPING

        if account_agreement_type == 'EnterpriseAgreement':
            grouping = [dimension for dimension in grouping if dimension['name'] != 'UnitOfMeasure']
            grouping.extend(GROUPING_EA_AGREEMENT_OPTION)

        parameters.update({
            'type': TYPE,
            'timeframe': TIMEFRAME,
            'timePeriod': {
                'from': start.isoformat(),
                'to': end.isoformat()
            },
            'dataset': {
                'aggregation': aggregation,
                'grouping': grouping,
                'granularity': options.get('granularity', GRANULARITY),
            }
        })

        return parameters

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
    def _set_product_from_benefit_name(benefit_name):
        _product_name_format = 'Reserved {_product_name}'
        product_name = _product_name_format.format(_product_name=benefit_name)

        try:
            if 'VM' in benefit_name.upper():
                product_name = _product_name_format.format(_product_name='VM Instances')
            elif 'REDIS' in benefit_name.upper():
                product_name = _product_name_format.format(_product_name='Redis Cache')
            elif 'DISK' in benefit_name.upper():
                product_name = _product_name_format.format(_product_name='Disk')
            elif 'BLOB' in benefit_name.upper():
                product_name = _product_name_format.format(_product_name='Blob Storage Capacity')
            elif 'FILE' in benefit_name.upper():
                product_name = _product_name_format.format(_product_name='File Capacity')
            elif len(benefit_name.split("_")) > 1:
                product_name = _product_name_format.format(_product_name=benefit_name.split("_")[0])

            return product_name
        except Exception as e:
            return product_name

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
    def _set_billed_at(start):
        try:
            if isinstance(start, int):
                start = str(start)
                formatted_start = datetime.strptime(start, "%Y%m%d")
            elif isinstance(start, datetime):
                return start
            else:
                formatted_start = start

            return datetime.strftime(formatted_start, "%Y-%m-%d")
        except Exception as e:
            _LOGGER.error(f'[_set_billed_at] set billed_at error: {e}', exc_info=True)
            return None

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

    def _make_monthly_time_period(self, start_date, end_date):
        monthly_time_period = []
        current_date = datetime.utcnow().strftime('%Y-%m-%d')

        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')

        start_year = start_date.year
        start_month = start_date.month
        end_year = end_date.year
        end_month = end_date.month

        for year in range(start_year, end_year + 1):
            start = start_month if year == start_year else 1
            end = end_month if year == end_year else 12

            for month in range(start, end + 1):
                first_date_of_month = datetime(year, month, 1).strftime('%Y-%m-%d')
                if month == 12:
                    last_date_of_month = (datetime(year + 1, 1, 1) - timedelta(days=1)).strftime('%Y-%m-%d')
                else:
                    last_date_of_month = (datetime(year, month + 1, 1) - timedelta(days=1)).strftime('%Y-%m-%d')
                if last_date_of_month > current_date:
                    last_date_of_month = current_date
                monthly_time_period.append({'start': self._convert_date_format_to_utc(first_date_of_month),
                                            'end': self._convert_date_format_to_utc(last_date_of_month)})
        return monthly_time_period

    @staticmethod
    def _check_task_options(task_options):
        if 'collect_scope' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.collect_scope')

        if 'start' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.start')

        if task_options['collect_scope'] == 'subscription_id':
            if 'subscription_id' not in task_options:
                raise ERROR_REQUIRED_PARAMETER(key='task_options.subscription_id')
        elif task_options['collect_scope'] == 'customer_tenants':
            raise ERROR_REQUIRED_PARAMETER(key='task_options.customer_tenants')

