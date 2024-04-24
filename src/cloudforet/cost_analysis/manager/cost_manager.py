import calendar
import logging
import json
import time

from typing import Union
from datetime import datetime, timezone
from spaceone.core.error import *
from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.connector.azure_cost_mgmt_connector import (
    AzureCostMgmtConnector,
)
from cloudforet.cost_analysis.conf.cost_conf import *

_LOGGER = logging.getLogger(__name__)


class CostManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.azure_cm_connector: AzureCostMgmtConnector = self.locator.get_connector(
            "AzureCostMgmtConnector"
        )

    def get_linked_accounts(
        self, options: dict, secret_data: dict, domain_id: str, schema
    ) -> list:
        self.azure_cm_connector.create_session(options, secret_data, schema)
        billing_account_info = self.azure_cm_connector.get_billing_account()
        agreement_type = billing_account_info.get("agreement_type")
        accounts_info = []

        if agreement_type == "MicrosoftPartnerAgreement":
            billing_accounts_info = self.azure_cm_connector.list_billing_accounts()
            customer_tenants = self._get_linked_customer_tenants(
                secret_data, billing_accounts_info
            )
            accounts_info = self._make_accounts_info_from_customer_tenants(
                billing_accounts_info, customer_tenants
            )
        elif agreement_type == "EnterpriseAgreement":
            pass
        elif agreement_type == "MicrosoftCustomerAgreement":
            pass
        else:
            pass
        _LOGGER.debug(
            f"[get_linked_accounts] total accounts count: {len(accounts_info)}"
        )
        return accounts_info

    def get_data(
        self, options: dict, secret_data: dict, schema, task_options: dict
    ) -> list:
        self.azure_cm_connector.create_session(options, secret_data, schema)
        self._check_task_options(task_options)

        collect_scope: str = task_options["collect_scope"]
        tenant_ids: list = self._get_tenant_ids(task_options, collect_scope)
        start: datetime = self._get_first_date_of_month(task_options["start"])
        end: datetime = datetime.utcnow()

        monthly_time_period = self._make_monthly_time_period(start, end)
        for time_period in monthly_time_period:
            _start = time_period["start"]
            _end = time_period["end"]
            parameters = self._make_parameters(_start, _end, options)

            start_time = time.time()
            _LOGGER.info(
                f"[get_data] {tenant_ids} start to collect data from {_start} to {_end}"
            )
            for idx, tenant_id in enumerate(tenant_ids):
                _LOGGER.info(
                    f"[get_data] #{idx + 1} {tenant_id} tenant start to collect data from {_start} to {_end}"
                )
                scope = self._make_scope(
                    secret_data, task_options, collect_scope, tenant_id
                )
                blobs = self.azure_cm_connector.begin_create_operation(
                    scope, parameters
                )

                response_stream = self.azure_cm_connector.get_cost_data(blobs)
                for results in response_stream:
                    yield self._make_cost_data(
                        results=results, end=_end, tenant_id=tenant_id, options=options
                    )
                _LOGGER.info(
                    f"[get_data] #{idx + 1} {tenant_id} tenant collect is done"
                )
            end_time = time.time()
            _LOGGER.info(
                f"[get_data] all collect is done in {int(end_time - start_time)} seconds"
            )
        yield []

    def _make_cost_data(
        self, results: list, end: datetime, options: dict, tenant_id: str = None
    ):
        """Source Data Model"""

        costs_data = []
        try:
            for result in results:
                result = {key.lower(): value for key, value in result.items()}

                billed_date = self._set_billed_date(result.get("date", end))
                if not billed_date:
                    continue

                data = self._make_data_info(result, billed_date, options, tenant_id)
                costs_data.append(data)

        except Exception as e:
            _LOGGER.error(f"[_make_cost_data] make data error: {e}", exc_info=True)
            raise e

        return costs_data

    def _make_data_info(
        self, result: dict, billed_date: str, options: dict, tenant_id: str = None
    ):
        additional_info = self._get_additional_info(result, options, tenant_id)
        cost = self._get_cost_from_result_with_options(result, options)
        usage_quantity = self._convert_str_to_float_format(result.get("quantity", 0.0))
        usage_type = result.get("metername", "")
        usage_unit = str(result.get("unitofmeasure", ""))
        region_code = self._get_region_code(result.get("resourcelocation", ""))
        product = result.get("metercategory", "")
        tags = self._convert_tags_str_to_dict(result.get("tags"))

        aggregate_data = self._get_aggregate_data(result, options)

        data = {
            "cost": cost,
            "usage_quantity": usage_quantity,
            "usage_type": usage_type,
            "usage_unit": usage_unit,
            "provider": "azure",
            "region_code": REGION_MAP.get(region_code, region_code),
            "product": product,
            "tags": tags,
            "billed_date": billed_date,
            "data": aggregate_data,
            "additional_info": additional_info,
        }

        return data

    def _get_additional_info(self, result: dict, options: dict, tenant_id: str = None):
        additional_info = {}

        meter_category = result.get("metercategory", "")
        tenant_id = (
            result.get("customertenantid")
            if result.get("customertenantid")
            else tenant_id
        )

        additional_info["Tenant Id"] = tenant_id
        additional_info["Subscription Id"] = result.get("subscriptionid", "Shared")

        if meter_category == "Virtual Machines" and "Meter" in result:
            additional_info["Instance Type"] = result["metername"]

        if result.get("resourcegroupname") != "" and result.get("resourcegroupname"):
            additional_info["Resource Group"] = result["resourcegroupname"]
        elif result.get("resourcegroup") != "" and result.get("resourcegroup"):
            additional_info["Resource Group"] = result["resourcegroup"]

        if result.get("subscriptionname") != "" and result.get("subscriptionname"):
            additional_info["Subscription Name"] = result["subscriptionname"]

        if result.get("pricingmodel") != "" and result.get("pricingmodel"):
            additional_info["Pricing Model"] = result["pricingmodel"]

        if result.get("benefitname") != "" and result.get("benefitname"):
            benefit_name = result["benefitname"]
            additional_info["Benefit Name"] = benefit_name

            if (
                result.get("pricingmodel") == "Reservation"
                and result["metercategory"] == ""
            ):
                result["metercategory"] = self._set_product_from_benefit_name(
                    benefit_name
                )

        if result.get("metersubcategory") != "" and result.get("metersubcategory"):
            additional_info["Meter SubCategory"] = result.get("metersubcategory")
            if (
                result.get("pricingmodel") == "OnDemand"
                and result.get("metercategory") == ""
            ):
                result["metercategory"] = result.get("metercategory")

        if result.get("customername") is None:
            if result.get("invoicesectionname") != "" and result.get(
                "invoicesectionname"
            ):
                additional_info["Department Name"] = result.get("invoicesectionname")
            elif result.get("departmentname") != "" and result.get("departmentname"):
                additional_info["Department Name"] = result["departmentname"]

        if result.get("accountname") != "" and result.get("accountname"):
            additional_info["Enrollment Account Name"] = result["accountname"]
        elif result.get("enrollmentaccountname") != "" and result.get(
            "enrollmentaccountname"
        ):
            additional_info["Enrollment Account Name"] = result["enrollmentaccountname"]

        collect_resource_id = options.get("collect_resource_id", False)
        if (
            collect_resource_id
            and result.get("resourceid") != ""
            and result.get("resourceid")
        ):
            additional_info["Resource Id"] = result["resourceid"]
            additional_info["Resource Name"] = result["resourceid"].split("/")[-1]

        if result.get("productname"):
            additional_info["Product Name"] = result["productname"]

        if result.get("unitprice") != "" and result.get("unitprice"):
            additional_info["Unit Price"] = result["unitprice"]

        if result.get("customername") != "" and result.get("customername"):
            additional_info["Customer Name"] = result["customername"]

        if result.get("servicefamily") != "" and result.get("servicefamily"):
            additional_info["Service Family"] = result["servicefamily"]

        if result.get("metername") != "" and result.get("metername"):
            additional_info["Meter Name"] = result["metername"]

        if result.get("term") != "" and result.get("term"):
            additional_info["Term"] = result["term"]

        return additional_info

    def _get_cost_from_result_with_options(self, result: dict, options: dict) -> float:
        cost = self._convert_str_to_float_format(
            result.get("costinbillingcurrency", 0.0)
        )
        if options.get("pay_as_you_go", False) and result.get("chargetype") == "Usage":
            cost = self.get_pay_as_you_go_cost(result, cost)

        return cost

    def get_pay_as_you_go_cost(self, result: dict, cost: float = 0.0) -> float:
        if pay_g_billing_price := result.get("paygcostinbillingcurrency"):
            cost_pay_as_you_go = pay_g_billing_price
        elif pay_g_price := result.get("paygprice", 0.0):
            usage_quantity = self._convert_str_to_float_format(
                result.get("quantity", 0.0)
            )
            exchange_rate = result.get("exchangeratepricingtobilling", 1.0) or 1.0
            cost_pay_as_you_go = pay_g_price * usage_quantity * exchange_rate
        else:
            cost_pay_as_you_go = cost
        return cost_pay_as_you_go

    def _get_aggregate_data(self, result: dict, options: dict) -> dict:
        aggregate_data = {}

        if not options.get("pay_as_you_go", False):
            pay_as_you_go_cost = self.get_pay_as_you_go_cost(result)
            aggregate_data.update({"PayAsYouGo": pay_as_you_go_cost})

        return aggregate_data

    @staticmethod
    def _get_region_code(resource_location: str) -> str:
        return resource_location.lower() if resource_location else resource_location

    @staticmethod
    def _make_parameters(start: datetime, end: datetime, options: dict) -> dict:
        parameters = {"timePeriod": {"start": start, "end": end}}

        if options.get("cost_metric") == "AmortizedCost":
            parameters["metric"] = "AmortizedCost"
        else:
            parameters["metric"] = "ActualCost"

        return parameters

    @staticmethod
    def _get_tenant_ids(task_options: dict, collect_scope: str) -> list:
        tenant_ids = []
        if "tenant_id" in task_options:
            tenant_ids.append(task_options["tenant_id"])
        elif collect_scope == "customer_tenant_id":
            tenant_ids.extend(task_options["customer_tenants"])
        else:
            tenant_ids.append("EA Agreement")
        return tenant_ids

    @staticmethod
    def _make_scope(
        secret_data: dict,
        task_options: dict,
        collect_scope: str,
        customer_tenant_id: str = None,
    ):
        if collect_scope == "subscription_id":
            subscription_id = task_options["subscription_id"]
            scope = SCOPE_MAP[collect_scope].format(subscription_id=subscription_id)
        elif collect_scope == "customer_tenant_id":
            billing_account_id = secret_data.get("billing_account_id")
            scope = SCOPE_MAP[collect_scope].format(
                billing_account_id=billing_account_id,
                customer_tenant_id=customer_tenant_id,
            )
        else:
            billing_account_id = secret_data.get("billing_account_id")
            scope = SCOPE_MAP[collect_scope].format(
                billing_account_id=billing_account_id
            )
        return scope

    @staticmethod
    def _convert_tags_str_to_dict(tags_str: Union[str, None]) -> dict:
        try:
            if tags_str is None:
                return {}

            if tags_str[0] != "{" and tags_str[:-1] != "}":
                tags_str = "{" + tags_str + "}"

            tags = json.loads(tags_str)
            return tags
        except Exception as e:
            _LOGGER.error(f"[_convert_tags_str_to_dict] tags : {tags_str} {e}")
            return {}

    @staticmethod
    def _set_product_from_benefit_name(benefit_name):
        _product_name_format = "Reserved {product_name}"
        product_name = _product_name_format.format(product_name=benefit_name)

        try:
            if "VM" in benefit_name.upper():
                product_name = _product_name_format.format(product_name="VM Instances")
            elif "REDIS" in benefit_name.upper():
                product_name = _product_name_format.format(product_name="Redis Cache")
            elif "DISK" in benefit_name.upper():
                product_name = _product_name_format.format(product_name="Disk")
            elif "BLOB" in benefit_name.upper():
                product_name = _product_name_format.format(
                    product_name="Blob Storage Capacity"
                )
            elif "FILE" in benefit_name.upper():
                product_name = _product_name_format.format(product_name="File Capacity")
            elif len(benefit_name.split("_")) > 1:
                product_name = _product_name_format.format(
                    product_name=benefit_name.split("_")[0]
                )

            return product_name
        except Exception as e:
            return product_name

    @staticmethod
    def _convert_str_to_float_format(num_str: Union[str, float, None]) -> float:
        if isinstance(num_str, str):
            return float(str(num_str))
        elif num_str is None:
            return 0.0
        else:
            return num_str

    @staticmethod
    def _set_billed_date(start: Union[str, int, datetime]):
        try:
            if isinstance(start, int):
                start = str(start)
                formatted_start = datetime.strptime(start, "%Y%m%d")
            elif isinstance(start, datetime):
                return start.strftime("%Y-%m-%d")
            elif len(start.split("/")) == 3:
                formatted_start = datetime.strptime(start, "%m/%d/%Y")
            else:
                formatted_start = start

            return datetime.strftime(formatted_start, "%Y-%m-%d")
        except Exception as e:
            _LOGGER.error(f"[_set_billed_at] set billed_at error: {e}", exc_info=True)
            return None

    @staticmethod
    def _get_first_date_of_month(start_month: str) -> datetime:
        return datetime.strptime(start_month, "%Y-%m").replace(day=1)

    @staticmethod
    def _get_last_date_of_month(year: int, month: int) -> datetime:
        last_day = calendar.monthrange(year, month)[1]
        return datetime(year, month, last_day)

    @staticmethod
    def _convert_date_format_to_utc(date_format: str) -> datetime:
        return datetime.strptime(date_format, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    def _make_monthly_time_period(
        self, start_date: datetime, end_date: datetime
    ) -> list:
        monthly_time_period = []
        current_date = end_date

        start_year = start_date.year
        start_month = start_date.month
        end_year = end_date.year
        end_month = end_date.month

        for year in range(start_year, end_year + 1):
            start = start_month if year == start_year else 1
            end = end_month if year == end_year else 12

            for month in range(start, end + 1):
                first_date_of_month = self._get_first_date_of_month(f"{year}-{month}")
                last_date_of_month = self._get_last_date_of_month(year, month)

                if last_date_of_month > current_date:
                    last_date_of_month = current_date
                monthly_time_period.append(
                    {
                        "start": first_date_of_month,
                        "end": last_date_of_month,
                    }
                )
        return monthly_time_period

    @staticmethod
    def _get_linked_customer_tenants(
        secret_data: dict, billing_accounts_info: list
    ) -> list:
        customer_tenants = secret_data.get("customer_tenants", [])
        if not customer_tenants:
            customer_tenants = [
                billing_account.get("customer_id")
                for billing_account in billing_accounts_info
            ]

        return customer_tenants

    @staticmethod
    def _make_accounts_info_from_customer_tenants(
        billing_accounts_info: list, customer_tenants: list
    ) -> list:
        accounts_info = []
        for billing_account_info in billing_accounts_info:
            if billing_account_info.get("customer_id") in customer_tenants:
                account_info = {
                    "account_id": billing_account_info.get("customer_id"),
                    "name": billing_account_info.get("display_name"),
                }
                accounts_info.append(account_info)
        return accounts_info

    @staticmethod
    def _check_task_options(task_options):
        if "collect_scope" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.collect_scope")

        if "start" not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key="task_options.start")

        if task_options["collect_scope"] == "subscription_id":
            if "subscription_id" not in task_options:
                raise ERROR_REQUIRED_PARAMETER(key="task_options.subscription_id")
        elif task_options["collect_scope"] == "customer_tenants":
            raise ERROR_REQUIRED_PARAMETER(key="task_options.customer_tenants")
