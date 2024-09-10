import logging
import math
from datetime import datetime, timedelta
from typing import Tuple, Union

from dateutil.relativedelta import relativedelta

from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.conf.cost_conf import SECRET_TYPE_DEFAULT
from cloudforet.cost_analysis.error.cost import *

_LOGGER = logging.getLogger("spaceone")

_TASK_LIST_SIZE = 5


class JobManager(BaseManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.azure_cm_connector = self.locator.get_connector("AzureCostMgmtConnector")

    def get_tasks(
        self,
        options: dict,
        secret_data: dict,
        linked_accounts: list,
        schema,
        start: str,
        last_synchronized_at: datetime,
        domain_id: str,
    ) -> dict:
        start_month = self._get_start_month(start, last_synchronized_at)

        self.azure_cm_connector.create_session(options, secret_data, schema)
        secret_type = options.get("secret_type", SECRET_TYPE_DEFAULT)

        if secret_type == "MANUAL":
            billing_account_info = self.azure_cm_connector.get_billing_account()
            billing_account_agreement_type = (
                self.azure_cm_connector.get_agreement_type_from_billing_account_info(
                    billing_account_info
                )
            )

            if billing_account_agreement_type == "MicrosoftPartnerAgreement":
                tasks = []
                changed = []
                synced_accounts = []

                # Only for MicrosoftPartnerAgreement
                if options.get("collect_scope") == "billing_account_id":
                    start = datetime.strptime(start_month, "%Y-%m")
                    end = datetime.utcnow()

                    month_range = relativedelta(end, start).months
                    month_range_step = math.ceil((month_range + 1) / _TASK_LIST_SIZE)

                    for month in range(
                        0,
                        month_range + 1,
                        math.ceil((month_range + 1) / _TASK_LIST_SIZE),
                    ):
                        task_start_month = datetime.strftime(
                            start + relativedelta(months=month), "%Y-%m"
                        )
                        task_end_month = datetime.strftime(
                            start + relativedelta(months=month + month_range_step - 1),
                            "%Y-%m",
                        )
                        tasks.append(
                            {
                                "task_options": {
                                    "start": task_start_month,
                                    "end": task_end_month,
                                    "account_agreement_type": billing_account_agreement_type,
                                    "collect_scope": "billing_account_id",
                                    "billing_tenant_id": secret_data["tenant_id"],
                                }
                            }
                        )
                    if linked_accounts:
                        synced_accounts = linked_accounts
                    changed.append({"start": start_month})
                else:
                    # divide customer tenants for each task
                    customer_tenants, first_sync_tenants = self._get_customer_tenants(
                        secret_data, linked_accounts
                    )

                    if len(customer_tenants) == 0 and len(first_sync_tenants) > 0:
                        customer_tenants.extend(first_sync_tenants)
                        first_sync_tenants = []

                    divided_customer_tenants = self._get_divided_customer_tenants(
                        customer_tenants
                    )

                    for divided_customer_tenant_info in divided_customer_tenants:
                        tasks.append(
                            {
                                "task_options": {
                                    "start": start_month,
                                    "account_agreement_type": billing_account_agreement_type,
                                    "collect_scope": "customer_tenant_id",
                                    "customer_tenants": divided_customer_tenant_info,
                                    "billing_tenant_id": secret_data["tenant_id"],
                                }
                            }
                        )
                        if linked_accounts:
                            synced_accounts = self._extend_synced_accounts(
                                synced_accounts, divided_customer_tenant_info
                            )
                    changed.append({"start": start_month})
                    if first_sync_tenants:
                        first_sync_start_month = self._get_start_month(start=None)
                        tasks.append(
                            {
                                "task_options": {
                                    "start": first_sync_start_month,
                                    "account_agreement_type": billing_account_agreement_type,
                                    "collect_scope": "customer_tenant_id",
                                    "customer_tenants": first_sync_tenants,
                                    "billing_tenant_id": secret_data["tenant_id"],
                                    "is_sync": False,
                                }
                            }
                        )
                        for tenant_id in first_sync_tenants:
                            changed.append(
                                {
                                    "start": first_sync_start_month,
                                    "filter": {"additional_info.Tenant Id": tenant_id},
                                }
                            )
                        if linked_accounts:
                            synced_accounts = self._extend_synced_accounts(
                                synced_accounts, first_sync_tenants
                            )
            else:
                tasks = [
                    {
                        "task_options": {
                            "start": start_month,
                            "account_agreement_type": billing_account_agreement_type,
                            "collect_scope": "billing_account_id",
                            "billing_tenant_id": secret_data["tenant_id"],
                            "include_credit_cost": options.get(
                                "include_credit_cost", False
                            ),
                        }
                    }
                ]
                changed = [{"start": start_month}]
                synced_accounts = []

            # Benefit Job Task
            if options.get("cost_metric") == "AmortizedCost":
                tasks.append(
                    {
                        "task_options": {
                            "start": start_month,
                            "account_agreement_type": billing_account_agreement_type,
                            "collect_scope": "billing_account_id",
                            "billing_tenant_id": secret_data["tenant_id"],
                            "is_benefit_job": True,
                        }
                    }
                )

        elif secret_type == "USE_SERVICE_ACCOUNT_SECRET":
            subscription_id = secret_data.get("subscription_id", "")
            tenant_id = secret_data.get("tenant_id")
            tasks = [
                {
                    "task_options": {
                        "collect_scope": "subscription_id",
                        "start": start_month,
                        "subscription_id": subscription_id,
                        "billing_tenant_id": tenant_id,
                    }
                }
            ]
            changed = [{"start": start_month}]
            synced_accounts = []

        else:
            raise ERROR_INVALID_SECRET_TYPE(secret_type=options.get("secret_type"))

        tasks = {"tasks": tasks, "changed": changed, "synced_accounts": synced_accounts}
        return tasks

    def _get_tenants_from_billing_account(self):
        tenants = []
        for (
            billing_account
        ) in self.azure_cm_connector.list_customers_by_billing_account():
            tenants.append(billing_account["customer_id"])
        return tenants

    def _get_start_month(
        self, start: Union[str, None], last_synchronized_at: datetime = None
    ) -> str:
        if start:
            start_time: datetime = self._parse_start_time(start)
        elif last_synchronized_at:
            start_time: datetime = last_synchronized_at - timedelta(days=7)
            start_time = start_time.replace(day=1)
        else:
            start_time: datetime = datetime.utcnow() - relativedelta(months=9)
            start_time = start_time.replace(day=1)

        start_time = start_time.replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )

        return start_time.strftime("%Y-%m")

    def _get_customer_tenants(
        self, secret_data: dict, linked_accounts: list = None
    ) -> Tuple[list, list]:
        first_sync_customer_tenants = []
        customer_tenants = secret_data.get(
            "customer_tenants", self._get_tenants_from_billing_account()
        )
        if len(customer_tenants) == 0:
            raise ERROR_EMPTY_CUSTOMER_TENANTS(customer_tenants=customer_tenants)

        # if linked_accounts:
        #     linked_accounts_map = {
        #         linked_account["account_id"]: linked_account
        #         for linked_account in linked_accounts
        #     }
        #
        #     for customer_tenant_id in customer_tenants:
        #         if linked_account_info := linked_accounts_map.get(customer_tenant_id):
        #             if not linked_account_info.get("is_sync"):
        #                 first_sync_customer_tenants.append(
        #                     linked_account_info.get("account_id")
        #                 )
        #                 customer_tenants.remove(customer_tenant_id)
        #         else:
        #             _LOGGER.debug(
        #                 f"[_get_customer_tenants] Customer tenant is not linked: {linked_account_info}"
        #             )
        #             customer_tenants.remove(customer_tenant_id)

        return customer_tenants, first_sync_customer_tenants

    @staticmethod
    def _extend_synced_accounts(synced_accounts: list, customer_tenants: list) -> list:
        synced_accounts.extend(
            {"account_id": tenant_id} for tenant_id in customer_tenants
        )
        return synced_accounts

    @staticmethod
    def _get_divided_customer_tenants(customer_tenants_info: list) -> list:
        tenant_size = math.ceil(len(customer_tenants_info) / _TASK_LIST_SIZE)
        divided_customer_tenants_info = [
            customer_tenants_info[idx : idx + tenant_size]
            for idx in range(0, len(customer_tenants_info), tenant_size)
        ]
        return divided_customer_tenants_info

    @staticmethod
    def _parse_start_time(start_time: str) -> datetime:
        date_format = "%Y-%m"

        try:
            return datetime.strptime(start_time, date_format)
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key="start", type=date_format)
