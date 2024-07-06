import logging
import os
import requests
import pandas as pd
import numpy as np
from io import StringIO

from azure.identity import DefaultAzureCredential
from azure.mgmt.billing import BillingManagementClient
from azure.mgmt.costmanagement import CostManagementClient
from azure.core.exceptions import ResourceNotFoundError
from spaceone.core.connector import BaseConnector

from cloudforet.cost_analysis.error.cost import *

__all__ = ["AzureCostMgmtConnector"]

_LOGGER = logging.getLogger(__name__)

_PAGE_SIZE = 6000


class AzureCostMgmtConnector(BaseConnector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.billing_client = None
        self.cost_mgmt_client = None
        self.billing_account_id = None
        self.next_link = None

    def create_session(self, options: dict, secret_data: dict, schema: str) -> None:
        self._check_secret_data(secret_data)

        subscription_id = secret_data.get("subscription_id", "")

        os.environ["AZURE_SUBSCRIPTION_ID"] = subscription_id
        os.environ["AZURE_TENANT_ID"] = secret_data["tenant_id"]
        os.environ["AZURE_CLIENT_ID"] = secret_data["client_id"]
        os.environ["AZURE_CLIENT_SECRET"] = secret_data["client_secret"]

        credential = DefaultAzureCredential()

        self.billing_account_id = secret_data.get("billing_account_id")
        self.billing_client = BillingManagementClient(
            credential=credential, subscription_id=subscription_id
        )
        self.cost_mgmt_client = CostManagementClient(
            credential=credential, subscription_id=subscription_id
        )

    def list_billing_accounts(self) -> list:
        billing_accounts_info = []

        billing_account_name = self.billing_account_id
        billing_accounts = self.billing_client.customers.list_by_billing_account(
            billing_account_name=billing_account_name
        )
        for billing_account in billing_accounts:
            billing_accounts_info.append(
                {
                    "display_name": billing_account.display_name,
                    "customer_id": billing_account.name,
                }
            )

        return billing_accounts_info

    def get_billing_account(self) -> dict:
        billing_account_name = self.billing_account_id
        billing_account_info = self.billing_client.billing_accounts.get(
            billing_account_name=billing_account_name
        )
        billing_account_info = self.convert_nested_dictionary(billing_account_info)
        return billing_account_info

    def begin_create_operation(self, scope: str, parameters: dict) -> list:
        try:
            content_type = "application/json"
            response = self.cost_mgmt_client.generate_cost_details_report.begin_create_operation(
                scope=scope, parameters=parameters, content_type=content_type
            )
            result = self.convert_nested_dictionary(response.result())
            _LOGGER.info(
                f"[begin_create_operation] result : {result} status : {response.status()}"
            )

            blobs = result.get("blobs", []) or []
            _LOGGER.debug(
                f"[begin_create_operation] csv_file_link: {blobs} / parameters: {parameters}"
            )
            return blobs
        except ResourceNotFoundError as e:
            _LOGGER.error(
                f"[begin_create_operation] Cost data is not ready: {e} / parameters: {parameters}"
            )
            return []
        except Exception as e:
            _LOGGER.error(
                f"[begin_create_operation] error message: {e} / parameters: {parameters}"
            )
            raise ERROR_UNKNOWN(message=f"[ERROR] begin_create_operation failed")

    def get_cost_data(self, blobs: list, options: dict) -> list:
        _LOGGER.debug(f"[get_cost_data] options: {options}")

        total_cost_count = 0
        for blob in blobs:
            cost_csv = self._download_cost_data(blob)

            df_chunk = pd.read_csv(
                StringIO(cost_csv), low_memory=False, chunksize=_PAGE_SIZE
            )

            for df in df_chunk:
                df = df.replace({np.nan: None})

                costs_data = df.to_dict("records")
                total_cost_count += len(costs_data)
                yield costs_data
            del df_chunk
            del cost_csv
        _LOGGER.debug(f"[get_cost_data] total_cost_count: {total_cost_count}")

    def list_by_billing_account(self):
        billing_account_name = self.billing_account_id
        return self.billing_client.billing_subscriptions.list_by_billing_account(
            billing_account_name=billing_account_name
        )

    def _make_request_headers(self, client_type=None):
        access_token = self._get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        if client_type:
            headers["ClientType"] = client_type

        return headers

    def convert_nested_dictionary(self, cloud_svc_object):
        cloud_svc_dict = {}
        if hasattr(
            cloud_svc_object, "__dict__"
        ):  # if cloud_svc_object is not a dictionary type but has dict method
            cloud_svc_dict = cloud_svc_object.__dict__
        elif isinstance(cloud_svc_object, dict):
            cloud_svc_dict = cloud_svc_object
        elif not isinstance(
            cloud_svc_object, list
        ):  # if cloud_svc_object is one of type like int, float, char, ...
            return cloud_svc_object

        # if cloud_svc_object is dictionary type
        for key, value in cloud_svc_dict.items():
            if hasattr(value, "__dict__") or isinstance(value, dict):
                cloud_svc_dict[key] = self.convert_nested_dictionary(value)
            if "azure" in str(type(value)):
                cloud_svc_dict[key] = self.convert_nested_dictionary(value)
            elif isinstance(value, list):
                value_list = []
                for v in value:
                    value_list.append(self.convert_nested_dictionary(v))
                cloud_svc_dict[key] = value_list

        return cloud_svc_dict

    @staticmethod
    def _download_cost_data(blob: dict) -> str:
        try:
            response = requests.get(blob.get("blob_link"))
            if response.status_code != 200:
                raise ERROR_CONNECTOR_CALL_API(reason=f"{response.reason}")
            return response.text
        except Exception as e:
            _LOGGER.error(f"[_download_cost_data] download error: {e}", exc_info=True)
            raise e

    @staticmethod
    def _get_sleep_time(response_headers):
        sleep_time = 0
        for key, value in response_headers.items():
            if "retry" in key.lower():
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
            _LOGGER.error(f"[ERROR] _get_access_token :{e}")
            raise ERROR_INVALID_TOKEN(token=e)

    @staticmethod
    def _check_secret_data(secret_data):
        if (
            "billing_account_id" not in secret_data
            and "subscription_id" not in secret_data
        ):
            raise ERROR_REQUIRED_PARAMETER(
                key="secret_data.billing_account_id or secret_data.subscription_id"
            )

        if "tenant_id" not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key="secret_data.tenant_id")

        if "client_id" not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key="secret_data.client_id")

        if "client_secret" not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key="secret_data.client_secret")
