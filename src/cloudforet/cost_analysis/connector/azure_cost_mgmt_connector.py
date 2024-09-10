import logging
import os
import tempfile
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from functools import wraps
from io import BytesIO
from typing import get_type_hints, Union, Any

from azure.identity import DefaultAzureCredential
from azure.mgmt.billing import BillingManagementClient
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.consumption import ConsumptionManagementClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from spaceone.core.connector import BaseConnector

from cloudforet.cost_analysis.error.cost import *
from cloudforet.cost_analysis.conf.cost_conf import *

__all__ = ["AzureCostMgmtConnector"]

_LOGGER = logging.getLogger("spaceone")

_PAGE_SIZE = 7000


def azure_exception_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs) -> Union[dict, list]:
        return_type = get_type_hints(func).get("return")
        try:
            return func(*args, **kwargs)
        except ResourceNotFoundError as error:
            _print_error_log(error)
            return _get_empty_value(return_type)
        except HttpResponseError as error:
            if error.status_code in ["404", "412"]:
                _print_error_log(error)
            else:
                _print_error_log(error)
            return _get_empty_value(return_type)
        except Exception as e:
            _print_error_log(ERROR_UNKNOWN(message=str(e)))
            raise e

    return wrapper


def _get_empty_value(return_type: object) -> Any:
    return_type_name = getattr(return_type, "__name__")
    empty_values = {
        "int": 0,
        "float": 0.0,
        "str": "",
        "bool": False,
        "list": [],
        "dict": {},
        "set": set(),
        "tuple": (),
    }

    return empty_values.get(return_type_name, None)


def _print_error_log(error):
    _LOGGER.error(f"(Error) => {error.message} {error}", exc_info=True)


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
        self.consumption_client = ConsumptionManagementClient(
            credential=credential, subscription_id=subscription_id
        )

    def list_customers_by_billing_account(self) -> list:
        billing_accounts_info = []

        billing_accounts = self.billing_client.customers.list_by_billing_account(
            billing_account_name=self.billing_account_id
        )
        for billing_account in billing_accounts:
            billing_account_info = self.convert_nested_dictionary(billing_account)
            billing_account_properties_info = billing_account_info.get("properties", {})
            billing_accounts_info.append(
                {
                    "display_name": billing_account_properties_info.get("display_name"),
                    "customer_id": billing_account_info.get("name"),
                }
            )

        return billing_accounts_info

    def query_usage_http(
        self,
        secret_data: dict,
        start: datetime,
        end: datetime,
        account_agreement_type: str,
        options=None,
    ):
        try:
            billing_account_id = secret_data["billing_account_id"]
            api_version = "2023-11-01"
            self.next_link = f"https://management.azure.com/providers/Microsoft.Billing/billingAccounts/{billing_account_id}/providers/Microsoft.CostManagement/query?api-version={api_version}"

            parameters = {
                "type": TYPE,
                "timeframe": TIMEFRAME,
                "timePeriod": {"from": start.isoformat(), "to": end.isoformat()},
                "dataset": {
                    "granularity": GRANULARITY,
                    "aggregation": AGGREGATION,
                    "grouping": BENEFIT_GROUPING,
                    "filter": BENEFIT_FILTER,
                },
            }
            if account_agreement_type == "MicrosoftPartnerAgreement":
                parameters["dataset"]["grouping"] = (
                    BENEFIT_GROUPING + BENEFIT_GROUPING_MPA
                )
            elif account_agreement_type == "EnterpriseAgreement":
                parameters["dataset"]["grouping"] = (
                    BENEFIT_GROUPING + BENEFIT_GROUPING_EA
                )
            else:
                parameters["dataset"]["grouping"] = (
                    BENEFIT_GROUPING + BENEFIT_GROUPING_MCA
                )

            _LOGGER.debug(f"[query_usage] parameters: {parameters}")

            while self.next_link:
                url = self.next_link
                headers = self._make_request_headers()

                _LOGGER.debug(f"[query_usage] url:{url}, parameters: {parameters}")
                response = requests.post(url=url, headers=headers, json=parameters)
                response_json = response.json()

                if response_json.get("error"):
                    response_json = self._retry_request(
                        response=response,
                        url=url,
                        headers=headers,
                        json=parameters,
                        retry_count=RETRY_COUNT,
                        method="post",
                    )

                self.next_link = response_json.get("properties").get("nextLink", None)
                yield response_json
        except Exception as e:
            _LOGGER.error(f"[ERROR] query_usage_http {e}", exc_info=True)
            raise ERROR_UNKNOWN(message=f"[ERROR] query_usage_http {e}")

    def get_billing_account(self) -> dict:
        billing_account_name = self.billing_account_id
        # todo : remove api_version
        billing_account_info = self.billing_client.billing_accounts.get(
            billing_account_name=billing_account_name, api_version="2020-05-01"
        )
        billing_account_info = self.convert_nested_dictionary(billing_account_info)
        return billing_account_info

    @staticmethod
    def get_agreement_type_from_billing_account_info(
        billing_account_info: dict,
    ) -> Union[str, None]:
        billing_account_properties_info = billing_account_info.get("properties")
        billing_account_agreement_type = billing_account_properties_info.get(
            "agreement_type", None
        )
        return billing_account_agreement_type

    @azure_exception_handler
    def begin_create_operation(self, scope: str, parameters: dict) -> list:
        content_type = "application/json"
        response = (
            self.cost_mgmt_client.generate_cost_details_report.begin_create_operation(
                scope=scope, parameters=parameters, content_type=content_type
            )
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

    def list_by_billing_account(self):
        return self.billing_client.billing_subscriptions.list_by_billing_account(
            billing_account_name=self.billing_account_id
        )

    @staticmethod
    def get_retail_price(meter_id: str, currency: str = "USD"):
        url = f"https://prices.azure.com/api/retail/prices?currencyCode={currency}&$filter=priceType eq 'Consumption' and meterId eq '{meter_id}'"
        try:
            response = requests.get(url=url)
            return response.json()
        except Exception as e:
            _LOGGER.error(f"[ERROR] get_retail_price {e}")
            raise ERROR_UNKNOWN(message=f"[ERROR] get_retail_price failed {e}")

    def get_credit_data(
        self, billing_period_name: str, account_agreement_type: str
    ) -> dict:
        if account_agreement_type == "MicrosoftPartnerAgreement":
            credit_info = {}
        elif account_agreement_type == "EnterpriseAgreement":
            response = self.consumption_client.balances.get_for_billing_period_by_billing_account(
                billing_account_id=self.billing_account_id,
                billing_period_name=billing_period_name,
            )
            credit_info = self.convert_nested_dictionary(response)
        else:
            credit_info = {}
        return credit_info

    def get_cost_data(self, blobs: list, options: dict) -> list:
        _LOGGER.debug(f"[get_cost_data] options: {options}")
        total_cost_count = 0
        for blob in blobs:
            with tempfile.TemporaryFile() as temp_file:
                self._download_cost_data(blob, temp_file)

                df_chunk = pd.read_csv(
                    BytesIO(temp_file.read()),
                    low_memory=False,
                    chunksize=_PAGE_SIZE,
                )

                for df in df_chunk:
                    df = df.replace({np.nan: None})

                    costs_data = df.to_dict("records")
                    total_cost_count += len(costs_data)
                    yield costs_data
                del df_chunk
        _LOGGER.debug(f"[get_cost_data] total_cost_count: {total_cost_count}")

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

    def _make_request_headers(self, client_type=None):
        access_token = self._get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        if client_type:
            headers["ClientType"] = client_type

        return headers

    def _retry_request(self, response, url, headers, json, retry_count, method="post"):
        try:
            _LOGGER.error(f"{datetime.utcnow()}[INFO] retry_request {response.headers}")
            if retry_count == 0:
                raise ERROR_UNKNOWN(
                    message=f"[ERROR] retry_request failed {response.json()}"
                )

            _sleep_time = self._get_sleep_time(response.headers)
            time.sleep(_sleep_time)

            if method == "post":
                response = requests.post(url=url, headers=headers, json=json)
            else:
                response = requests.get(url=url, headers=headers, json=json)
            response_json = response.json()

            if response_json.get("error"):
                response_json = self._retry_request(
                    response=response,
                    url=url,
                    headers=headers,
                    json=json,
                    retry_count=retry_count - 1,
                    method=method,
                )
            return response_json
        except Exception as e:
            _LOGGER.error(f"[ERROR] retry_request failed {e}")
            raise e

    @staticmethod
    def _download_cost_data(blob: dict, temp_file) -> None:
        try:
            with requests.get(blob.get("blob_link"), stream=True) as response:
                response.raise_for_status()

                for chunk in response.iter_content(chunk_size=_PAGE_SIZE):
                    temp_file.write(chunk)
            temp_file.seek(0)

        except Exception as e:
            _LOGGER.error(f"[_download_cost_data] download error: {e}", exc_info=True)
            raise e

    @staticmethod
    def _get_sleep_time(response_headers):
        sleep_time = 30
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
