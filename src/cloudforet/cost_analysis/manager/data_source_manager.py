import copy
import logging

from spaceone.core.error import ERROR_INVALID_ARGUMENT
from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.connector.azure_cost_mgmt_connector import (
    AzureCostMgmtConnector,
)

_LOGGER = logging.getLogger("spaceone")

_DEFAULT_DATA_SOURCE_RULES = [
    {
        "name": "match_service_account",
        "conditions_policy": "ALWAYS",
        "actions": {
            "match_service_account": {
                "source": "additional_info.Subscription Id",
                "target": "data.subscription_id",
            }
        },
        "options": {"stop_processing": True},
    }
]

_DEFAULT_METADATA_ADDITIONAL_INFO = {
    "Tenant Id": {"name": "Tenant Id", "visible": False},
    "Subscription Name": {"name": "Subscription Name", "visible": True},
    "Subscription Id": {"name": "Subscription Id", "visible": False},
    "Resource Group": {"name": "Resource Group", "visible": True},
    "Resource Name": {"name": "Resource Name", "visible": True},
    "Resource Id": {"name": "Resource Id", "visible": False},
    "Charge Type": {
        "name": "Charge Type",
        "visible": True,
        "enums": [
            "Usage",
            "Purchase",
            "UnusedReservation",
            "UnusedSavingsPlan",
            "Refund",
            "RoundAdjustment",
        ],
    },
    "Pricing Model": {
        "name": "Pricing Model",
        "visible": True,
        "enums": ["OnDemand", "Reservation", "SavingsPlan", "Spot"],
    },
    "Benefit Name": {"name": "Benefit Name", "visible": False},
    "Benefit Id": {"name": "Benefit Id", "visible": False},
    "Frequency": {"name": "Frequency", "visible": False},
    "Instance Type": {"name": "Instance Type", "visible": True},
    "Meter Id": {"name": "Meter Id", "visible": False},
    "Meter Name": {"name": "Meter Name", "visible": False},
    "Meter SubCategory": {"name": "Meter SubCategory", "visible": False},
    "Product Name": {"name": "Product Name", "visible": True},
    "PayG Unit Price": {"name": "PayG Unit Price", "visible": False},
    "Reservation Id": {"name": "Reservation Id", "visible": False},
    "Reservation Name": {"name": "Reservation Name", "visible": False},
    "Service Family": {"name": "Service Family", "visible": True},
    "Term": {"name": "Term", "visible": False},
    "Usage Type Details": {"name": "Usage Type Details", "visible": True},
    "Exchange Rate": {"name": "Exchange Rate", "visible": False},
    "Billing Tenant Id": {"name": "Billing Tenant Id", "visible": True},
    "Adjustment Name": {"name": "Adjustment Name", "visible": False},
    "Product Id": {"name": "Product Id", "visible": False},
    "RI Normalization Ratio": {"name": "RI Normalization Ratio", "visible": False},
}

_METADATA_INFO_ADDITIONAL_INFO_MPA = {
    "Customer Name": {"name": "Customer Name", "visible": True}
}

_METADATA_INFO_ADDITIONAL_INFO_EA = {
    "Department Name": {"name": "Department Name", "visible": True},
    "Enrollment Account Name": {"name": "Enrollment Account Name", "visible": True},
}


class DataSourceManager(BaseManager):
    @staticmethod
    def init_response(options: dict, domain_id: str) -> dict:
        """
        Returns:
            {
                "metadata":{
                    "data_source_rules(list)": [],
                    "supported_secret_types(list)": [],
                    "currency(str)": "KRW",
                    "use_account_routing(bool)": False,
                    "exclude_license_cost(bool)": False,
                    "include_credit_cost(bool)": False,
                    "include_reservation_cost_at_payg(bool)": False,
                    "cost_info(dict)": {
                        "name" :"PayAsYouGo",
                        "unit" :"KRW"
                    },
                    "data_info(dict)": {
                        "Actual Cost": {
                            "name": "Actual Cost",
                            "unit": "KRW"
                        }
                    },
                    "additional_info(dict)": {
                        "Subscription Name": {
                            "name": "Subscription Name",
                            "visible": True,
                            "enums": ["","",...]
                        }
                    }
                }
            }
        """
        plugin_metadata = {
            "data_source_rules": _DEFAULT_DATA_SOURCE_RULES,
            "supported_secret_types": ["MANUAL"],
            "currency": "KRW",
            "use_account_routing": False,
            "exclude_license_cost": False,
            "include_credit_cost": False,
            "include_reservation_cost_at_payg": False,
            "cost_info": {},
            "data_info": {},
            "additional_info": copy.deepcopy(_DEFAULT_METADATA_ADDITIONAL_INFO),
        }

        _check_options(options)

        if options.get("account_agreement_type") == "MicrosoftPartnerAgreement":
            plugin_metadata["additional_info"].update(
                _METADATA_INFO_ADDITIONAL_INFO_MPA
            )
        elif options.get("account_agreement_type") == "EnterpriseAgreement":
            plugin_metadata["additional_info"].update(_METADATA_INFO_ADDITIONAL_INFO_EA)

        if currency := options.get("currency"):
            plugin_metadata["currency"] = currency

        plugin_metadata["cost_info"] = {
            "name": "PayAsYouGo",
            "unit": options.get("currency", "KRW"),
        }

        if options.get("cost_metric") == "AmortizedCost":
            plugin_metadata["data_info"]["Amortized Cost"] = {
                "name": "Amortized Cost",
                "unit": options.get("currency", "KRW"),
            }
        else:
            plugin_metadata["data_info"]["Actual Cost"] = {
                "name": "Actual Cost",
                "unit": options.get("currency", "KRW"),
            }

        if options.get("use_account_routing"):
            plugin_metadata["use_account_routing"] = True
            plugin_metadata["account_match_key"] = "additional_info.Tenant Id"

        if options.get("exclude_license_cost"):
            plugin_metadata["exclude_license_cost"] = True

        # Now only Support Enterprise Agreement
        if options.get("include_credit_cost"):
            plugin_metadata["include_credit_cost"] = True

        if options.get("include_reservation_cost_at_payg"):
            plugin_metadata["include_reservation_cost_at_payg"] = True

        return {"metadata": plugin_metadata}

    def verify_plugin(self, options, secret_data, schema):
        azure_cm_connector: AzureCostMgmtConnector = self.locator.get_connector(
            "AzureCostMgmtConnector"
        )
        azure_cm_connector.create_session(options, secret_data, schema)


def _check_options(options: dict):
    if account_agreement_type := options.get("account_agreement_type"):
        if options["account_agreement_type"] not in [
            "MicrosoftPartnerAgreement",
            "EnterpriseAgreement",
            "MicrosoftCustomerAgreement",
        ]:
            raise ERROR_INVALID_ARGUMENT(
                key="options.account_agreement_type", value=account_agreement_type
            )
