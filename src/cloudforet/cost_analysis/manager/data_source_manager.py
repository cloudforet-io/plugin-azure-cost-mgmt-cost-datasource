import logging

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
                            "visible": True
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
            "cost_info": {},
            "data_info": {},
            "additional_info": {},
        }

        # plugin_metadata.additional_info = {
        #     "Subscription Name": {"name": "Subscription Name", "visible": True},
        #     "Resource Id": {"name": "Resource Id", "visible": False},
        #     "Subscription Id": {"name": "Subscription Id", "visible": False},
        #     "Charge Type": {"name": "Charge Type", "visible": False},
        # }

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

        return {"metadata": plugin_metadata}

    def verify_plugin(self, options, secret_data, schema):
        azure_cm_connector: AzureCostMgmtConnector = self.locator.get_connector(
            "AzureCostMgmtConnector"
        )
        azure_cm_connector.create_session(options, secret_data, schema)
