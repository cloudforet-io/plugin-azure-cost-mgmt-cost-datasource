import logging

from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.model.data_source_model import PluginMetadata
from cloudforet.cost_analysis.connector.azure_cost_mgmt_connector import (
    AzureCostMgmtConnector,
)

_LOGGER = logging.getLogger(__name__)


class DataSourceManager(BaseManager):
    @staticmethod
    def init_response(options):
        plugin_metadata = PluginMetadata()
        if currency := options.get("currency"):
            plugin_metadata.currency = currency

        plugin_metadata.cost_info = {
            "name": "PayAsYouGo",
            "unit": options.get("currency", "KRW"),
        }

        if options.get("cost_metric") == "AmortizedCost":
            plugin_metadata.data_info["Amortized Cost"] = {
                "name": "Amortized Cost",
                "unit": options.get("currency", "KRW"),
            }
        else:
            plugin_metadata.data_info["Actual Cost"] = {
                "name": "Actual Cost",
                "unit": options.get("currency", "KRW"),
            }

        if options.get("use_account_routing"):
            plugin_metadata.use_account_routing = True
            plugin_metadata.account_match_key = "additional_info.Tenant Id"

        if options.get("exclude_license_cost"):
            plugin_metadata.exclude_license_cost = True

        plugin_metadata.validate()
        return {"metadata": plugin_metadata.to_primitive()}

    def verify_plugin(self, options, secret_data, schema):
        azure_cm_connector: AzureCostMgmtConnector = self.locator.get_connector(
            "AzureCostMgmtConnector"
        )
        azure_cm_connector.create_session(options, secret_data, schema)
