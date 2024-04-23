import logging

from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.model.data_source_model import (
    PluginMetadata,
    DEFAULT_ACCOUNT_CONNECT_POLICES,
)
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

        if options.get("pay_as_you_go"):
            plugin_metadata.alias.update({"cost": "PayAsYouGo"})
        elif options.get("cost_metric") == "AmortizedCost":
            plugin_metadata.alias.update({"cost": "Amortized Cost"})
        else:
            plugin_metadata.alias.update({"cost": "Actual Cost"})

        if options.get("use_account_routing"):
            plugin_metadata.use_account_routing = True
            plugin_metadata.account_connect_polices = DEFAULT_ACCOUNT_CONNECT_POLICES

        plugin_metadata.validate()
        return {"metadata": plugin_metadata.to_primitive()}

    def verify_plugin(self, options, secret_data, schema):
        azure_cm_connector: AzureCostMgmtConnector = self.locator.get_connector(
            "AzureCostMgmtConnector"
        )
        azure_cm_connector.create_session(options, secret_data, schema)
