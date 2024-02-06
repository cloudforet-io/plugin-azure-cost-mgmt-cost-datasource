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
        plugin_metadata.validate()

        return {"metadata": plugin_metadata.to_primitive()}

    def verify_plugin(self, options, secret_data, schema):
        azure_cm_connector: AzureCostMgmtConnector = self.locator.get_connector(
            "AzureCostMgmtConnector"
        )
        azure_cm_connector.create_session(options, secret_data, schema)
