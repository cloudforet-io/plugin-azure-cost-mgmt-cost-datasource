import logging
from spaceone.core.service import *
from cloudforet.cost_analysis.manager.data_source_manager import DataSourceManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@event_handler
class DataSourceService(BaseService):
    resource = "DataSource"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_source_mgr: DataSourceManager = self.locator.get_manager(
            DataSourceManager
        )

    @transaction
    @check_required(["options"])
    def init(self, params):
        """init plugin by options

        Args:
            params (dict): {
                'options': 'dict',
                'domain_id': 'str'
            }

        Returns:
            None
        """
        options = params.get("options", {})

        return self.data_source_mgr.init_response(options)

    @transaction
    @check_required(["options", "secret_data"])
    def verify(self, params):
        """Verifying data source plugin

        Args:
            params (dict): {
                'options': 'dict',
                'schema': 'str',
                'secret_data': 'dict',
                'domain_id': 'str'
            }

        Returns:
            None
        """

        options = params["options"]
        secret_data = params["secret_data"]
        schema = params.get("schema")

        return self.data_source_mgr.verify_plugin(options, secret_data, schema)
