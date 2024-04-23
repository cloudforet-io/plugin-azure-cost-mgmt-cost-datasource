import logging

from spaceone.core.service import *
from cloudforet.cost_analysis.manager.cost_manager import CostManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@event_handler
class CostService(BaseService):
    resource = "Cost"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cost_mgr: CostManager = self.locator.get_manager("CostManager")

    @transaction
    @check_required(["options", "secret_data", "domain_id"])
    def get_linked_accounts(self, params: dict):
        """Get linked accounts
        Args:
            params (dict): {
                'options': 'dict',
                'schema': 'dict,
                'secret_data': 'dict',
                'domain_id': 'str'
            }
        Returns:
            list of linked_accounts
        """
        options = params["options"]
        schema = params.get("schema")
        secret_data = params["secret_data"]
        domain_id = params["domain_id"]

        return self.cost_mgr.get_linked_accounts(
            options, secret_data, domain_id, schema
        )

    @transaction
    @check_required(["options", "secret_data", "task_options"])
    def get_data(self, params):
        """Get Cost Data

        Args:
            params (dict): {
                'options': 'dict',
                'secret_data': 'dict',
                'schema': 'str',
                'task_options': 'dict',
                'domain_id': 'str'
            }

        Returns:
            list of cost_data

        """

        options = params["options"]
        secret_data = params["secret_data"]
        schema = params.get("schema")
        task_options = params["task_options"]

        return self.cost_mgr.get_data(options, secret_data, schema, task_options)
