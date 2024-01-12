import logging

from spaceone.core.service import *
from cloudforet.cost_analysis.manager.job_manager import JobManager

_LOGGER = logging.getLogger(__name__)


@authentication_handler
@authorization_handler
@event_handler
class JobService(BaseService):
    resource = "Job"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_mgr: JobManager = self.locator.get_manager(JobManager)

    @transaction
    @check_required(["options", "secret_data"])
    @change_timestamp_value(["last_synchronized_at"], timestamp_format="iso8601")
    def get_tasks(self, params):
        """Get Job Tasks

        Args:
            params (dict): {
                'options': 'dict',
                'secret_data': 'dict',
                'schema': 'str',
                'start': 'datetime',
                'last_synchronized_at': 'datetime',
                'domain_id': 'str'
            }

        Returns:
            list of task_data

        """

        options = params["options"]
        secret_data = params["secret_data"]
        schema = params.get("schema")
        start = params.get("start")
        last_synchronized_at = params.get("last_synchronized_at")
        domain_id = params["domain_id"]

        return self.job_mgr.get_tasks(
            options, secret_data, schema, start, last_synchronized_at, domain_id
        )
