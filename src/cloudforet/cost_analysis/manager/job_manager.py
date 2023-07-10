import logging
from datetime import datetime, timedelta

from spaceone.core.manager import BaseManager
from cloudforet.cost_analysis.connector.azure_cost_mgmt_connector import AzureCostMgmtConnector
from cloudforet.cost_analysis.model.job_model import Tasks
from cloudforet.cost_analysis.conf.cost_conf import SECRET_TYPE_DEFAULT
from cloudforet.cost_analysis.error.cost import *

_LOGGER = logging.getLogger(__name__)


class JobManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.azure_cm_connector: AzureCostMgmtConnector = self.locator.get_connector('AzureCostMgmtConnector')

    def get_tasks(self, options, secret_data, schema, start, last_synchronized_at, domain_id):

        start_time = self._get_start_time(start, last_synchronized_at)
        start_date = start_time.strftime('%Y-%m-%d')
        changed_time = start_time

        self.azure_cm_connector.create_session(options, secret_data, schema)

        secret_type = options.get('secret_type', SECRET_TYPE_DEFAULT)

        if secret_type == SECRET_TYPE_DEFAULT:
            tenants = self._get_tenants_from_billing_account()
            tasks = [{'task_options': {'tenants': tenants, 'start': start_date}}]
            changed = [{'start': changed_time}]
        elif secret_type == 'USE_SERVICE_ACCOUNT_SECRET':
            subscription_id = secret_data.get('subscription_id', '')
            tenants = [secret_data.get('tenant_id')]
            tasks = [{'task_options': {'subscription_id': subscription_id, 'tenants': tenants, 'start': start_date}}]
            changed = [{'start': changed_time}]

        else:
            raise ERROR_INVALID_SECRET_TYPE(secret_type=options.get('secret_type'))

        tasks = Tasks({'tasks': tasks, 'changed': changed})
        tasks.validate()
        return tasks.to_primitive()

    def _get_tenants_from_billing_account(self):
        tenants = []
        for billing_account in self.azure_cm_connector.list_billing_accounts():
            tenants.append(billing_account['customer_id'])
        return tenants

    @staticmethod
    def _get_start_time(start, last_synchronized_at=None):

        if start:
            start_time: datetime = start
        elif last_synchronized_at:
            start_time: datetime = last_synchronized_at - timedelta(days=7)
            start_time = start_time.replace(day=1)
        else:
            start_time: datetime = datetime.utcnow() - timedelta(days=365)
            start_time = start_time.replace(day=1)

        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

        return start_time
