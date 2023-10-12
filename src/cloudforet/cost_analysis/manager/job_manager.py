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

        start_month = self._get_start_month(start, last_synchronized_at)

        self.azure_cm_connector.create_session(options, secret_data, schema)
        secret_type = options.get('secret_type', SECRET_TYPE_DEFAULT)

        if secret_type == 'MANUAL':
            billing_account_info = self.azure_cm_connector.get_billing_account()
            billing_account_agreement_type = getattr(billing_account_info, 'agreement_type', '')

            if billing_account_agreement_type == 'MicrosoftPartnerAgreement':
                customer_tenants = secret_data.get('customer_tenants', self._get_tenants_from_billing_account())
                if len(customer_tenants) == 0:
                    raise ERROR_EMPTY_CUSTOMER_TENANTS(customer_tenants=customer_tenants)
                tasks = [{'task_options': {'start': start_month, 'account_agreement_type': billing_account_agreement_type,
                                           'collect_scope': 'customer_tenant_id', 'customer_tenants': customer_tenants}}]
            else:
                tasks = [{'task_options': {'start': start_month, 'account_agreement_type': billing_account_agreement_type,
                                           'collect_scope': 'billing_account_id'}}]
            changed = [{'start': start_month}]

        elif secret_type == 'USE_SERVICE_ACCOUNT_SECRET':
            subscription_id = secret_data.get('subscription_id', '')
            tenant_id = secret_data.get('tenant_id')
            tasks = [{'task_options': {'collect_scope': 'subscription_id', 'start': start_month, 'subscription_id': subscription_id, 'tenant_id': tenant_id}}]
            changed = [{'start': start_month}]

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

    def _get_start_month(self, start, last_synchronized_at=None):

        if start:
            start_time: datetime = self._parse_start_time(start)
        elif last_synchronized_at:
            start_time: datetime = last_synchronized_at - timedelta(days=7)
            start_time = start_time.replace(day=1)
        else:
            start_time: datetime = datetime.utcnow() - timedelta(days=365)
            start_time = start_time.replace(day=1)

        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

        return start_time.strftime('%Y-%m')

    @staticmethod
    def _parse_start_time(start_str):
        date_format = '%Y-%m'

        try:
            return datetime.strptime(start_str, date_format)
        except Exception as e:
            raise ERROR_INVALID_PARAMETER_TYPE(key='start', type=date_format)
