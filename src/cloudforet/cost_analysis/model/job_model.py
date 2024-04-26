from schematics.models import Model
from schematics.types import ListType, StringType, DictType, BooleanType
from schematics.types.compound import ModelType

__all__ = ["Tasks"]


class TaskOptionsWithSubscription(Model):
    subscription_id = StringType(serialize_when_none=False)
    tenant_id = StringType(serialize_when_none=False)


class TaskOptionsWithCustomerTenant(Model):
    customer_tenants = ListType(StringType, serialize_when_none=False)


class TaskOptions(TaskOptionsWithSubscription, TaskOptionsWithCustomerTenant):
    start = StringType(required=True, max_length=7)
    collect_scope = StringType(
        choices=["subscription_id", "billing_account_id", "customer_tenant_id"],
        required=True,
    )
    account_agreement_type = StringType(
        choices=[
            "EnterpriseAgreement",
            "MicrosoftPartnerAgreement",
            "MicrosoftCustomerAgreement",
            "MicrosoftOnlineServicesProgram",
        ],
        serialize_when_none=False,
    )
    is_sync = BooleanType(default=True)


class Task(Model):
    task_options = ModelType(TaskOptions, required=True)


class Changed(Model):
    start = StringType(required=True, max_length=7)
    end = StringType(default=None, max_length=7)
    filter = DictType(StringType, default={})


class SyncedAccount(Model):
    account_id = StringType(required=True, max_length=255)


class Tasks(Model):
    tasks = ListType(ModelType(Task), required=True)
    changed = ListType(ModelType(Changed), default=[])
    synced_accounts = ListType(ModelType(SyncedAccount), default=[])
