from schematics.models import Model
from schematics.types import ListType, IntType, DateTimeType, StringType, DictType
from schematics.types.compound import ModelType
# todo: change pydantic
__all__ = ['Tasks']


class TaskOptionsWithSubscription(Model):
    subscription_id = StringType(serialize_when_none=False)
    tenant_id = StringType(serialize_when_none=False)


class TaskOptionsWithCustomerTenants(Model):
    customer_tenants = ListType(StringType, serialize_when_none=False)


class TaskOptions(TaskOptionsWithSubscription, TaskOptionsWithCustomerTenants):
    start = StringType(required=True)
    collect_scope = StringType(choices=['subscription_id', 'billing_account_id', 'customer_tenant_id'], required=True)


class Task(Model):
    task_options = ModelType(TaskOptions, required=True)


class Changed(Model):
    start = DateTimeType(required=True)
    end = DateTimeType(default=None)
    filter = DictType(StringType, default={})


class Tasks(Model):
    tasks = ListType(ModelType(Task), required=True)
    changed = ListType(ModelType(Changed), default=[])