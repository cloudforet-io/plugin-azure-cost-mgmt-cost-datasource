from schematics.models import Model
from schematics.types import ListType, IntType, DateTimeType, StringType, DictType
from schematics.types.compound import ModelType
# todo: change pydantic
__all__ = ['Tasks']


class TaskOptions(Model):
    start = StringType(required=True)
    tenants = ListType(StringType, default=None, serialize_when_none=False)
    subscription_id = StringType(default=None, serialize_when_none=False)


class Task(Model):
    task_options = ModelType(TaskOptions, required=True)


class Changed(Model):
    start = DateTimeType(required=True)
    end = DateTimeType(default=None)
    filter = DictType(StringType, default={})


class Tasks(Model):
    tasks = ListType(ModelType(Task), required=True)
    changed = ListType(ModelType(Changed), default=[])