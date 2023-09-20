from schematics.models import Model
from schematics.types import DictType, StringType, FloatType, DateTimeType

__all__ = ['Cost']


class Cost(Model):
    cost = FloatType(required=True)
    usage_quantity = FloatType(required=True)
    usage_unit = StringType(default=None)
    usage_type = StringType()
    provider = StringType(required=True)
    region_code = StringType()
    product = StringType()
    billed_date = StringType(required=True)
    additional_info = DictType(StringType, default={})
    tags = DictType(StringType, default={})