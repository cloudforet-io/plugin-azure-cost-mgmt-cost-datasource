from schematics.models import Model
from schematics.types import ListType, DictType, StringType, BooleanType
from schematics.types.compound import ModelType

__all__ = ["PluginMetadata"]

_DEFAULT_DATA_SOURCE_RULES = [
    {
        "name": "match_service_account",
        "conditions_policy": "ALWAYS",
        "actions": {
            "match_service_account": {
                "source": "additional_info.Subscription Id",
                "target": "data.subscription_id",
            }
        },
        "options": {"stop_processing": True},
    }
]


class MatchServiceAccount(Model):
    source = StringType(required=True)
    target = StringType(required=True)


class Actions(Model):
    match_service_account = ModelType(MatchServiceAccount)


class Options(Model):
    stop_processing = BooleanType(default=False)


class Condition(Model):
    key = StringType(required=True)
    value = StringType(required=True)
    operator = StringType(
        required=True, choices=["eq", "contain", "not", "not_contain"]
    )


class AccountMatchPolicy(Model):
    name = StringType(required=True)
    polices = DictType(DictType(StringType), required=True)


class DataSourceRule(Model):
    name = StringType(required=True)
    conditions = ListType(ModelType(Condition), default=[])
    conditions_policy = StringType(required=True, choices=["ALL", "ANY", "ALWAYS"])
    actions = ModelType(Actions, required=True)
    options = ModelType(Options, default={})
    tags = DictType(StringType, default={})


class MetadataDataInfo(Model):
    name = StringType(required=True)
    unit = StringType(required=True)


class PluginMetadata(Model):
    data_source_rules = ListType(
        ModelType(DataSourceRule), default=_DEFAULT_DATA_SOURCE_RULES
    )
    supported_secret_types = ListType(StringType, default=["MANUAL"])
    currency = StringType(default="KRW")
    use_account_routing = BooleanType(default=False)
    alias = DictType(StringType, default={})
    account_match_key = StringType(default=None)
    exclude_license_cost = BooleanType(default=False)
    cost_info = DictType(StringType, default={})
    data_info = DictType(ModelType(MetadataDataInfo), default={})
    additional_info = ListType(StringType, default=[])
