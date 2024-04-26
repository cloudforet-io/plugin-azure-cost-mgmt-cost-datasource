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

DEFAULT_ACCOUNT_CONNECT_POLICES = [
    {
        "name": "connect_account_to_workspace",
        "polices": {
            "connect_account_to_workspace": {
                "operator": "in",
                "source": "account_id",
                "target": "references",
            }
        },
    },
    {
        "name": "connect_cost_to_account",
        "polices": {
            "connect_cost_to_account": {
                "operator": "eq",
                "source": "additional_info.Tenant Id",
                "target": "account_id",
            }
        },
    },
]


class MatchServiceAccount(Model):
    source = StringType(required=True)
    target = StringType(required=True)


class ConnectResource(Model):
    operator = StringType(choices=["in", "eq"], default=["eq"])
    source = StringType(required=True)
    target = StringType(required=True)


class Actions(Model):
    match_service_account = ModelType(MatchServiceAccount)


class Polices(Model):
    connect_account_to_workspace = ModelType(ConnectResource)
    connect_cost_to_account = ModelType(ConnectResource)


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


class PluginMetadata(Model):
    data_source_rules = ListType(
        ModelType(DataSourceRule), default=_DEFAULT_DATA_SOURCE_RULES
    )
    account_connect_polices = ListType(ModelType(AccountMatchPolicy), default=[])
    supported_secret_types = ListType(StringType, default=["MANUAL"])
    currency = StringType(default="KRW")
    use_account_routing = BooleanType(default=False)
    alias = DictType(StringType, default={})
