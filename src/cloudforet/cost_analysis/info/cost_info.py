import functools
import logging
from spaceone.api.cost_analysis.plugin import cost_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils

__all__ = ["CostInfo", "CostsInfo", "AccountInfo", "AccountsInfo"]

_LOGGER = logging.getLogger(__name__)


def AccountInfo(account_data):
    try:
        info = {
            "account_id": account_data["account_id"],
            "name": account_data["name"],
        }
        return cost_pb2.AccountInfo(**info)

    except Exception as e:
        _LOGGER.debug(f"[AccountInfo] account data: {account_data}")
        _LOGGER.debug(f"[AccountInfo] error reason: {e}", exc_info=True)
        raise e


def CostInfo(cost_data):
    try:
        info = {
            "cost": cost_data["cost"],
            "usage_quantity": cost_data.get("usage_quantity"),
            "usage_type": cost_data.get("usage_type"),
            "usage_unit": cost_data.get("usage_unit"),
            "provider": cost_data.get("provider"),
            "region_code": cost_data.get("region_code"),
            "product": cost_data.get("product"),
            "billed_date": cost_data["billed_date"],
            "additional_info": change_struct_type(cost_data["additional_info"])
            if "additional_info" in cost_data
            else None,
            "data": change_struct_type(cost_data["data"])
            if "data" in cost_data
            else None,
            "tags": change_struct_type(cost_data["tags"])
            if "tags" in cost_data
            else None,
        }
        return cost_pb2.CostInfo(**info)

    except Exception as e:
        _LOGGER.debug(f"[CostInfo] cost data: {cost_data}")
        _LOGGER.debug(f"[CostInfo] error reason: {e}", exc_info=True)
        raise e


def AccountsInfo(accounts_data, **kwargs):
    return cost_pb2.AccountsInfo(
        results=list(map(functools.partial(AccountInfo, **kwargs), accounts_data))
    )


def CostsInfo(costs_data, **kwargs):
    return cost_pb2.CostsInfo(
        results=list(map(functools.partial(CostInfo, **kwargs), costs_data))
    )
