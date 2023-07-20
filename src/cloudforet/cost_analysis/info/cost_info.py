import functools
import logging
from spaceone.api.cost_analysis.plugin import cost_pb2
from spaceone.core.pygrpc.message_type import *
from spaceone.core import utils

__all__ = ['CostInfo', 'CostsInfo']

_LOGGER = logging.getLogger(__name__)


def CostInfo(cost_data):
    try:
        info = {
            'cost': float(cost_data['cost']),
            # 'usd_cost': float(cost_data['usd_cost']),
            'currency': cost_data['currency'],
            'usage_quantity': float(cost_data.get('usage_quantity')),
            'usage_type': cost_data.get('usage_type'),
            # 'usage_unit': cost_data.get('usage_unit'),
            'provider': cost_data.get('provider'),
            'region_code': cost_data.get('region_code'),
            'account': cost_data.get('account'),
            'product': cost_data.get('product'),
            'resource_group': cost_data.get('resource_group'),
            'tags': change_struct_type(cost_data['tags']) if 'tags' in cost_data else None,
            'additional_info': change_struct_type(cost_data['additional_info']) if 'additional_info' in cost_data else None,
            'billed_at': utils.datetime_to_iso8601(cost_data['billed_at'])
        }

        return cost_pb2.CostInfo(**info)

    except Exception as e:
        _LOGGER.debug(f'[CostInfo] cost data: {cost_data}')
        _LOGGER.debug(f'[CostInfo] error reason: {e}', exc_info=True)
        raise e


def CostsInfo(costs_data, **kwargs):
    return cost_pb2.CostsInfo(results=list(map(functools.partial(CostInfo, **kwargs), costs_data)))