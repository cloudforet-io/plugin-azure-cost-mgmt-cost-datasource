TYPE = 'ActualCost'
TIMEFRAME = 'Custom'
GRANULARITY = 'Daily'
AGGREGATION = {
    'totalCost': {'name': 'Cost', 'function': 'Sum'},
    'UsageQuantity': {'name': 'UsageQuantity', 'function': 'Sum'}
}

GROUPING = [
    {'type': 'Dimension', 'name': 'ResourceGroup'},
    {'type': 'Dimension', 'name': 'ResourceType'},
    {'type': 'Dimension', 'name': 'ResourceId'},
    {'type': 'Dimension', 'name': 'ResourceLocation'},
    {'type': 'Dimension', 'name': 'SubscriptionId'},
    {'type': 'Dimension', 'name': 'SubscriptionName'},
    {'type': 'Dimension', 'name': 'MeterCategory'},
    {'type': 'Dimension', 'name': 'Meter'},
    {'type': 'Dimension', 'name': 'UnitOfMeasure'},
]

PARAMETERS = {
    'type': TYPE,
    'timeframe': TIMEFRAME,
    'dataset': {
        'granularity': GRANULARITY,
        'aggregation': AGGREGATION,
        'grouping': GROUPING
    }
}

AGGREGATION_WITH_USD = {
    "totalCostUSD": {"name": "CostUSD", "function": "Sum"},
    'UsageQuantity': {'name': 'UsageQuantity', 'function': 'Sum'}
}

GROUPING_WITH_TAG = [
    {'type': 'Dimension', 'name': 'ResourceGroup'},
    {'type': 'Dimension', 'name': 'ResourceType'},
    {'type': 'Dimension', 'name': 'ResourceId'},
    {'type': 'Dimension', 'name': 'ResourceLocation'},
    {'type': 'Dimension', 'name': 'SubscriptionId'},
    {'type': 'Dimension', 'name': 'SubscriptionName'},
    {'type': 'Dimension', 'name': 'MeterCategory'},
    {'type': 'Dimension', 'name': 'Meter'},
    {'type': 'Dimension', 'name': 'UnitOfMeasure'},
    {'type': 'Tag', 'name': ''}
]

PARAMETERS_WITH_USD_AND_TAG = {
    'type': TYPE,
    'timeframe': TIMEFRAME,
    'dataset': {
        'granularity': GRANULARITY,
        'aggregation': AGGREGATION_WITH_USD,
        'grouping': GROUPING_WITH_TAG
    }
}

REGION_MAP = {
    'global': 'global',
    'ca central': 'canadacentral',
    'east us': 'eastus',
    'east us2': 'eastus2',
    'kr central': 'koeracentral',
    'kr south': 'koreasouth',
    'jp east': 'japaneast',
    'jp west': 'japanwest',
}