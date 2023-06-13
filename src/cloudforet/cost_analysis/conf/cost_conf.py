RETRY_COUNT = 3
TYPE = 'ActualCost'
TIMEFRAME = 'Custom'
GRANULARITY = 'Daily'

AGGREGATION_USD_COST = {"totalCostUSD": {"name": "CostUSD", "function": "Sum"}}
AGGREGATION_COST = {"totalCost": {"name": "Cost", "function": "Sum"}}
AGGREGATION_USAGE_QUANTITY = {"UsageQuantity": {"name": "UsageQuantity", "function": "Sum"}}

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

GROUPING_TAG_OPTION = {'type': 'Tag', 'name': ''}

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