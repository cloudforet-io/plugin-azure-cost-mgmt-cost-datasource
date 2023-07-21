SECRET_TYPE_DEFAULT = 'MANUAL'
RETRY_COUNT = 4
TYPE = 'ActualCost'
TIMEFRAME = 'Custom'
GRANULARITY = 'Daily'

AGGREGATION_USD_COST = {'totalCostUSD': {'name': 'CostUSD', 'function': 'Sum'}}
AGGREGATION_COST = {'totalCost': {'name': 'Cost', 'function': 'Sum'}}
AGGREGATION_USAGE_QUANTITY = {'UsageQuantity': {'name': 'UsageQuantity', 'function': 'Sum'}}

GROUPING = [
    {'type': 'Dimension', 'name': 'ResourceGroup'},
    {'type': 'Dimension', 'name': 'ResourceType'},
    # {'type': 'Dimension', 'name': 'ResourceId'},
    {'type': 'Dimension', 'name': 'ResourceLocation'},
    {'type': 'Dimension', 'name': 'SubscriptionId'},
    {'type': 'Dimension', 'name': 'SubscriptionName'},
    {'type': 'Dimension', 'name': 'MeterCategory'},
    {'type': 'Dimension', 'name': 'Meter'},
    {'type': 'Dimension', 'name': 'UnitOfMeasure'},
    {'type': 'Dimension', 'name': 'BenefitName'},
    {'type': 'Dimension', 'name': 'PricingModel'},
    {'type': 'Dimension', 'name': 'MeterSubcategory'}
]


GROUPING_EA_AGREEMENT_OPTION = [
    {'type': 'Dimension', 'name': 'Department'},
    {'type': 'Dimension', 'name': 'EnrollmentAccount'},
]
GROUPING_CUSTOMER_TENANT_OPTION = {'type': 'Dimension', 'name': 'CustomerTenantId'}
GROUPING_TAG_OPTION = {'type': 'Tag', 'name': ''}

REGION_MAP = {
    'global': 'Global',
    'unknown': 'Unknown',
    'ap east': 'eastasia',
    'ca central': 'canadacentral',
    'ca east': 'canadaeast',
    'east us': 'eastus',
    'east us2': 'eastus2',
    'eu west': 'westeurope',
    'kr central': 'koreacentral',
    'kr south': 'koreasouth',
    'ja east': 'japaneast',
    'ja west': 'japanwest',
    'us east': 'eastus',
    'us east 2': 'eastus2',
    'us west': 'westus',
    'us west 2': 'westus2stage',
    'us central': 'centralus',
    'us north central': 'northcentralus',
    'ap southeast': 'southeastasia',
    'za north': 'southafricanorth',
    'uk south': 'uksouth',
    'br south': 'brazilsouth',
    'in west': 'westindia',
    'in central': 'centralindia',
    'de west central': 'germanywestcentral',
    'us south central': 'southcentralus',
}

SCOPE_MAP = {
    'subscription_id': 'subscriptions/{subscription_id}',
    'billing_account_id': 'providers/Microsoft.Billing/billingAccounts/{billing_account_id}',
    'customer_tenant_id': 'providers/Microsoft.Billing/billingAccounts/{billing_account_id}/customers/{customer_tenant_id}',
}
