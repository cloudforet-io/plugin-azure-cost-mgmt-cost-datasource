SECRET_TYPE_DEFAULT = "MANUAL"
RETRY_COUNT = 4
TYPE = "ActualCost"
TIMEFRAME = "Custom"
GRANULARITY = "Daily"

AGGREGATION = {
    "totalCost": {"name": "Cost", "function": "Sum"},
    "UsageQuantity": {"name": "UsageQuantity", "function": "Sum"},
}

BENEFIT_FILTER = {
    "and": [
        {
            "dimensions": {
                "name": "ChargeType",
                "operator": "In",
                "values": ["Purchase"],
            }
        },
        {
            "dimensions": {
                "name": "PricingModel",
                "operator": "In",
                "values": ["Reservation", "SavingsPlan"],
            }
        },
    ]
}
BENEFIT_GROUPING = [
    {"type": "Dimension", "name": "CustomerTenantId"},
    {"type": "Dimension", "name": "CustomerName"},
    {"type": "Dimension", "name": "PricingModel"},
    {"type": "Dimension", "name": "Frequency"},
    {"type": "Dimension", "name": "BenefitId"},
    {"type": "Dimension", "name": "BenefitName"},
    {"type": "Dimension", "name": "ReservationId"},
    {"type": "Dimension", "name": "ReservationName"},
    {"type": "Dimension", "name": "ChargeType"},
    {"type": "Dimension", "name": "MeterCategory"},
]

GROUPING_EA_AGREEMENT_OPTION = [
    {"type": "Dimension", "name": "DepartmentName"},
    {"type": "Dimension", "name": "EnrollmentAccountName"},
]

REGION_MAP = {
    "global": "Global",
    "unknown": "Unknown",
    "unassigned": "Unassigned",
    "all regions": "All Regions",
    "intercontinental": "Intercontinental",
    "ap east": "eastasia",
    "ca central": "canadacentral",
    "ca east": "canadaeast",
    "east us": "eastus",
    "east us2": "eastus2",
    "eu west": "westeurope",
    "kr central": "koreacentral",
    "kr south": "koreasouth",
    "ja east": "japaneast",
    "ja west": "japanwest",
    "us east": "eastus",
    "us east 2": "eastus2",
    "us west": "westus",
    "us west 2": "westus2stage",
    "us central": "centralus",
    "us north central": "northcentralus",
    "ap southeast": "southeastasia",
    "za north": "southafricanorth",
    "uk south": "uksouth",
    "br south": "brazilsouth",
    "in west": "westindia",
    "in central": "centralindia",
    "de west central": "germanywestcentral",
    "us south central": "southcentralus",
}

SCOPE_MAP = {
    "subscription_id": "subscriptions/{subscription_id}",
    "billing_account_id": "providers/Microsoft.Billing/billingAccounts/{billing_account_id}",
    "customer_tenant_id": "providers/Microsoft.Billing/billingAccounts/{billing_account_id}/customers/{customer_tenant_id}",
}

EXCLUDE_LICENSE_SERVICE_FAMILY = ["Office 365 Global"]
