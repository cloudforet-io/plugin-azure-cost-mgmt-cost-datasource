# Getting Started

All the following are executed with the `spacectl` command.
In this guide you will:

- [Register the datasource with Azure credentials](#register-the-datasource-with-azure-credentials)
- [Modify datasource currency (optional)](#modify-datasource-currency-optional)

## Pre-requisites

- [spacectl](https://github.com/cloudforet-io/spacectl)
- [Azure role assignment for access to ARM API](https://learn.microsoft.com/en-us/azure/cost-management-billing/automate/cost-management-api-permissions#assign-service-principal-access-to-azure-resource-manager-apis)
- Azure credentials for the billing account

---

## Register the datasource with Azure credentials

Prepare the yaml file for register the datasource with Azure credentials with name `register_azure_csp_datasource.yaml`

if you are installed cloudforet version `1.12`

```yaml
data_source_type: EXTERNAL
provider: azure
name: Azure
plugin_info:
  plugin_id: plugin-azure-cost-mgmt-cost-datasource
  secret_data:
    client_id: { client_id }
    client_secret: { client_secret }
    tenant_id: { tenant_id }
    billing_account_id: { billing_account_id }
  options:
    collect_resource_id: true
  upgrade_mode: AUTO
tags: { }
template: { }
domain_id: { domain_id }
```

if you are installed cloudforet version `2.0`

```yaml
data_source_type: EXTERNAL
provider: azure
name: Azure
plugin_info:
  schema_id: azure-client-secret-cost-management
  plugin_id: plugin-azure-cost-mgmt-cost-datasource
  secret_data:
    client_id: { client_id }
    client_secret: { client_secret }
    tenant_id: { tenant_id }
    billing_account_id: { billing_account_id }
  options:
    collect_resource_id: true
  upgrade_mode: AUTO

tags: { }
resource_group: DOMAIN
```

and execute the following command

```bash
spacectl exec register cost_analysis.DataSource -f register_azure_csp_datasource.yaml
```

## Sync with your Azure Cost Management data

We synchronize cost data via our scheduler every day at `16:00 UTC`.
If you want to sync immediately, you can use the following command.

before execute the following command, you must get the `data_source_id` from the previous step.

```bash
spacectl list cost_analysis.DataSource --minimal
```

and execute the following command <br>
If you are enterprise customer and have huge data, it may take a long time.

```bash
spacectl exec sync cost_analysis.DataSource -p data_source_id={ data_source_id } -p domain_id = { domain_id }
```

## Modify datasource currency (optional)

> If you modify the currency, it will only affect SpaceONE. This plugin collects your billing currency setting from
> Azure.

By default, the currency is set to KRW. If you wish to change the currency, you can use the following command.

if you are installed cloudforet version `1.12`

```bash
spacecetl exec update_plugin cost_analysis.DataSource -p data_source_id={ data_source_id } -p domain_id={ domain_id } -j '{"options": {"currency": "USD"}}'
```

if you are installed cloudforet version `2.0` and plugin version `1.6.10` above

```bash
spacecetl exec update_plugin cost_analysis.DataSource -p data_source_id={ data_source_id } -j '{"options": {"currency": "USD"}}'
```