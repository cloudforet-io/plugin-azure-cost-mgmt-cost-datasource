<h1 align="center">Azure Cost Management Plugin </h1>

Plugin for collecting Azure Cost management data

# Contents

- [Azure Service Endpoint(in use)](#azure-service-endpoint-in-use)
- [Schema Data](#schema-data)
- [Options](#options)
- [Release Note](#release-note)

---

## Azure Service Endpoint (in use)

<pre>
https://*.blob.core.windows.net
https://management.azure.com
https://login.microsoftonline.com
</pre>

----

## Schema Data

*Schema*

- billing_account_id (str):
- tenant_id (str):
- client_id (str):
- client_secret (str):
- customer_tenants (list):(Optional) Customer's tenant id list

*Example for EA*
<pre>
<code>
{
    "billing_account_id": "*****",
    "tenant_id": "*****",
    "client_id": "*****",
    "client_secret": "*****"
}
</code>
</pre>

*Example for CSP*
<pre>
<code>
{
    "billing_account_id": "*****",
    "tenant_id": "*****",
    "client_id": "*****",
    "client_secret": "*****"
    "customer_tenants":                #(optional)
        - "*****"
     
}
</code>
</pre>

## Options

Currently, not required.

---

# Release Note

| Version | Description                                                                                                                    | Release Date |
|---------|--------------------------------------------------------------------------------------------------------------------------------|--------------|
| 1.1.11  | - [Add `Meter Name` to check snapshot cost](https://github.com/cloudforet-io/plugin-azure-cost-mgmt-cost-datasource/issues/49) | 2024-02-20   |
