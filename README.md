# plugin-azure-cost-mgmt-cost-datasource

Plugin for collecting Azure Cost management data


---

## Azure Service Endpoint(in use)

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