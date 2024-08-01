from typing import Generator
from spaceone.cost_analysis.plugin.data_source.lib.server import DataSourcePluginServer

from .manager import CostManager, DataSourceManager, JobManager

app = DataSourcePluginServer()


@app.route("DataSource.init")
def data_source_init(params: dict) -> dict:
    """init plugin by options

    Args:
        params (DataSourceInitRequest): {
            'options': 'dict',      # Required
            'domain_id': 'str'      # Required
        }

    Returns:
        PluginResponse: {
            'metadata': 'dict'
        }
    """
    data_source_mgr = DataSourceManager()
    return data_source_mgr.init_response(**params)


@app.route("DataSource.verify")
def data_source_verify(params: dict) -> None:
    """Verifying data source plugin

    Args:
        params (CollectorVerifyRequest): {
            'options': 'dict',      # Required
            'secret_data': 'dict',  # Required
            'schema': 'str',
            'domain_id': 'str'      # Required
        }

    Returns:
        None
    """
    pass


@app.route("Job.get_tasks")
def job_get_tasks(params: dict) -> dict:
    """Get job tasks

    Args:
        params (JobGetTaskRequest): {
            'options': 'dict',      # Required
            'secret_data': 'dict',  # Required
            'linked_accounts': 'list', # optional
            'schema': 'str',
            'start': 'str',
            'last_synchronized_at': 'datetime',
            'domain_id': 'str'      # Required
        }

    Returns:
        TasksResponse: {
            'tasks': 'list',
            'changed': 'list'
            'synced_accounts': 'list'
        }

    """
    tasks = {
        "tasks": [],
        "changed": [],
        "synced_accounts": [],
    }

    job_mgr = JobManager()
    params["schema"] = params.pop("schema_name", None)
    secret_data = params.pop("secret_data")

    secrets = secret_data.get("secrets", [secret_data])
    for _secret_data in secrets:
        params["secret_data"] = _secret_data
        job_tasks = job_mgr.get_tasks(**params)

        tasks["tasks"].extend(job_tasks.get("tasks", []))
        tasks["changed"].extend(job_tasks.get("changed", []))
        tasks["synced_accounts"].extend(job_tasks.get("synced_accounts", []))

    tasks["changed"] = __remove_duplicate_list_of_dict(tasks.get("changed", []))
    return tasks


@app.route("Cost.get_linked_accounts")
def cost_get_linked_accounts(params: dict) -> dict:
    """Get linked accounts

    Args:
        params (GetLinkedAccountsRequest): {
            'options': 'dict',      # Required
            'schema': 'dict',
            'secret_data': 'dict',  # Required
            'domain_id': 'str'      # Required
    }

    Returns:
        AccountsResponse: {
            'results': 'list'
    }
    """
    result = []
    cost_mgr = CostManager()
    params["schema"] = params.pop("schema_name", None)
    secret_data = params.pop("secret_data")

    secrets = secret_data.get("secrets", [secret_data])
    for _secret_data in secrets:
        params["secret_data"] = _secret_data
        result.extend(cost_mgr.get_linked_accounts(**params))

    return {"results": result}


@app.route("Cost.get_data")
def cost_get_data(params: dict) -> Generator[dict, None, None]:
    """Get external cost data

    Args:
        params (CostGetDataRequest): {
            'options': 'dict',      # Required
            'secret_data': 'dict',  # Required
            'schema': 'str',
            'task_options': 'dict',
            'domain_id': 'str'      # Required
        }

    Returns:
        Generator[ResourceResponse, None, None]
        {
            'cost': 'float',
            'usage_quantity': 'float',
            'usage_unit': 'str',
            'provider': 'str',
            'region_code': 'str',
            'product': 'str',
            'usage_type': 'str',
            'resource': 'str',
            'tags': 'dict'
            'additional_info': 'dict'
            'data': 'dict'
            'billed_date': 'str'
        }
    """
    cost_mgr = CostManager()
    options = params.get("options", {})
    task_options = params.get("task_options", {})
    secret_data = params.pop("secret_data")

    params["schema"] = params.pop("schema_name", None)
    params["secret_data"] = __get_secret_data_with_tenant_id(
        secret_data, task_options.get("tenant_id")
    )

    if options.get("cost_metric") == "AmortizedCost" and task_options.get(
        "is_benefit_job", False
    ):
        for cost_response in cost_mgr.get_benefit_data(**params):
            yield {"results": cost_response}
    else:
        for cost_response in cost_mgr.get_data(**params):
            yield {"results": cost_response}


def __remove_duplicate_list_of_dict(changed: list) -> list:
    seen = set()
    unique_list = []
    for changed_info in changed:
        # Convert dictionary to frozenset of tuples
        frozenset_changed_info = frozenset(changed_info.items())
        if frozenset_changed_info not in seen:
            seen.add(frozenset_changed_info)
            unique_list.append(changed_info)
    return unique_list


def __get_secret_data_with_tenant_id(secret_data: dict, tenant_id: str = None) -> dict:
    secrets = secret_data.get("secrets", [secret_data])
    if len(secrets) == 1:
        return secrets[0]

    for _secret_data in secrets:
        if _secret_data["billing_tenant_id"] == tenant_id:
            secret_data = _secret_data

    return secret_data
