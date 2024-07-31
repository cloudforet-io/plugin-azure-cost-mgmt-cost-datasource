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
    print(params)
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
    job_mgr = JobManager()
    params["schema"] = params.pop("schema_name", None)
    return job_mgr.get_tasks(**params)


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
    cost_mgr = CostManager()
    params["schema"] = params.pop("schema_name", None)
    return cost_mgr.get_linked_accounts(**params)


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
    params["schema"] = params.pop("schema_name", None)

    if options.get("cost_metric") == "AmortizedCost" and task_options.get(
        "is_benefit_job", False
    ):
        for cost_response in cost_mgr.get_benefit_data(**params):
            yield {"results": cost_response}
    else:
        for cost_response in cost_mgr.get_data(**params):
            yield {"results": cost_response}
