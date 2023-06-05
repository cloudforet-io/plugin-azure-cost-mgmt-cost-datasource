import functools
from spaceone.api.cost_analysis.plugin import job_pb2
from spaceone.core.pygrpc.message_type import *

__all__ = ['TaskInfo', 'TasksInfo']


def TaskInfo(task_data):
    info = {
        'task_options': change_struct_type(task_data['task_options'])
    }

    return job_pb2.TaskInfo(**info)


def ChangedInfo(changed_data):
    info = {
        'start': changed_data['start']
    }

    if 'end' in changed_data:
        info['end'] = changed_data['end']

    if 'filter' in changed_data:
        info['filter'] = change_struct_type(changed_data['filter'])

    return job_pb2.ChangedInfo(**info)


def TasksInfo(result, **kwargs):
    tasks_data = result.get('tasks', [])
    changed_data = result.get('changed', [])

    return job_pb2.TasksInfo(tasks=list(map(functools.partial(TaskInfo, **kwargs), tasks_data)),
                             changed=list(map(functools.partial(ChangedInfo, **kwargs), changed_data)))