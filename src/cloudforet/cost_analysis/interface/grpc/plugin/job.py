from spaceone.api.cost_analysis.plugin import job_pb2, job_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Job(BaseAPI, job_pb2_grpc.JobServicer):

    pb2 = job_pb2
    pb2_grpc = job_pb2_grpc

    def get_tasks(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('JobService', metadata) as job_service:
            return self.locator.get_info('TasksInfo', job_service.get_tasks(params))