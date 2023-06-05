from spaceone.api.cost_analysis.plugin import cost_pb2, cost_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class Cost(BaseAPI, cost_pb2_grpc.CostServicer):

    pb2 = cost_pb2
    pb2_grpc = cost_pb2_grpc

    def get_data(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('CostService', metadata) as cost_service:
            response_stream = cost_service.get_data(params)
            for costs_data in response_stream:
                yield self.locator.get_info('CostsInfo', costs_data)
