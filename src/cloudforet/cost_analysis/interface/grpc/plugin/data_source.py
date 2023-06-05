from spaceone.api.cost_analysis.plugin import data_source_pb2, data_source_pb2_grpc
from spaceone.core.pygrpc import BaseAPI


class DataSource(BaseAPI, data_source_pb2_grpc.DataSourceServicer):

    pb2 = data_source_pb2
    pb2_grpc = data_source_pb2_grpc

    def init(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DataSourceService', metadata) as data_source_service:
            return self.locator.get_info('PluginInfo', data_source_service.init(params))

    def verify(self, request, context):
        params, metadata = self.parse_request(request, context)

        with self.locator.get_service('DataSourceService', metadata) as data_source_service:
            data_source_service.verify(params)
            return self.locator.get_info('EmptyInfo')
        