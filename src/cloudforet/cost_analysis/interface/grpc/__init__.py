from spaceone.core.pygrpc.server import GRPCServer

from cloudforet.cost_analysis.interface.grpc.plugin.cost import Cost
from cloudforet.cost_analysis.interface.grpc.plugin.data_source import DataSource
from cloudforet.cost_analysis.interface.grpc.plugin.job import Job

__all__ = ["app"]

app = GRPCServer()
app.add_service(Cost)
app.add_service(DataSource)
app.add_service(Job)