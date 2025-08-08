from flowfile_core.flowfile.flow_graph import  FlowGraph
from flowfile_core.schemas.schemas import FlowGraphConfig


# Config of how and where the flow should execute, what it is named and where it is stored
flow_settings_config = FlowGraphConfig(execution_location='local')


graph = FlowGraph(flow_settings=flow_settings_config)

print(graph)

# FlowGraph(
# Nodes: {}
# Settings:
#   -flow_id: 4242139341
#   -description: None
#   -save_location: None
#   -name:
#   -path:
#   -execution_mode: Performance
#   -execution_location: local
#   -auto_save: False
#   -modified_on: None
#   -show_detailed_progress: True
#   -is_running: False
#   -is_canceled: False

