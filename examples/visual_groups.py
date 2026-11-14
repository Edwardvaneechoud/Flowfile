"""Visual groups from Python: nested boxes around nodes, a group that follows a join, and the code export."""

# --8<-- [start:imports]
import tempfile
from pathlib import Path

import flowfile as ff
from flowfile_core.flowfile.code_generator import export_flow_to_flowframe
from flowfile_core.flowfile.manage.io_flowfile import open_flow

# --8<-- [end:imports]

# --8<-- [start:nested]
cleaning = ff.FlowGroup("Cleaning", color="blue")
scoring = ff.FlowGroup("Scoring", parent_group=cleaning)  # a box inside the Cleaning box

orders = ff.from_dict({"id": [1, 2, 3], "amount": [120.0, 40.0, 900.0]}).add_to_group(cleaning)
kept = orders.filter(ff.col("amount") > 50).add_to_group(cleaning)
scored = kept.with_columns((ff.col("amount") * 1.1).alias("scored")).add_to_group(scoring)
gate = ff.Gate(scored, formula="[scored] > 500").add_to_group(scoring)  # native nodes join a group too
big = gate.then.with_columns(ff.lit("big").alias("size"))  # not in any group
# --8<-- [end:nested]

flow = big.flow_graph
assert scoring.parent_group is cleaning and scoring.flow_graph is flow
assert sorted(cleaning.node_ids) == sorted([orders.node_id, kept.node_id])
assert sorted(scoring.node_ids) == sorted([scored.node_id, gate.node_id])
assert flow.get_node(big.node_id).setting_input.group_id is None
assert flow._groups[scoring.id].parent_group_id == cleaning.id
assert big.collect()["size"].to_list() == ["big", "big"]  # an open gate passes every row through

# --8<-- [start:join]
sources = ff.FlowGroup("Sources", color="green")
customers = ff.from_dict({"id": [1, 2, 3], "name": ["Ann", "Bob", "Cy"]}).add_to_group(sources)
regions = ff.from_dict({"id": [1, 2, 3], "region": ["EU", "US", "EU"]})  # its own graph, so not in the group yet
joined = customers.join(regions, on="id")  # merges the two graphs; the group follows its nodes
regions.add_to_group(sources)  # on the merged graph now, so it can join
joined.add_to_group(sources)
# --8<-- [end:join]

assert sources.flow_graph is joined.flow_graph
assert sorted(sources.node_ids) == sorted([customers.node_id, regions.node_id, joined.node_id])
assert joined.collect()["region"].to_list() == ["EU", "US", "EU"]

# --8<-- [start:export]
code = export_flow_to_flowframe(flow)
print(code)
# --8<-- [end:export]

assert 'cleaning = ff.FlowGroup("Cleaning", color="blue")' in code
assert 'scoring = ff.FlowGroup("Scoring", parent_group=cleaning)' in code
assert ".add_to_group(scoring)" in code

# --8<-- [start:round-trip]
with tempfile.TemporaryDirectory() as folder:
    path = Path(folder) / "grouped.yaml"
    big.save_graph(str(path))  # open this file in the designer to see the boxes
    reopened = open_flow(path)
# --8<-- [end:round-trip]

names = {group.name: group for group in reopened._groups.values()}
assert names["Scoring"].parent_group_id == names["Cleaning"].id
assert sorted(reopened._member_node_ids(names["Cleaning"].id)) == sorted(cleaning.node_ids)
