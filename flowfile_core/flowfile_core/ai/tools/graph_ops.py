"""Graph-op tool surface — owned by W30.

Wraps the ``FlowGraph.add_*`` / ``connect_node`` / ``delete_node`` /
``delete_connection`` paths (see ``flow_graph.py:813`` for ``FlowGraph``,
``add_node_promise`` at line 1150) so the LLM can mutate the DAG via tool
calls. Naming follows D004's MCP convention: ``flowfile.graph.add_filter``,
``flowfile.graph.connect``, etc.

Stub until W30.
"""
