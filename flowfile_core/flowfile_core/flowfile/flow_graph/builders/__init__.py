"""Node builders of FlowGraph, one mixin per node family.

Every ``add_<type>`` stays a method of the composed class so ``getattr(graph, "add_" + node_type)``
dispatch keeps working.
"""
