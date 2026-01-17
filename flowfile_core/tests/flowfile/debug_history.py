#!/usr/bin/env python3
"""
Practical debugging script for the undo/redo history system.

Run with: python flowfile_core/tests/flowfile/debug_history.py

This script creates a flow, performs operations, and tests undo/redo.
"""

import sys
import os

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'shared'))

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.history_schema import HistoryActionType


def create_test_flow():
    """Create a test flow with a handler."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=1,
            name='debug_flow',
            path='.',
            execution_mode='Development'
        )
    )
    return handler.get_flow(1)


def print_state(flow, label=""):
    """Print current flow state."""
    print(f"\n{'='*60}")
    print(f"STATE: {label}")
    print(f"{'='*60}")
    print(f"Nodes: {[n.node_id for n in flow.nodes]}")
    print(f"Node count: {len(flow.nodes)}")

    history_state = flow.get_history_state()
    print(f"\nHistory:")
    print(f"  Can undo: {history_state.can_undo} ({history_state.undo_count} steps)")
    print(f"  Can redo: {history_state.can_redo} ({history_state.redo_count} steps)")
    if history_state.undo_description:
        print(f"  Undo would: {history_state.undo_description}")
    if history_state.redo_description:
        print(f"  Redo would: {history_state.redo_description}")

    # Memory usage
    usage = flow._history_manager.get_memory_usage()
    print(f"\nMemory usage:")
    print(f"  Undo stack: {usage['undo_stack_bytes']} bytes ({usage['undo_stack_entries']} entries)")
    print(f"  Redo stack: {usage['redo_stack_bytes']} bytes ({usage['redo_stack_entries']} entries)")
    print(f"  Total: {usage['total_bytes']} bytes")


def main():
    print("=" * 60)
    print("UNDO/REDO HISTORY SYSTEM DEBUG TEST")
    print("=" * 60)

    # Create flow
    flow = create_test_flow()
    print_state(flow, "Initial empty flow")

    # Step 1: Add a manual input node
    print("\n>>> Adding manual input node (id=1)...")
    flow.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add manual_input node", node_id=1)

    node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type='manual_input')
    flow.add_node_promise(node_promise)

    sample_data = [
        {'name': 'John', 'city': 'New York', 'age': 30},
        {'name': 'Jane', 'city': 'Los Angeles', 'age': 25},
        {'name': 'Bob', 'city': 'Chicago', 'age': 35},
    ]
    input_settings = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData.from_pylist(sample_data)
    )
    flow.add_manual_input(input_settings)
    print_state(flow, "After adding manual input node")

    # Step 2: Add a filter node
    print("\n>>> Adding filter node (id=2)...")
    flow.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add filter node", node_id=2)

    node_promise2 = input_schema.NodePromise(flow_id=1, node_id=2, node_type='filter')
    flow.add_node_promise(node_promise2)

    filter_settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field='age',
                operator='greater_than',
                value='25'
            )
        )
    )
    flow.add_filter(filter_settings)
    print_state(flow, "After adding filter node")

    # Step 3: Add connection
    print("\n>>> Adding connection 1 -> 2...")
    flow.capture_history_snapshot(HistoryActionType.ADD_CONNECTION, "Connect 1 -> 2")

    connection = input_schema.NodeConnection.create_from_simple_input(
        from_id=1,
        to_id=2,
        input_type='main'
    )
    add_connection(flow, connection)
    print_state(flow, "After adding connection")

    # Step 4: Add a sample node
    print("\n>>> Adding sample node (id=3)...")
    flow.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add sample node", node_id=3)

    node_promise3 = input_schema.NodePromise(flow_id=1, node_id=3, node_type='sample')
    flow.add_node_promise(node_promise3)
    print_state(flow, "After adding sample node")

    # Now test UNDO
    print("\n" + "=" * 60)
    print("TESTING UNDO")
    print("=" * 60)

    # Undo 1: Remove sample node
    print("\n>>> Undo #1...")
    result = flow.undo()
    print(f"Undo result: success={result.success}, action='{result.action_description}'")
    print_state(flow, "After undo #1 (should remove sample node)")

    # Undo 2: Remove connection
    print("\n>>> Undo #2...")
    result = flow.undo()
    print(f"Undo result: success={result.success}, action='{result.action_description}'")
    print_state(flow, "After undo #2 (should remove connection)")

    # Undo 3: Remove filter
    print("\n>>> Undo #3...")
    result = flow.undo()
    print(f"Undo result: success={result.success}, action='{result.action_description}'")
    print_state(flow, "After undo #3 (should remove filter node)")

    # Now test REDO
    print("\n" + "=" * 60)
    print("TESTING REDO")
    print("=" * 60)

    # Redo 1: Add filter back
    print("\n>>> Redo #1...")
    result = flow.redo()
    print(f"Redo result: success={result.success}, action='{result.action_description}'")
    print_state(flow, "After redo #1 (should restore filter node)")

    # Redo 2: Add connection back
    print("\n>>> Redo #2...")
    result = flow.redo()
    print(f"Redo result: success={result.success}, action='{result.action_description}'")
    print_state(flow, "After redo #2 (should restore connection)")

    # Test: New action should clear redo stack
    print("\n" + "=" * 60)
    print("TESTING: New action clears redo stack")
    print("=" * 60)

    print(f"\nRedo stack has {flow.get_history_state().redo_count} entries")

    print("\n>>> Adding new node (id=4)...")
    flow.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add new select node", node_id=4)
    node_promise4 = input_schema.NodePromise(flow_id=1, node_id=4, node_type='select')
    flow.add_node_promise(node_promise4)

    print(f"Redo stack now has {flow.get_history_state().redo_count} entries (should be 0)")
    print_state(flow, "After adding new node (redo stack should be cleared)")

    # Final summary
    print("\n" + "=" * 60)
    print("TEST COMPLETE")
    print("=" * 60)
    print("\nThe undo/redo system is working if:")
    print("  1. Nodes were correctly added and removed via undo")
    print("  2. Redo restored the previously undone state")
    print("  3. New action cleared the redo stack")
    print("  4. Memory usage shows compressed bytes < uncompressed")


if __name__ == "__main__":
    main()
