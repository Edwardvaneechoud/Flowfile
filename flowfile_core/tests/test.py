from flowfile_core.routes import *
import psutil
import tracemalloc
import polars as pl
import time
import gc
import objsize
from functools import wraps


def deep_memory_profile(interval=2, depth=3):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            def get_object_tree(obj, level=0, max_depth=3):
                if level >= max_depth:
                    return {}

                tree = {}
                if hasattr(obj, '__dict__'):
                    for k, v in obj.__dict__.items():
                        size = objsize.get_deep_size(v)
                        if isinstance(v, pl.DataFrame):
                            df_size = v.estimated_size()
                            tree[k] = {
                                'size': size + df_size,
                                'type': f"DataFrame ({v.shape})",
                                'children': {'estimated_size': {'size': df_size, 'type': 'DataFrame Memory'}}
                            }
                        else:
                            tree[k] = {
                                'size': size,
                                'type': type(v).__name__,
                                'children': get_object_tree(v, level + 1, max_depth)
                            }
                return tree

            def print_memory_tree(tree, indent=''):
                for key, data in tree.items():
                    print(f"{indent}{key}: {data['size'] / 1024:.2f} KB ({data['type']})")
                    if data['children']:
                        print_memory_tree(data['children'], indent + '  ')

            def print_polars_info(df):
                if isinstance(df, pl.DataFrame):
                    print("\nPolars DataFrame Details:")
                    print(f"Shape: {df.shape}")
                    print(f"Estimated Size: {df.estimated_size() / 1024 / 1024:.2f} MB")
                    print("Column Memory Usage:")
                    for col in df.columns:
                        print(f"  {col}: {df.select(col).estimated_size() / 1024 / 1024:.2f} MB")

            tracemalloc.start()
            process = psutil.Process()
            initial_mem = process.memory_info().rss
            snapshot = tracemalloc.take_snapshot()

            # Track memory before collect
            before_result = process.memory_info().rss
            before_snapshot = tracemalloc.take_snapshot()

            result = func(*args, **kwargs)

            # Track memory immediately after collect
            after_result = process.memory_info().rss
            after_snapshot = tracemalloc.take_snapshot()
            print(f"\nMemory impact of collect():")
            print(f"Delta: {(after_result - before_result) / 1024 / 1024:.2f} MB")

            try:
                while True:
                    current_mem = process.memory_info().rss
                    current_snapshot = tracemalloc.take_snapshot()

                    print(f"\n=== Detailed Memory Analysis at {time.strftime('%H:%M:%S')} ===")
                    print(f"Process Memory: {current_mem / 1024 / 1024:.2f} MB")
                    print(f"Delta from start: {(current_mem - initial_mem) / 1024 / 1024:.2f} MB")

                    gc.collect()
                    print("\nGarbage Collection:")
                    for generation, count in enumerate(gc.get_count()):
                        print(f"Generation {generation}: {count} objects")

                    if isinstance(result, pl.DataFrame):
                        print_polars_info(result)

                    print("\nObject Hierarchy:")
                    tree = get_object_tree(result, max_depth=depth)
                    print_memory_tree(tree)

                    stats = current_snapshot.compare_to(snapshot, 'traceback')
                    print("\nTop Memory Allocations:")
                    for stat in stats[:5]:
                        print(f"\nAllocation: {stat.size_diff / 1024:.2f} KB")
                        print("Traceback:")
                        for line in stat.traceback.format():
                            if 'polars' in line or 'flowfile' in line:
                                print(f"  {line}")

                    vm = psutil.virtual_memory()
                    print(f"\nSystem Memory:")
                    print(f"Total: {vm.total / 1024 / 1024:.0f} MB")
                    print(f"Available: {vm.available / 1024 / 1024:.0f} MB")
                    print(f"Used: {vm.used / 1024 / 1024:.0f} MB")

                    time.sleep(interval)

            except KeyboardInterrupt:
                print("\nStopping profiler...")
                tracemalloc.stop()

            return result

        return wrapper

    return decorator
#
# flow_file_handler.import_flow('/Users/edwardvanechoud/Documents/flowfile_docs/files/further_analysis.flowfile')
# graph = flow_file_handler.get_flow(1)
# self = graph.get_node(node_id=49)
# performance_mode = True
# run_location = 'auto'
# self.execute_remote
# # self.get_table_example(True)
# # self.get_table_example(include_data=)
# # graph.run_grdaph
# # graph.run_graph(performance_mode=False)


@deep_memory_profile(interval=20, depth=5)
def your_function():
    flow_file_handler.import_flow('/Users/edwardvanechoud/Documents/flowfile_docs/files/further_analysis.flowfile')
    graph = flow_file_handler.get_flow(1)
    graph.run_graph(performance_mode=True)


if __name__ == '__main__':
    your_function()