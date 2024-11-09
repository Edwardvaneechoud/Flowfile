from flowfile_core.routes import *
import os
import subprocess  # To call the dot command

try:
    os.chdir('backend')
except:
    pass

try:
    os.chdir('backend')
except:
    pass


def run_test():
    flow_file_handler.import_flow('new_test')
    flow = flow_file_handler.get_flow(1)

    flow.run_graph()
    node = flow.get_node(1)
    node.get_resulting_data()
    # Inspect the most common types of objects in memory
    objgraph.show_most_common_types(limit=10)

    # Display backreferences to a specific object (e.g., node) with deeper graph
    dot_filename = '/tmp/backref.dot'

    # Increase max_depth to make the graph deeper
    objgraph.show_backrefs([node], max_depth=5, filename=dot_filename)

    # Automatically render the .dot file to a PNG using Graphviz (dot)
    try:
        subprocess.run(['xdot', dot_filename], check=True)
    except Exception as e:
        print(f"Error rendering graph: {e}")


if __name__ == "__main__":
    run_test()


