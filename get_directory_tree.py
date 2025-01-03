import os
import json


def get_directory_structure(rootdir, target_dirs=["flowfile_worker", "flowfile_core"], max_depth=4, exclude_dirs=["node_modules", ".git", "__pycache__", ".venv", "dist", "build", ".pytest_cache", ".idea", ".github"]):
    """
    Generates a directory structure with full exploration of target directories.
        """

    dir_structure = {}

    if not os.path.isdir(rootdir): #exits if rootdir does not exists
        return "Error: Invalid root directory path provided."

    def _scan_directory(path, depth, structure):
        if depth > max_depth:
            return


        for item in os.listdir(path):
          if item in exclude_dirs:
              continue #skip excluded directories

          item_path = os.path.join(path, item)
          if os.path.isdir(item_path):
                structure[item] = {}
                _scan_directory(item_path, depth + 1, structure[item])
          else:
              structure[item] = None #represents a file


    for item in os.listdir(rootdir):
      item_path = os.path.join(rootdir, item)
      if os.path.isdir(item_path):
          if item in target_dirs: # Full exploration for target directories
              dir_structure[item] = {}
              _scan_directory(item_path, 1, dir_structure[item])
          elif item not in exclude_dirs:
              dir_structure[item] = {} #only includes directories, no files


    return dir_structure



if __name__ == "__main__":
    root_directory_to_scan = "."
    directory_data = get_directory_structure(root_directory_to_scan)


    # Nicely formatted JSON output for easy review or parsing
    print(json.dumps(directory_data, indent=4))

    # Or, save it to a file:
    with open("directory_structure.json", "w") as f:
        json.dump(directory_data, f, indent=4)
    print("File saved to directory_structure.json")