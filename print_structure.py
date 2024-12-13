import os


def print_directory_structure(startpath, indent_level=0, exclude_folders=None):
    """Prints the directory and file structure starting from the given path, excluding specified folders."""
    if exclude_folders is None:
        exclude_folders = []

    for root, dirs, files in os.walk(startpath):
        # Modify dirs in-place to exclude the specified folders
        dirs[:] = [d for d in dirs if d not in exclude_folders]

        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f'{indent}{os.path.basename(root)}/')
        subindent = ' ' * 4 * (level + 1)
        for file in files:
            print(f'{subindent}{file}')


if __name__ == "__main__":
    # Start from the current directory
    current_dir = os.getcwd()
    print(f"Directory structure of: {current_dir}\n")

    # Call the function, excluding the 'frontend' folder
    print_directory_structure(current_dir, exclude_folders=['.cache',
                                                            '.cache_data', '.git', '.idea', '__pycache__'])