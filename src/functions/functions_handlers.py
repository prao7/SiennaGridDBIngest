import os
import pprint

def get_directory_structure(directory_path):
    """
    Returns a nested dictionary that represents the folder structure of directory_path.
    Directories are keys and their values are dictionaries of their contents.
    Files are stored in a list under the key '__files__'.
    
    Example output:
    {
        'subdir1': {
            '__files__': ['file1.txt', 'file2.txt'],
            'nested': {
                '__files__': ['file3.txt']
            }
        },
        '__files__': ['root_file.py']
    }
    """

    structure = {}
    # Get a sorted list of all items in the current directory
    for item in sorted(os.listdir(directory_path)):
        item_path = os.path.join(directory_path, item)
        if os.path.isdir(item_path):
            structure[item] = get_directory_structure(item_path)
        else:
            structure.setdefault('__files__', []).append(item)
    return structure


def print_directory_structure(directory_path):
    """
    Print the directory structure of directory_path.
    """
    pprint.pprint(get_directory_structure(directory_path))


