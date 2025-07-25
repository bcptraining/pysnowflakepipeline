import os


def print_tree(dir_path, depth=3, prefix=""):
    if depth == 0:
        return
    entries = [
        e
        for e in os.listdir(dir_path)
        if e not in ("__pycache__", ".venv", ".pytest_cache")
    ]
    for entry in entries:
        path = os.path.join(dir_path, entry)
        print(f"{prefix}├── {entry}")
        if os.path.isdir(path):
            print_tree(path, depth - 1, prefix + "│   ")


print_tree(".", depth=3)
