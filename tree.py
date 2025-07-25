import os

EXCLUDED_DIRS = {"__pycache__", ".venv", ".pytest_cache", "htmlcov"}


def print_tree(dir_path, depth=3, prefix=""):
    if depth == 0:
        return
    entries = [
        e
        for e in os.listdir(dir_path)
        if not e.startswith(".") and e not in EXCLUDED_DIRS
    ]
    for entry in sorted(entries):
        path = os.path.join(dir_path, entry)
        print(f"{prefix}├── {entry}")
        if os.path.isdir(path):
            print_tree(path, depth - 1, prefix + "│   ")


print_tree(".", depth=3)
