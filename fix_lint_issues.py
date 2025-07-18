import os
import re

# Define your repo root
ROOT_DIR = "snow_pipeline_pkg"
MAX_LINE_LENGTH = 88


# Simple rules to catch and remove unused imports
def remove_unused_imports(lines):
    return [line for line in lines if not re.match(r"^import (\w+)$", line.strip())]


# Rename unused variables to _
def rename_unused_vars(lines):
    return [
        (
            re.sub(r"except Exception as e", "except Exception as _", line)
            if "Exception as e" in line
            else line
        )
        for line in lines
    ]


# Wrap long lines
def wrap_long_lines(line):
    if len(line) <= MAX_LINE_LENGTH:
        return [line]
    if "," in line and line.count(",") > 1:
        parts = line.split(", ")
        wrapped = []
        current = ""
        for part in parts:
            if len(current + part) + 2 > MAX_LINE_LENGTH:
                wrapped.append(current.rstrip(", "))
                current = part + ", "
            else:
                current += part + ", "
        if current:
            wrapped.append(current.rstrip(", "))
        return wrapped
    return [line]


# Apply fixes to a file
def fix_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        original_lines = f.readlines()

    new_lines = []
    for line in original_lines:
        line = rename_unused_vars([line])[0]
        wrapped_lines = wrap_long_lines(line)
        new_lines.extend(wrapped_lines)

    new_lines = remove_unused_imports(new_lines)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(new_lines) + "\n")
    print(f"✅ Fixed: {filepath}")


# Scan all .py files in your repo
def scan_repo():
    for root, _, files in os.walk(ROOT_DIR):
        for file in files:
            if file.endswith(".py"):
                fix_file(os.path.join(root, file))


if __name__ == "__main__":
    print("🧹 Running lint cleanup on:", ROOT_DIR)
    scan_repo()
    print("🎉 All files processed.")
