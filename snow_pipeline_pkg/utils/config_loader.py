



def load_config(config_path):

    """

    This function reads a config file from disk
validates that it exists and has content,

    then returns it as a Python dictionary which describes the copy operation.

    """

    if not os.path.isfile(config_path):

        raise FileNotFoundError(f"❌ File not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:

        content = f.read()

        if not content.strip():

            raise ValueError("⚠️ JSON config file is empty.")

    return json.loads(content)

