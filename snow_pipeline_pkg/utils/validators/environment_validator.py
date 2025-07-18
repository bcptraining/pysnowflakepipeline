



def check_dependencies(dependencies=None, log=None):

    """

    Checks whether required libraries are installed.

    Logs missing dependencies and raises an ImportError if any are missing.



    Args:

        dependencies (list): List of package names to check.

        log (Logger): Optional logger instance for output.

    """



    log = log or logging.getLogger(__name__)

    if dependencies is None:

        dependencies = ["pandas", "snowflake.connector", "snowflake.snowpark"]



    missing = []

    for dep in dependencies:

        try:

            importlib.import_module(dep)

        except ImportError:

            missing.append(dep)



    if missing:

        message = (

            f"🚫 Missing required dependencies: {', '.join(missing)}.\n"

            "Please install them using:\n"

            f"pip install {' '.join(missing)}"

        )

        log.error(message)

        raise ImportError(message)

    else:

        log.info("✅ All required dependencies are installed.")

