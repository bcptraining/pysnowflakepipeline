import logging
import os
import sys


class SafeConsoleHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except UnicodeEncodeError:
            msg = self.format(record).encode("ascii", errors="replace").decode("ascii")
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()


# def setup_logger(
#     name: str = "pipeline_logger",
#     log_to_file: bool = False,
#     log_filename: str = "pipeline.log",
#     level=logging.INFO
# ):
#     try:
#         logger = logging.getLogger(name)

#         # Prevent adding handlers multiple times
#         if not logger.handlers:
#             logger.setLevel(level)
#             formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

#             # Console output
#             console = SafeConsoleHandler(stream=sys.stdout)
#             console.setFormatter(formatter)
#             logger.addHandler(console)

#             # Optional file output
#             if log_to_file:
#                 log_dir = os.path.dirname(log_filename)
#                 if log_dir and not os.path.exists(log_dir):
#                     os.makedirs(log_dir, exist_ok=True)
#                 file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
#                 file_handler.setFormatter(formatter)
#                 logger.addHandler(file_handler)

#         return logger

#     except Exception as e:
#         print(f"⚠️ Failed to initialize logger: {e}")
#         return None


def setup_logger(
    name: str = "pipeline_logger",
    log_to_file: bool = False,
    log_filename: str = "pipeline.log",
    level=logging.DEBUG,
):
    try:
        logger = logging.getLogger(name)
        logger.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Ensure console handler is attached (only once)
        if not any(isinstance(h, SafeConsoleHandler) for h in logger.handlers):
            console = SafeConsoleHandler(stream=sys.stdout)
            console.setFormatter(formatter)
            logger.addHandler(console)

        # Ensure file handler is attached (only once)
        if log_to_file and not any(
            isinstance(h, logging.FileHandler) for h in logger.handlers
        ):
            log_dir = os.path.dirname(log_filename)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            file_handler = logging.FileHandler(log_filename, mode="a", encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    except Exception as e:
        print(f"⚠️ Failed to initialize logger: {e}")
        return None


if __name__ == "__main__":
    log = setup_logger(log_to_file=True, log_filename="logs/pipeline.log")
    print(f"Logger initialized? → {log}")
