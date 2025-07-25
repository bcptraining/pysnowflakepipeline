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


# def setup_logger(
#     log_to_file=False, log_filename="pipeline.log", name=None, verbose=False
# ):
#     # import logging

#     logger = logging.getLogger(name)
#     logger.setLevel(logging.DEBUG if verbose else logging.INFO)

#     formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

#     # Console handler
#     ch = logging.StreamHandler()
#     ch.setLevel(logging.DEBUG if verbose else logging.INFO)
#     ch.setFormatter(formatter)
#     logger.addHandler(ch)

#     # Optional file handler
#     if log_to_file:
#         fh = logging.FileHandler(log_filename)
#         fh.setLevel(logging.DEBUG if verbose else logging.INFO)
#         fh.setFormatter(formatter)
#         logger.addHandler(fh)


#     return logger
def setup_logger(
    log_to_file=False, log_filename="pipeline.log", name=None, verbose=False
):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # ✅ Console handler using SafeConsoleHandler
    console = SafeConsoleHandler(stream=sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # 📁 Optional file handler (UTF-8 for emoji)
    if log_to_file:
        fh = logging.FileHandler(log_filename, encoding="utf-8")
        fh.setLevel(logging.DEBUG if verbose else logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


if __name__ == "__main__":
    log = setup_logger(log_to_file=True, log_filename="logs/pipeline.log")
    print(f"Logger initialized? → {log}")
