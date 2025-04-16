
import psutil
import os
def log_memory(logger, message=""):
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info().rss  # in bytes
    logger.info(f"{message} Memory usage: {mem_info / (1024 ** 2):.2f} MB")
