import logging
from functools import wraps


# Set up the logger
def setup_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Create a console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create a formatter and attach it to the handler
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(ch)

    # Add custom methods for different log levels
    logger.log_info = lambda message: logger.info(message)
    logger.log_warning = lambda message: logger.warning(message)
    logger.log_error = lambda message: logger.error(message)
    logger.log_debug = lambda message: logger.debug(message)

    return logger


# Logger instance
logger = setup_logger("app")


# Decorator for logging function calls


def log_function_call(func):
    """
    Decorator to log function calls with arguments, return value, and errors.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        # Log function call start with info
        logger.log_info(f"Calling {func.__name__} with args: {args}, kwargs: {kwargs}")
        try:
            # You can choose to log at different levels
            result = func(*args, **kwargs)
            # Log function return value (info level)
            logger.log_info(f"{func.__name__} returned: {result}")
            return result
        except Exception as e:
            # Log any exception that occurred in the function (error level)
            logger.log_error(f"Exception in {func.__name__}: {e}")
            raise e

    return wrapper
