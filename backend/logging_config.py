import logging
import sys

def configure_logging():
    """
    Configure structured logging for the FastAPI backend.

    Format is inspired by SPDLog:
    [timestamp] [level] [logger] message
    
    This configuration sends output to sys.stderr, which is standard practice for log streaming.
    """
    root = logging.getLogger()
    if root.handlers:
        # Already configured
        return

    # Logs are generally written to stderr, while stdout is reserved for application output.
    handler = logging.StreamHandler(sys.stderr)
    
    # Use the full SPDLog-style date format, including millisecond precision
    fmt = "[%(asctime)s.%(msecs)03d] [%(levelname)s] [%(name)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    handler.setFormatter(formatter)

    # Set the base logging level
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    
    # Suppress redundant or excessively verbose logs from external libraries 
    # (FastAPI/Uvicorn access logs are handled separately by Uvicorn itself)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    # Log successful configuration
    logging.getLogger(__name__).info("Custom logging initialized with SPDLog style format.")

# You don't call this function here; it is called in main.py
