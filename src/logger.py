import logging
import os
from datetime import datetime
import inspect


def get_log_file_name():
    """
    Dynamically generates a log file name based on the caller's file name and current timestamp.
    Skips internal Python frames and ensures a valid filename.
    """
    try:
        # Traverse the call stack to find the caller's file name
        stack = inspect.stack()
        for frame in stack:
            caller_file_path = frame.filename
            # Skip internal Python frames and the current file
            if not caller_file_path.startswith("<") and caller_file_path != __file__:
                caller_file_name = os.path.splitext(os.path.basename(caller_file_path))[0]
                log_file_name = f"{caller_file_name}_{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"
                return log_file_name
        # Fallback if no valid caller is found
        return f"log_{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"
    except Exception as e:
        print(f"Error generating log file name: {e}")
        return f"log_{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"  # Fallback name

# Create the logs directory if it doesn't exist
logs_path = os.path.join(os.getcwd(), "logs")
os.makedirs(logs_path, exist_ok=True)

# Generate the full log file path
LOG_FILE = get_log_file_name()
LOG_FILE_PATH = os.path.join(logs_path, LOG_FILE)

# Configure logging
logging.basicConfig(
    filename=LOG_FILE_PATH,
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# if __name__ == "__main__":
#     logging.info("Logging has started.")