import logging

# Configure logging settings
logging.basicConfig(
    level=logging.INFO,  # Set logging level to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",  # Format log messages
    handlers=[
        logging.FileHandler("pipeline.log"),  # Save logs to a file
        logging.StreamHandler()  # Print logs to console
    ]
)

# Create a logger instance
logger = logging.getLogger(__name__)
