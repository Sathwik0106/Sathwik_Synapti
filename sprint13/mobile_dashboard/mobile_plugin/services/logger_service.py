import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.config import DEBUG_MODE


class WebLogHandler(logging.Handler):
    """Custom log handler that emits log messages via SocketIO."""
    
    def __init__(self, socketio):
        super().__init__()
        self.socketio = socketio
    
    def emit(self, record):
        try:
            msg = record.getMessage()
            self.socketio.emit('log_message', {
                'message': msg,
                'level': record.levelname
            })
        except Exception:
            pass


def setup_logger(socketio):
    """
    Set up the logger with both web and console handlers.
    
    Args:
        socketio: Flask-SocketIO instance for real-time log emission
    
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG if DEBUG_MODE else logging.INFO)

    # Silence noisy third-party libraries that spam DEBUG logs
    for noisy_logger in ("comtypes", "asyncio", "urllib3", "engineio", "socketio", "werkzeug"):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)

    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Web handler for real-time updates
    web_handler = WebLogHandler(socketio)
    web_handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(web_handler)
    
    # Console handler for local debugging
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)
    
    return logger
