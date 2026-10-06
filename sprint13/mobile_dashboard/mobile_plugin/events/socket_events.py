import time
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from flask_socketio import emit


def register_socket_events(socketio, mqtt_service, pipeline_scheduler, action_observer=None):
    """
    Register all Flask-SocketIO event handlers.
    
    Args:
        socketio: Flask-SocketIO instance
        mqtt_service: MQTTService instance for publishing commands
        pipeline_scheduler: PipelineScheduler instance for pause/resume control
    """
    
    @socketio.on('connect')
    def handle_connect():
        emit('sync_state', {'is_paused': pipeline_scheduler.is_paused()})
    
    @socketio.on('toggle_pause')
    def handle_pause(data):
        should_pause = data.get('paused', False)
        pipeline_scheduler.toggle_pause(should_pause)
        
        # Toggle media subscription based on pause state
        mqtt_service.toggle_media_subscription(not should_pause)
    
    @socketio.on('manual_command')
    def handle_manual(data):
        raw_key = data.get('raw_command')
        conf = data.get('confidence', 0.95)
        ts = time.time()

        logging.info(f"[MANUAL TRIGGER] Dispatching {raw_key}")
        mqtt_service.publish_command(raw_key, conf, ts)
        if action_observer:
            action_observer(raw_key, "mobile")
        
        # Activate manual command mode to allow media updates
        mqtt_service.activate_manual_command_mode(duration_seconds=10)
    
    @socketio.on('live_search_input')
    def handle_search_input(data):
        # Live search is always allowed regardless of pause state
        query_text = data.get('query', '')
        if not query_text or not query_text.strip():
            return

        logging.info(f"[MANUAL SEARCH] Query: '{query_text.strip()}'")
        mqtt_service.publish_search_query(query_text.strip())
