import json
import logging
import threading
import time
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.config import DATA_FILE_PATH, CONFIDENCE_THRESHOLD, TARGET_DOMAIN, COMMAND_MAP


class PipelineScheduler:
    """Manages the BCI pipeline background task with pause/resume functionality."""
    
    def __init__(self, mqtt_service, socketio):
        self.mqtt_service = mqtt_service
        self.socketio = socketio
        self.pause_event = threading.Event()
        self.pause_event.set()  # Pipeline starts in HALTED/PAUSED state initially
        self.logger = logging.getLogger()
    
    def interruptible_sleep(self, seconds: float):
        """Sleep that can be interrupted by pause_event."""
        steps = int(seconds * 10)
        for _ in range(steps):
            if self.pause_event.is_set():
                return True
            time.sleep(0.1)
        return False
    
    def toggle_pause(self, should_pause: bool):
        """Toggle the pause state of the pipeline."""
        if should_pause:
            self.pause_event.set()
            self.logger.info("[PIPELINE] Automatic pipeline HALTED.")
        else:
            self.pause_event.clear()
            self.logger.info("[PIPELINE] Automatic pipeline RESUMED.")
    
    def is_paused(self):
        """Check if the pipeline is currently paused."""
        return self.pause_event.is_set()
    
    def _connect_mqtt(self):
        mqtt_client = None
        while mqtt_client is None:
            try:
                mqtt_client = self.mqtt_service.connect()
            except Exception:
                logging.exception("[MQTT ERROR] Connection failed. Retrying in 5s...")
                time.sleep(5)
        return mqtt_client

    def _read_data(self, file_path):
        try:
            with open(file_path, 'r', encoding="utf-8") as file:
                return json.load(file).get("data", [])
        except Exception:
            logging.exception("[FILE READ ERROR] Could not read JSON")
            time.sleep(3)
            return None

    @staticmethod
    def _should_skip_packet(domain, raw_command, confidence):
        return domain != TARGET_DOMAIN or raw_command == "Right" or confidence < CONFIDENCE_THRESHOLD

    def _process_packet(self, packet, last_processed_timestamp):
        domain = packet.get("domain")
        confidence = packet.get("confidence", 0.0)
        raw_command = packet.get("command")
        timestamp = packet.get("timestamp", 0.0)

        if timestamp <= last_processed_timestamp:
            return False, last_processed_timestamp

        if self._should_skip_packet(domain, raw_command, confidence):
            return False, max(timestamp, last_processed_timestamp)

        target_command = COMMAND_MAP.get(raw_command)
        if not target_command:
            return False, max(timestamp, last_processed_timestamp)

        self.mqtt_service.publish_command(raw_command, confidence, timestamp)
        return True, timestamp

    def bci_pipeline_loop(self):
        """Main BCI pipeline loop that processes EEG data and publishes commands."""
        file_path = DATA_FILE_PATH
        self._connect_mqtt()

        try:
            while True:
                if self.pause_event.is_set():
                    time.sleep(0.5)
                    continue

                readings = self._read_data(file_path)
                if readings is None:
                    continue

                last_processed_timestamp = 0.0
                for packet in readings:
                    if self.pause_event.is_set():
                        break

                    published, last_processed_timestamp = self._process_packet(packet, last_processed_timestamp)
                    if published and self.interruptible_sleep(3):
                        break

                if not self.pause_event.is_set():
                    self.interruptible_sleep(2)

        except Exception as e:
            logging.critical(f"[PIPELINE CRASH] {e}")



