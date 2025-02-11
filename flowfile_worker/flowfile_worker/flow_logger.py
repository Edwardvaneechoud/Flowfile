import logging
import requests
from flowfile_worker.models import RawLogInput
from flowfile_worker.configs import FLOWFILE_CORE_URI

LOGGING_URL = FLOWFILE_CORE_URI + "/raw_logs"


class FlowfileLogHandler(logging.Handler):
    def __init__(self, flowfile_flow_id: int = 1, flowfile_node_id: int | str = -1):
        super().__init__()
        self.flowfile_flow_id = flowfile_flow_id
        self.flowfile_node_id = flowfile_node_id

    def emit(self, record):
        try:
            log_message = self.format(record)

            extra = {"Node Id": self.flowfile_node_id}
            for k, v in extra.items():
                log_message = f"{k}: {v} - {log_message}"
            raw_log_input = RawLogInput(
                flowfile_flow_id=self.flowfile_flow_id,
                log_message=log_message,
                log_type=record.levelname.upper(),
                extra={
                }
            )
            if self.flowfile_flow_id != -1 and self.flowfile_node_id != -1:
                response = requests.post(LOGGING_URL, json=raw_log_input.__dict__,
                                         headers={"Content-Type": "application/json"})
                if response.status_code != 200:
                    raise Exception(f"Failed to send log: {response.text}")
        except Exception as e:
            print(f"Error sending log to {LOGGING_URL}: {e}")


def get_worker_logger(flowfile_flow_id: int, flowfile_node_id: int | str) -> logging.Logger:
    logger_name = f"NodeLog: {flowfile_node_id}"
    logger = logging.getLogger(logger_name)
    logger.propagate = False  # Prevent propagation to parent loggers
    logger.setLevel(logging.DEBUG)

    # Only add handlers if they don't already exist to avoid duplicates
    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

        http_handler = FlowfileLogHandler(flowfile_flow_id=flowfile_flow_id, flowfile_node_id = flowfile_node_id)
        http_handler.setLevel(logging.INFO)
        http_formatter = logging.Formatter('%(message)s')
        http_handler.setFormatter(http_formatter)
        logger.addHandler(http_handler)

    return logger
