from threading import Lock
from multiprocessing import Process
from typing import Dict


class ProcessManager:
    def __init__(self):
        self.process_dict: Dict[str, Process] = {}
        self.lock = Lock()

    def add_process(self, task_id: str, process: Process):
        """Add a process to the manager."""
        with self.lock:
            self.process_dict[task_id] = process

    def get_process(self, task_id: str) -> Process:
        """Retrieve a process by its task ID."""
        with self.lock:
            return self.process_dict.get(task_id)

    def remove_process(self, task_id: str):
        """Remove a process from the manager by its task ID."""
        with self.lock:
            self.process_dict.pop(task_id, None)

    def cancel_process(self, task_id: str):
        """Cancel a running process by its task ID."""
        with self.lock:
            process = self.process_dict.get(task_id)
            if process:
                # Terminate and remove the process
                process.terminate()
                process.join()
                self.process_dict.pop(task_id, None)
                return True
        return False
