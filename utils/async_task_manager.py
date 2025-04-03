import asyncio
import logging
from typing import Callable, Dict, Optional

class AsyncTaskManager:
    """Class to manage asynchronous tasks in an event loop."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.logger = logger or logging.getLogger("AsyncTaskManager")

    def add_task(self, name: str, coro: Callable, *args, **kwargs) -> None:
        """Add a new task to the manager."""
        if name in self.tasks:
            self.logger.warning(f"Task '{name}' already exists.")
            return

        async def wrapper():
            try:
                await coro(*args, **kwargs)
            except asyncio.CancelledError:
                self.logger.info(f"Task '{name}' was cancelled.")
            except Exception as e:
                self.logger.error(f"Error in task '{name}': {e}")

        self.tasks[name] = asyncio.create_task(wrapper())
        self.logger.info(f"Task '{name}' added.")

    def cancel_task(self, name: str) -> None:
        """Cancel a task by name."""
        task = self.tasks.get(name)
        if task:
            task.cancel()
            self.logger.info(f"Task '{name}' cancelled.")
            del self.tasks[name]
        else:
            self.logger.warning(f"Task '{name}' not found.")

    def pause_task(self, name: str) -> None:
        """Pause a task by name (not directly supported by asyncio)."""
        self.logger.warning("Pausing tasks is not natively supported in asyncio.")

    def resume_task(self, name: str) -> None:
        """Resume a paused task (not directly supported by asyncio)."""
        self.logger.warning("Resuming tasks is not natively supported in asyncio.")

    def get_task(self, name: str) -> Optional[asyncio.Task]:
        """Get a task by name."""
        return self.tasks.get(name)

    def list_tasks(self) -> Dict[str, str]:
        """List all tasks and their statuses."""
        return {name: task._state for name, task in self.tasks.items()}

    async def shutdown(self) -> None:
        """Cancel all tasks and wait for them to finish."""
        self.logger.info("Shutting down all tasks...")
        for name, task in list(self.tasks.items()):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                self.logger.info(f"Task '{name}' cancelled during shutdown.")
        self.tasks.clear()
