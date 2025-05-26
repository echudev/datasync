import asyncio
import logging
import random
from typing import Dict

class MockNox():
    def __init__(self, port=None, logger=None):
        self.logger = logger if logger else logging.getLogger("collector")

    def connect(self) -> None:
        try:
            self.logger.info("Connected to mock nox")
        except Exception as e:
            self.logger.error(f"Error connecting to mock nox: {e}")
            raise         

    async def read(self) -> Dict[str, int]:
        try:
            await asyncio.sleep(1)
            return {
                "NO": random.randint(0, 100),
                "NO2": random.randint(0, 50),
                "NOx": random.randint(0, 150),
            } 
        except Exception as e:
            self.logger.error(f"Error reading data: {e}")
            return {}