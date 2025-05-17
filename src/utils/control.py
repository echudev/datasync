import json
import logging
import aiofiles
from typing import Union
from utils.path_dir import CONFIG_DIR  

logger = logging.getLogger(__name__)

# Ruta del control.json relativa al repo
CONTROL_FILE = CONFIG_DIR / "control.json"

async def aupdate_control_file(service_name: str, new_state: Union[str, dict]) -> None:
    """Update service state or last_successful data in control.json."""
    try:
        async with aiofiles.open(CONTROL_FILE, "r") as f:
            data = json.loads(await f.read())

        if service_name == "last_successful":
            # Actualizar solo la entrada específica en last_successful
            if "last_successful" not in data:
                data["last_successful"] = {}
            data["last_successful"].update(new_state["last_successful"])
        else:
            # Preservar last_successful al actualizar estados
            last_successful = data.get("last_successful", {})
            data[service_name] = new_state
            data["last_successful"] = last_successful

        async with aiofiles.open(CONTROL_FILE, "w") as f:
            await f.write(json.dumps(data, indent=4))

    except Exception as e:
        logger.error(f"Error updating control file: {e}")


async def ainitialize_control_file() -> None:
    """Create control.json with initial state if it doesn't exist."""
    if not CONTROL_FILE.exists():
        initial_state = {
            "data_collector": "STOPPED",
            "csv_publisher": "STOPPED",
            "winaqms_publisher": "STOPPED",
            "last_successful": {},
        }
        try:
            async with aiofiles.open(CONTROL_FILE, "w") as f:
                await f.write(json.dumps(initial_state, indent=4))
        except Exception as e:
            logger.error(f"Error creating control file: {e}")


def update_control_file(service_name: str, new_state: Union[str, dict]) -> None:
    """Update service state or last_successful data in control.json (síncrona)."""
    try:
        with open(CONTROL_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if service_name == "last_successful":
            # Actualizar solo la entrada específica en last_successful
            if "last_successful" not in data:
                data["last_successful"] = {}
            data["last_successful"].update(new_state["last_successful"])
        else:
            # Preservar last_successful al actualizar estados
            last_successful = data.get("last_successful", {})
            data[service_name] = new_state
            data["last_successful"] = last_successful

        with open(CONTROL_FILE, "w", encoding="utf-8") as f:
            f.write(json.dumps(data, indent=4))

    except Exception as e:
        logger.error(f"Error updating control file: {e}")


def initialize_control_file() -> None:
    """Create control.json with initial state if it doesn't exist."""
    if not CONTROL_FILE.exists():
        initial_state = {
            "data_collector": "STOPPED",
            "csv_publisher": "STOPPED",
            "winaqms_publisher": "STOPPED",
            "last_successful": {},
        }
        try:
            with open(CONTROL_FILE, "w", encoding="utf-8") as f:
                f.write(json.dumps(initial_state, indent=4))
        except Exception as e:
            logger.error(f"Error creating control file: {e}")
