import os
import csv
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union


class WinAQMS:
    def __init__(self, base_path: str = "c:/data", output_dir: str = None):
        self.base_path = base_path
        self.output_dir = output_dir or os.path.dirname(os.path.abspath(__file__))
        self.sensors = ["C1", "C2", "C3", "C4", "C5", "C6"]
        self.sensor_map = {
            "C1": "CO",
            "C2": "NO",
            "C3": "NO2",
            "C4": "NOX",
            "C5": "O3",
            "C6": "PM10",
        }
        self._setup_logging()

    def _setup_logging(self) -> None:
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def _get_file_path(self, process_date: datetime) -> str:
        year_str = process_date.strftime("%Y")
        month_str = process_date.strftime("%m")
        day_str = process_date.strftime("%d")
        folder_path = os.path.join(self.base_path, year_str, month_str)
        wad_file = f"eco{year_str}{month_str}{day_str}.wad"
        return os.path.join(folder_path, wad_file)

    def _process_row_data(
        self, row_data: Dict[str, List[float]]
    ) -> Dict[str, Optional[float]]:
        averages = {}
        for sensor, values in row_data.items():
            if not values:
                averages[sensor] = None
                continue

            avg = sum(values) / len(values)
            if sensor in ("C1", "C2", "C3", "C4"):
                avg = round(avg, 3)
            elif sensor == "C6":
                avg = round(avg)
            averages[sensor] = avg
        return averages

    def get_last_hour(self) -> Dict[str, Union[str, float, None]]:
        now = datetime.now()

        if now.hour == 0:
            process_date = now - timedelta(days=1)
            hour_to_process = 23
        else:
            process_date = now
            hour_to_process = now.hour - 1

        wad_path = self._get_file_path(process_date)

        if not os.path.exists(wad_path):
            self.logger.error(f"File not found: {wad_path}")
            return {}

        start_time = datetime.strptime(
            f"{process_date.strftime('%Y/%m/%d')} {hour_to_process:02d}:00:00",
            "%Y/%m/%d %H:%M:%S",
        )
        end_time = start_time + timedelta(hours=1)

        sensor_data = {sensor: [] for sensor in self.sensors}

        try:
            with open(wad_path, "r", newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    try:
                        dt = datetime.strptime(row["Date_Time"], "%Y/%m/%d %H:%M:%S")
                    except ValueError:
                        self.logger.warning(f"Invalid date format: {row['Date_Time']}")
                        continue

                    if start_time <= dt < end_time:
                        for sensor in self.sensors:
                            val_str = row.get(sensor, "").strip()
                            if val_str:
                                try:
                                    sensor_data[sensor].append(float(val_str))
                                except ValueError:
                                    self.logger.warning(
                                        f"Invalid value for {sensor}: {val_str}"
                                    )

            averages = self._process_row_data(sensor_data)

            result = {
                "FECHA": start_time.strftime("%Y-%m-%d"),
                "HORA": f"{hour_to_process:02d}",
                **{self.sensor_map[sensor]: avg for sensor, avg in averages.items()},
            }

            self._save_result(result, process_date, hour_to_process)
            return result

        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            return {}

    def _save_result(self, result: dict, process_date: datetime, hour: int) -> None:
        output_file = f"promedio_{process_date.strftime('%Y%m%d')}_{hour:02d}.json"
        output_path = os.path.join(self.output_dir, output_file)

        try:
            with open(output_path, "w") as jsonfile:
                json.dump(result, jsonfile, indent=4)
            self.logger.info(f"JSON file generated: {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving results: {str(e)}")


if __name__ == "__main__":
    aqms = WinAQMS()
    aqms.get_last_hour()
