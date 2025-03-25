"""
Measurements Tab Module

This module contains the functions to create and manage the measurements tab.
"""

import tkinter as tk
from tkinter import ttk
from typing import Dict, Optional


class SensorBox(ttk.Frame):
    """A custom widget to display sensor measurements."""
    
    def __init__(self, parent, title: str, unit: str):
        super().__init__(parent)
        self.title = title
        self.unit = unit
        
        # Configure style
        style = ttk.Style()
        style.configure("Sensor.TFrame", relief="solid", borderwidth=1)
        self.configure(style="Sensor.TFrame", padding=10)
        
        # Title
        ttk.Label(
            self,
            text=title,
            font=("Arial", 12, "bold"),
            wraplength=120,
            justify="center"
        ).pack(pady=(0, 5))
        
        # Real-time value
        self.realtime_var = tk.StringVar(value="--")
        ttk.Label(
            self,
            textvariable=self.realtime_var,
            font=("Arial", 16)
        ).pack()
        
        # Unit label
        ttk.Label(
            self,
            text=unit,
            font=("Arial", 10)
        ).pack()
        
        # Average value from WAD
        self.wad_var = tk.StringVar(value="--")
        ttk.Label(
            self,
            text="1-min avg:",
            font=("Arial", 10)
        ).pack(pady=(5, 0))
        ttk.Label(
            self,
            textvariable=self.wad_var,
            font=("Arial", 12)
        ).pack()
    
    def update_realtime(self, value: Optional[float]) -> None:
        """Update the real-time value display."""
        if value is not None:
            self.realtime_var.set(f"{value:.2f}")
        else:
            self.realtime_var.set("--")
    
    def update_wad(self, value: Optional[float]) -> None:
        """Update the WAD average value display."""
        if value is not None:
            self.wad_var.set(f"{value:.2f}")
        else:
            self.wad_var.set("--")


class MeasurementsDisplay:
    """Manages the display and updates of all sensor measurements."""
    
    def __init__(self, frame: ttk.Frame):
        self.frame = frame
        
        # Configure grid weights
        frame.grid_columnconfigure((0,1,2,3), weight=1, uniform="column")
        frame.grid_rowconfigure((0,1,2,3), weight=1, uniform="row")
        
        # Meteorological sensors (Davis VP2)
        self.meteo_sensors = {
            "Temperature": SensorBox(frame, "Temp", "°C"),
            "Humidity": SensorBox(frame, "HR", "%"),
            "Pressure": SensorBox(frame, "PA", "hPa"),
            "WindSpeed": SensorBox(frame, "Vel Viento", "m/s"),
            "WindDirection": SensorBox(frame, "Dir Viento", "°"),
            "RainRate": SensorBox(frame, "Lluvia", "mm/h"),
            "UV": SensorBox(frame, "UV", ""),
            "SolarRadiation": SensorBox(frame, "Rad", "W/m²"),
        }
        
        # Air quality sensors (WAD file)
        self.air_sensors = {
            "CO": SensorBox(frame, "CO", "ppm"),
            "NO": SensorBox(frame, "NO", "ppb"),
            "NO2": SensorBox(frame, "NO₂", "ppb"),
            "NOx": SensorBox(frame, "NOₓ", "ppb"),
            "O3": SensorBox(frame, "O₃", "ppb"),
            "PM10": SensorBox(frame, "PM₁₀", "µg/m³"),
        }
        
        # Place sensors in grid
        # Meteorological sensors (top rows)
        row, col = 0, 0
        for sensor in self.meteo_sensors.values():
            sensor.grid(row=row, column=col, padx=5, pady=5, sticky="nsew")
            col += 1
            if col > 3:
                col = 0
                row += 1
        
        # Air quality sensors (bottom rows)
        row, col = 2, 0
        for sensor in self.air_sensors.values():
            sensor.grid(row=row, column=col, padx=5, pady=5, sticky="nsew")
            col += 1
            if col > 3:
                col = 0
                row += 1
    
    def update_meteo_data(self, data: Dict[str, float]) -> None:
        """Update meteorological sensor displays with new data."""
        for key, value in data.items():
            if key in self.meteo_sensors:
                self.meteo_sensors[key].update_realtime(value)
    
    def update_air_data(self, data: Dict[str, float]) -> None:
        """Update air quality sensor displays with WAD file data."""
        for key, value in data.items():
            if key in self.air_sensors:
                self.air_sensors[key].update_wad(value)


def create_measurements_tab(notebook: ttk.Notebook) -> tuple[ttk.Frame, MeasurementsDisplay]:
    """
    Create the measurements tab.
    
    Args:
        notebook: The notebook widget
        
    Returns:
        A tuple containing the tab frame and the measurements display manager
    """
    # Create the tab frame
    measurements_tab = ttk.Frame(notebook)
    
    # Title
    ttk.Label(
        measurements_tab, 
        text="Real-time Measurements", 
        font=("Arial", 14, "bold")
    ).pack(pady=10)
    
    # Create the measurements frame
    measurements_frame = ttk.Frame(measurements_tab)
    measurements_frame.pack(pady=10, fill=tk.BOTH, expand=True)
    
    # Create the measurements display
    display = MeasurementsDisplay(measurements_frame)
    
    return measurements_tab, display 