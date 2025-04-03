"""
Measurements Tab Module

This module contains the functions to create and manage the measurements tab.
"""

import tkinter as tk
from tkinter import ttk
from typing import Dict, Optional, Tuple


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
        
        # Average value from csv fil
        self.average_var = tk.StringVar(value="--")
        ttk.Label(
            self,
            text="1-min avg:",
            font=("Arial", 10)
        ).pack(pady=(5, 0))
        ttk.Label(
            self,
            textvariable=self.average_var,
            font=("Arial", 12)
        ).pack()
    
    def update_realtime(self, value: Optional[float]) -> None:
        """Update the real-time value display."""
        if value is not None:
            self.realtime_var.set(f"{value:.2f}")
        else:
            self.realtime_var.set("--")
    
    def update_average(self, value: Optional[float]) -> None:
        """Update the average value display."""
        if value is not None:
            self.average_var.set(f"{value:.2f}")
        else:
            self.average_var.set("--")


class MeasurementsDisplay:
    """Frame personalizado para mostrar las mediciones de los sensores."""
    
    def __init__(self, frame: ttk.Frame):
        self.frame = frame
        
        # Configure grid weights
        frame.grid_columnconfigure((0,1,2,3), weight=1, uniform="column")
        frame.grid_rowconfigure((0,1,2,3), weight=1, uniform="row")
        
        # Meteorological sensors (csv file)
        self.meteo_sensors = {
            "Temperature": SensorBox(frame, "Temp Ext", "°C"),
            "Humidity": SensorBox(frame, "HR Ext", "%"),
            "Pressure": SensorBox(frame, "PA", "hPa"),
            "WindSpeed": SensorBox(frame, "Vel Viento", "m/s"),
            "WindDirection": SensorBox(frame, "Dir Viento", "°"),
            "RainRate": SensorBox(frame, "Lluvia", "mm/h"),
            "UV": SensorBox(frame, "UV", ""),
            "SolarRadiation": SensorBox(frame, "Rad", "W/m²"),
        }
        
        # Air quality sensors (WAD file)
        self.air_sensors = {
            "C1": SensorBox(frame, "CO", "ppm"),
            "C2": SensorBox(frame, "NO", "ppb"),
            "C3": SensorBox(frame, "NO₂", "ppb"),
            "C4": SensorBox(frame, "NOₓ", "ppb"),
            "C5": SensorBox(frame, "O₃", "ppb"),
            "C6": SensorBox(frame, "PM₁₀", "µg/m³"),
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
                self.air_sensors[key].update_average(value)


def create_measurements_tab(notebook: ttk.Notebook) -> Tuple[ttk.Frame, MeasurementsDisplay]:
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
        text="Mediciones Tiempo Real", 
        font=("Arial", 14, "bold")
    ).pack(pady=10)
    ttk.Label(
        measurements_tab, 
        text="Promedios Minutales", 
        font=("Arial", 10, "bold")
    ).pack()
    
    # Create the measurements frame
    measurements_frame = ttk.Frame(measurements_tab)
    measurements_frame.pack(pady=10, fill=tk.BOTH, expand=True)
    
    # Create the measurements display
    display = MeasurementsDisplay(measurements_frame)
    
    return measurements_tab, display 