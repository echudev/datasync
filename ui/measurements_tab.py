"""
Measurements Tab Module

This module contains the functions to create and manage the measurements tab.
"""

import tkinter as tk
from tkinter import ttk


def create_measurements_tab(notebook):
    """
    Create the measurements tab.
    
    Args:
        notebook: The notebook widget
        
    Returns:
        A tuple containing the tab frame and the measurements frame
    """
    # Crear el frame para la pestaña
    measurements_tab = ttk.Frame(notebook)
    
    # Título de la pestaña de mediciones
    ttk.Label(
        measurements_tab, 
        text="Últimas Mediciones", 
        font=("Arial", 14, "bold")
    ).pack(pady=10)
    
    # Crear el frame para las mediciones
    measurements_frame = ttk.Frame(measurements_tab)
    measurements_frame.pack(pady=10, fill=tk.BOTH, expand=True)
    
    return measurements_tab, measurements_frame 