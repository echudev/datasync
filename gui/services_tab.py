"""
Services Tab Module

This module contains the functions to create and manage the services tab.
"""

import tkinter as tk
from tkinter import ttk


def create_services_tab(notebook, collector, publisher, winaqms_publisher):
    """
    Create the services tab.
    
    Args:
        notebook: The notebook widget
        collector: The data collector instance
        publisher: The CSV publisher instance
        winaqms_publisher: The WinAQMS publisher instance
        
    Returns:
        A tuple containing the tab frame and the services frame
    """
    # Crear el frame para la pestaña
    services_tab = ttk.Frame(notebook)
    
    # Título de la pestaña de servicios
    ttk.Label(
        services_tab, 
        text="Control de Servicios", 
        font=("Arial", 14, "bold")
    ).pack(pady=10)
    
    # Crear el frame para los servicios
    services_frame = ttk.Frame(services_tab)
    services_frame.pack(pady=10, fill=tk.BOTH, expand=True)
    
    return services_tab, services_frame 