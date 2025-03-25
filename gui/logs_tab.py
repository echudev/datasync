"""
Logs Tab Module

This module contains the functions to create and manage the logs tab.
"""

import os
import logging
import tkinter as tk
from tkinter import ttk, scrolledtext

# Obtener el logger
logger = logging.getLogger("data_collection")


def create_logs_tab(notebook):
    """
    Create the logs tab.
    
    Args:
        notebook: The notebook widget
        
    Returns:
        A tuple containing the tab frame and the logs text widget
    """
    # Crear el frame para la pestaña
    logs_tab = ttk.Frame(notebook)
    
    # Título de la pestaña de logs
    ttk.Label(
        logs_tab, 
        text="Registros del Sistema", 
        font=("Arial", 14, "bold")
    ).pack(pady=10)
    
    # Área de texto con desplazamiento para los logs
    logs_text = scrolledtext.ScrolledText(logs_tab, wrap=tk.WORD)
    logs_text.pack(pady=10, fill=tk.BOTH, expand=True)
    
    # Botón para refrescar logs manualmente
    ttk.Button(
        logs_tab,
        text="Refrescar Logs",
        command=lambda: refresh_logs(logs_text)
    ).pack(pady=5)
    
    return logs_tab, logs_text


def refresh_logs(text_widget):
    """
    Refresh the logs in the text widget.
    
    Args:
        text_widget: The text widget to update
    """
    try:
        log_dir = "logs"
        log_file = os.path.join(log_dir, "data_collection.log")
        if os.path.exists(log_file):
            with open(log_file, "r") as f:
                logs_content = f.read()
                text_widget.delete(1.0, tk.END)
                text_widget.insert(tk.END, logs_content)
                text_widget.see(tk.END)  # Desplazar al final
    except Exception as e:
        logger.error(f"Error refreshing logs: {e}") 