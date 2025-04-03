"""
Logs Tab Module

This module contains the functions to create and manage the logs tab.
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from utils.log_manager import LogManager

# Crear instancia del LogManager
log_manager = LogManager()

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
    
    # Botones de control
    buttons_frame = ttk.Frame(logs_tab)
    buttons_frame.pack(pady=5)
    
    ttk.Button(
        buttons_frame,
        text="Refrescar Logs",
        command=lambda: refresh_logs(logs_text)
    ).pack(side=tk.LEFT, padx=5)

    ttk.Button(
        buttons_frame,
        text="Eliminar Logs",
        command=clear_logs_file
    ).pack(side=tk.RIGHT, padx=5)

    return logs_tab, logs_text

def refresh_logs(text_widget):
    """Refresh the logs in the text widget."""
    try:
        logs_content = log_manager.read_logs()
        text_widget.delete(1.0, tk.END)
        text_widget.insert(tk.END, logs_content)
        text_widget.see(tk.END)
    except Exception as e:
        messagebox.showerror("Error", f"Error al refrescar los logs: {e}")

def clear_logs_file():
    """Delete the logs file after confirmation."""
    if messagebox.askyesno("Confirmar eliminación", "¿Está seguro que desea eliminar los logs?"):
        success, message = log_manager.clear_logs()
        if success:
            messagebox.showinfo("Éxito", message)
        else:
            messagebox.showerror("Error", message)