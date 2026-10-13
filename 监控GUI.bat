@echo off
rem monitor GUI launcher: pythonw = no console window
cd /d %~dp0
start "" pythonw monitor_gui.py
