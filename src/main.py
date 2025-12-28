#!/usr/bin/env python3
"""
Main entry point for the Real-Time Economic Analytics Platform.
UPDATED: Now uses the new modern interface
"""

import sys
import os

# Add src to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Use the new interface
from interface.new_user_interface import NewUserInterface
from utils.config import Config

if __name__ == "__main__":
    # Initialize and run the new interface
    config = Config()
    ui = NewUserInterface(config)
    ui.run()