#!/usr/bin/env python3
"""
Entrypoint for Telegram Organizer
Supports:
  python main.py --list-chats
  python main.py
"""
import os
import sys

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    from organizer import main
    main()
