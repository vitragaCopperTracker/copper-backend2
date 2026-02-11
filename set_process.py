#!/usr/bin/env python3
"""
Simple Process Setter
Just input a number (1-5) to set the process
"""

from database_config import get_curser
from database_operations import update_process_status

def main():
    try:
        # Ask for process number
        process_num = input("Enter process number (1-5): ").strip()
        
        # Validate input
        if process_num not in ['1', '2', '3', '4', '5']:
            print("Invalid input. Please enter 1, 2, 3, 4, or 5")
            return
        
        # Update process
        process_name = f"process{process_num}"
        connection, cursor = get_curser()
        update_process_status(cursor, connection, process_name)
        cursor.close()
        connection.close()
        
        print(f"Process updated to {process_name}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()