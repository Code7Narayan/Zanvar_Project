import os
import pickle
import json
from datetime import datetime
from tkinter import filedialog

class ConfigManager:
    """Manages application configuration, including query history and last login details."""
    
    HISTORY_FILE = "query_history.pkl"
    LAST_LOGIN_FILE = "last_login.json"
    MAX_HISTORY_ENTRIES = 50

    def __init__(self):
        self.query_history = []
        self.load_history()

    def add_to_history(self, query):
        """Add a query with a timestamp to the history."""
        self.query_history.append((datetime.now(), query))
        if len(self.query_history) > self.MAX_HISTORY_ENTRIES:
            self.query_history.pop(0)

    def get_history(self):
        """Return the current query history."""
        return self.query_history

    def load_history(self):
        """Load query history from file."""
        if os.path.exists(self.HISTORY_FILE):
            try:
                with open(self.HISTORY_FILE, "rb") as f:
                    self.query_history = pickle.load(f)
            except (IOError, pickle.PickleError):
                self.query_history = []
                
    def save_history(self):
        """Save query history to file."""
        try:
            with open(self.HISTORY_FILE, "wb") as f:
                pickle.dump(self.query_history, f)
        except (IOError, pickle.PickleError):
            pass

    def save_query_log(self, log_content, server_config):
        """Saves a query log to a file chosen by the user."""
        default_filename = f"query_log_{datetime.now():%Y-%m-%d_%H-%M-%S}.txt"
        file_path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            initialfile=default_filename,
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            title="Save Query Log"
        )
        if file_path:
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    header = f"--- Query Log for Server: {server_config['server']} ---\n"
                    header += f"--- User: {server_config['username']} ---\n"
                    header += f"--- Timestamp: {datetime.now():%Y-%m-%d %H:%M:%S} ---\n\n"
                    f.write(header)
                    f.write(log_content)
                return True, f"Log saved to {file_path}"
            except IOError as e:
                return False, f"Failed to save file: {e}"
        return False, "Save operation cancelled."

    def save_last_login(self, server, username):
        """Saves the last successfully used server and username."""
        data = {"server": server, "username": username}
        try:
            with open(self.LAST_LOGIN_FILE, "w") as f:
                json.dump(data, f)
        except IOError:
            pass

    def load_last_login(self):
        """Loads the last saved server and username."""
        if os.path.exists(self.LAST_LOGIN_FILE):
            try:
                with open(self.LAST_LOGIN_FILE, "r") as f:
                    data = json.load(f)
                    return data.get("server"), data.get("username")
            except (IOError, json.JSONDecodeError):
                return None, None
        return None, None