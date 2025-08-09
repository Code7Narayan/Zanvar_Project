import tkinter as tk
from tkinter import ttk, messagebox
from queue import Queue, Empty
import threading
from PIL import Image, ImageTk, ImageDraw
import os

# Import the new modules
from sql_connector import SQLConnector
from query_executor import QueryExecutor
from ui_builder import UIBuilder
from config_manager import ConfigManager
from sql_validator import SQLValidator

class SQLToolApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Zanvar's SQL Tool")
        self.root.geometry("1100x800")
        
        self.setup_styles()
        self.set_icon()
        self.message_queue = Queue()
        
        self.logo_image = self.load_logo()
        
        self.ui_builder = UIBuilder(self.root, self.styles, self.logo_image)
        self.config_manager = ConfigManager()
        
        self.sql_connector = None
        self.query_executor = None

        self.current_server_config = None
        self.query_running = False
        self.current_query_log = None

        self.conn_status_text = tk.StringVar(value="")
        self.main_server_info_text = tk.StringVar(value="")

        # Load last login from config
        last_server, last_username = self.config_manager.load_last_login()

        self.ui_builder.build_connection_ui(
            self.connect_to_server,
            self.root.quit,
            self.conn_status_text,
            last_server,
            last_username
        )
        self.check_queue()

    def set_icon(self):
        """Sets the application window icon."""
        try:
            icon_path = "assets/icon.ico"
            if os.path.exists(icon_path):
                self.root.iconbitmap(icon_path)
        except tk.TclError as e:
            print(f"Could not set icon: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while setting the icon: {e}")

    def load_logo(self):
        """Attempts to load the company logo image."""
        logo_path = "assets/logo.png"
        if os.path.exists(logo_path):
            try:
                img = Image.open(logo_path)
                img = img.resize((100, 100), Image.LANCZOS)
                return ImageTk.PhotoImage(img)
            except Exception as e:
                print(f"Error loading logo image from file: {e}")
                return self.create_placeholder_logo()
        else:
            return self.create_placeholder_logo()

    def create_placeholder_logo(self):
        """Creates a simple placeholder image."""
        img = Image.new('RGB', (100, 100), color=self.styles['primary_color'])
        d = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype("arial.ttf", 36)
        except:
            font = ImageFont.load_default()
        d.text((50, 50), "Z", anchor="mm", font=font, fill="#ffffff")
        return ImageTk.PhotoImage(img)

    def setup_styles(self):
        self.styles = {
            "bg_color": "#f0f0f0",
            "primary_color": "#4a6ea9",
            "error_color": "#d9534f",
            "success_color": "#5cb85c",
            "warning_color": "#f0ad4e",
            "font_normal": ('Segoe UI', 10),
            "font_bold": ('Segoe UI', 12, 'bold'),
            "font_label": ('Segoe UI', 11, 'bold'),
            "font_header": ('Segoe UI', 14, 'bold'),
            "font_section_title": ('Segoe UI', 12, 'bold')
        }
        
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        # Create checkmark images
        self.checked_img = self.create_checkmark_image(checked=True)
        self.unchecked_img = self.create_checkmark_image(checked=False)
        
        # Configure button styles
        self.style.configure('Accent.TButton', foreground='white', background=self.styles['primary_color'], 
                           font=self.styles['font_normal'], padding=10, borderwidth=0, relief='flat')
        self.style.map('Accent.TButton', 
                      background=[('active', '#3a5e99')], 
                      foreground=[('active', 'white')])
        
        self.style.configure('Warning.TButton', foreground='white', background=self.styles['warning_color'], 
                           font=self.styles['font_normal'], padding=10, borderwidth=0, relief='flat')
        self.style.map('Warning.TButton', 
                      background=[('active', '#e09d3e')], 
                      foreground=[('active', 'white')])
        
        self.style.configure('Red.TButton', foreground='white', background='#F44336', 
                           font=self.styles['font_bold'], padding=10, borderwidth=0, relief='flat')
        self.style.map('Red.TButton', 
                      background=[('active', '#D32F2F')], 
                      foreground=[('active', 'white')])
        
        # Progressbar style
        self.style.configure('Custom.Horizontal.TProgressbar', 
                           troughcolor=self.styles['bg_color'], 
                           background=self.styles['primary_color'], 
                           borderwidth=0)
        
        # Label styles
        self.style.configure('Header.TLabel', 
                           font=self.styles['font_header'], 
                           background=self.styles['bg_color'])
        self.style.configure('Header.Bold.TLabel', 
                           font=self.styles['font_header'], 
                           foreground='#4CAF50', 
                           background=self.styles['bg_color'])
        self.style.configure('Section.TLabel', 
                           font=self.styles['font_section_title'], 
                           background=self.styles['bg_color'])
        
        # Custom Checkbutton style with checkmark
        self.style.element_create('CustomCheck.indicator', 'image',
                                self.unchecked_img,
                                ('selected', self.checked_img),
                                width=20, height=20)
        
        self.style.layout('Custom.TCheckbutton',
            [('Checkbutton.padding',
              {'children':
               [('CustomCheck.indicator', {'side': 'left', 'sticky': ''}),
                ('Checkbutton.label', {'side': 'left', 'sticky': ''})],
              'sticky': ''})])
        
        self.style.configure('Custom.TCheckbutton',
                           font=self.styles['font_normal'],
                           background=self.styles['bg_color'],
                           foreground='black')
        
        self.root.configure(bg=self.styles['bg_color'])

    def create_checkmark_image(self, checked=True):
        """Create images for custom checkbox states."""
        img = Image.new('RGBA', (20, 20), (255, 255, 255, 0))
        draw = ImageDraw.Draw(img)
        
        # Draw the box
        draw.rectangle((2, 2, 18, 18), outline='black', width=1)
        
        if checked:
            # Draw checkmark
            draw.line((4, 10, 8, 14), fill='black', width=2)
            draw.line((8, 14, 16, 6), fill='black', width=2)
        
        return ImageTk.PhotoImage(img)

    def connect_to_server(self):
        """Handle server connection with improved validation."""
        server_config = self.ui_builder.get_connection_inputs()
        
        if not server_config['server']:
            SQLValidator.show_input_error("Input Error", "Server is required")
            return
        if not server_config['username']:
            SQLValidator.show_input_error("Input Error", "Username is required")
            return
            
        self.current_server_config = server_config
        self.sql_connector = SQLConnector(self.current_server_config)
        
        self.ui_builder.update_connect_button_state("disabled")
        self.ui_builder.show_progress()
        self.conn_status_text.set("Connecting...")
        
        threading.Thread(
            target=self._attempt_connection_thread, 
            daemon=True
        ).start()

    def _attempt_connection_thread(self):
        """Attempt to connect to server in a separate thread."""
        success, message = self.sql_connector.test_connection()
        if success:
            self.message_queue.put(("connection_success", message))
            self.config_manager.save_last_login(self.current_server_config['server'], self.current_server_config['username'])
        else:
            self.message_queue.put(("connection_error", message))

    def handle_connection_success(self, message):
        self.ui_builder.hide_progress()
        self.conn_status_text.set(message)
        
        self.ui_builder.build_main_ui(
            self.current_server_config['server'],
            self.current_server_config['username'],
            self.disconnect_server,
            self.start_query_thread,
            lambda: self.ui_builder.update_query_editor(""),
            self.show_query_history,
            self.clear_results,
            self.save_query_log,
            self.on_database_select,
            self.toggle_select_all_dbs,
            self.update_select_all_state
        )

        self.populate_databases()
        self.ui_builder.update_connect_button_state("normal")

    def handle_connection_error(self, message):
        self.ui_builder.hide_progress()
        self.conn_status_text.set(message)
        self.ui_builder.update_connect_button_state("normal")
        self.current_server_config = None

    def populate_databases(self):
        """Populate the databases dropdown and checkboxes."""
        try:
            dbs = self.sql_connector.get_databases()
            self.ui_builder.populate_db_checkboxes(dbs, self.update_select_all_state)
            self.ui_builder.populate_db_dropdown(dbs)
            if dbs:
                self.on_database_select()
        except Exception as e:
            SQLValidator.show_error("Database Error", f"Failed to fetch databases: {e}")
            self.ui_builder.update_status_bar(f"Error loading databases: {e}")

    def update_select_all_state(self):
        """Update the 'Select All' checkbox based on individual database checkboxes."""
        all_selected = all(var.get() for var in self.ui_builder.db_vars.values())
        self.ui_builder.select_all_var.set(all_selected)

    def on_database_select(self, event=None):
        """Handle database selection change in dropdown."""
        db = self.ui_builder.get_selected_db_from_dropdown()
        if db:
            self.load_tables_for_database(db)

    def load_tables_for_database(self, db):
        """Load tables for the selected database."""
        try:
            tables_data = self.sql_connector.get_tables_for_database(db)
            self.ui_builder.update_table_treeview(tables_data)
            count = len(tables_data)
            self.ui_builder.update_status_bar(f"Loaded {count} tables/views from {db}")
        except Exception as e:
            SQLValidator.show_error("Error", f"Failed to load tables: {e}")
            self.ui_builder.update_status_bar(f"Error loading tables from {db}")

    def toggle_select_all_dbs(self):
        """Toggle selection of all databases based on the Select All checkbox."""
        select_all = self.ui_builder.select_all_var.get()
        self.ui_builder.toggle_all_db_checkboxes(select_all)

    def clear_results(self):
        """Clear the results text area."""
        self.ui_builder.clear_result_text()
        self.ui_builder.update_row_count_label("")
        self.ui_builder.update_exec_time_label("")
        self.ui_builder.update_status_bar("Results cleared")
        self.ui_builder.update_save_log_button_state('disabled')

    def disconnect_server(self):
        """Disconnect from the current server."""
        if messagebox.askyesno("Disconnect", "Are you sure you want to disconnect from the server?"):
            self.sql_connector = None
            self.query_executor = None
            self.current_server_config = None
            self.current_query_log = None
            
            self.ui_builder.clear_main_ui()
            
            # Load last login again for the new connection UI
            last_server, last_username = self.config_manager.load_last_login()
            self.ui_builder.build_connection_ui(
                self.connect_to_server,
                self.root.quit,
                self.conn_status_text,
                last_server,
                last_username
            )
            self.conn_status_text.set("")

    def start_query_thread(self):
        """Prepare and start a new thread to execute the query."""
        if not self.sql_connector:
            SQLValidator.show_error("Connection Error", "Not connected to a database server.")
            return

        selected_dbs = self.ui_builder.get_selected_databases()
        query = self.ui_builder.get_query_text()
        
        if self.query_running:
            SQLValidator.show_info("Wait", "Query already running")
            return
        if not selected_dbs:
            SQLValidator.show_input_error("Selection Error", "Select at least one database.")
            return
        if not query:
            SQLValidator.show_input_error("Input Error", "Enter a SQL query.")
            return

        if SQLValidator.contains_dangerous_sql(query):
            if not SQLValidator.confirm_dangerous_query():
                return
        
        self.config_manager.add_to_history(query)
        self.query_running = True
        self.ui_builder.update_query_buttons_state("disabled")
        self.ui_builder.update_save_log_button_state("disabled")
        self.clear_results()
        self.ui_builder.append_result_text("Executing query...\n")
        self.ui_builder.update_status_bar("Executing query...")
        
        self.query_executor = QueryExecutor(self.sql_connector) 
        
        threading.Thread(
            target=self.query_executor.execute_query, 
            args=(query, selected_dbs, self.message_queue), 
            daemon=True
        ).start()

    def save_query_log(self):
        """Save the current query log."""
        if self.current_query_log and self.current_server_config:
            self.config_manager.save_query_log(self.current_query_log, self.current_server_config)
        else:
            SQLValidator.show_info("No Results", "No query results to save.")

    def show_query_history(self):
        """Display query history in a new window."""
        history = self.config_manager.get_history()
        if not history:
            SQLValidator.show_info("History", "No query history yet.")
            return
            
        hw = tk.Toplevel(self.root)
        hw.title("Query History")
        hw.geometry("600x400")
        
        tk.Label(hw, text="Recent Queries", font=self.styles['font_bold']).pack(pady=5)
        
        lb = tk.Listbox(hw, width=80, height=20, font=('Consolas', 12))
        sb = ttk.Scrollbar(hw, command=lb.yview)
        lb.config(yscrollcommand=sb.set)
        lb.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")
        
        for dt, q in reversed(history):
            preview = q if len(q)<=50 else q[:47]+"..."
            lb.insert(tk.END, f"{dt:%Y-%m-%d %H:%M:%S}: {preview}")

        def load_selected_query(event):
            sel = lb.curselection()
            if sel:
                original_index = len(history) - 1 - sel[0] 
                _, q = history[original_index]
                self.ui_builder.update_query_editor(q)
                hw.destroy()
        lb.bind("<Double-Button-1>", load_selected_query)

    def check_queue(self):
        """Check the message queue for updates from threads."""
        try:
            while True:
                msg_type, msg_data = self.message_queue.get_nowait()

                if msg_type == "connection_success":
                    self.handle_connection_success(msg_data)
                elif msg_type == "connection_error":
                    self.handle_connection_error(msg_data)
                elif msg_type == "status":
                    self.ui_builder.update_status_bar(msg_data)
                elif msg_type == "result":
                    self.ui_builder.append_result_text(msg_data)
                elif msg_type == "row_count":
                    self.ui_builder.update_row_count_label(msg_data)
                elif msg_type == "exec_time":
                    self.ui_builder.update_exec_time_label(msg_data)
                elif msg_type == "total_rows":
                    self.ui_builder.update_status_bar(f"Done. {msg_data}")
                elif msg_type == "current_query_log":
                    self.current_query_log = msg_data
                elif msg_type == "enable_log_button":
                    self.ui_builder.update_save_log_button_state("normal")
                elif msg_type == "done":
                    self.query_running = False
                    self.ui_builder.update_query_buttons_state("normal")

        except Empty:
            pass
        finally:
            self.root.after(100, self.check_queue)

    def on_close(self):
        """Handle application close event."""
        if messagebox.askyesno("Exit", "Are you sure you want to exit?"):
            self.config_manager.save_history()
            self.root.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    try:
        from ctypes import windll
        windll.shcore.SetProcessDpiAwareness(1)
    except:
        pass
    app = SQLToolApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_close)
    root.mainloop()