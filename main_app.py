import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from queue import Queue, Empty
import threading
from PIL import Image, ImageTk, ImageDraw
import os

# Assuming these are the imports for the other modules
from sql_connector import SQLConnector
from query_executor import QueryExecutor
from config_manager import ConfigManager
from sql_validator import SQLValidator

class UIBuilder:
    def __init__(self, root, styles, logo_image):
        self.root = root
        self.styles = styles
        self.logo_image = logo_image
        
        # Set default colors if not provided in styles
        self.bg_color = styles.get('bg_color', '#f5f5f5')
        self.primary_color = styles.get('primary_color', '#2c3e50')
        self.muted_color = styles.get('muted_color', '#7f8c8d')
        self.success_color = styles.get('success_color', '#27ae60')
        self.accent_color = styles.get('accent_color', '#3498db')
        self.border_color = styles.get('border_color', '#bdc3c7')
        self.card_bg = styles.get('card_bg', '#ffffff')
        self.light_gray = styles.get('light_gray', '#ecf0f1')
        self.dark_bg = styles.get('dark_bg', '#2c3e50')
        
        # Font definitions
        self.font_normal = ('Segoe UI', 11)
        self.font_bold = ('Segoe UI', 11, 'bold')
        self.font_label = ('Segoe UI', 11, 'bold')
        self.font_header = ('Segoe UI', 12, 'bold')
        self.font_database = ('Segoe UI', 12, 'bold')
        self.font_subtitle = ('Segoe UI', 14, 'bold')
        self.font_small = ('Segoe UI', 10)

        self.conn_frame = None
        self.main_frame = None

        # Connection UI elements
        self.server_entry = None
        self.username_entry = None
        self.password_entry = None
        self.connect_button = None
        self.status_label = None
        self.progress = None
        
        # Main UI elements
        self.select_all_var = None
        self.select_all_cb = None
        self.db_vars = {}
        self.db_checkbuttons = {}
        self.db_var = None
        self.db_dropdown = None
        self.tree = None
        self.query_text = None
        self.result_text = None
        self.run_query_btn = None
        self.row_count_label = None
        self.exec_time_label = None
        self.save_log_btn = None
        self.status_bar = None
        self.db_vars_frame = None

    def build_connection_ui(self, connect_command, quit_command, status_text_var, last_server=None, last_username=None):
        """Build the modern connection UI."""
        if self.conn_frame:
            self.conn_frame.destroy()

        self.conn_frame = tk.Frame(self.root, bg="#f5f5f5")
        self.conn_frame.pack(expand=True, fill="both")

        # Card frame with subtle shadow effect
        card_frame = tk.Frame(
            self.conn_frame, 
            bg="#ffffff", 
            bd=0,
            highlightbackground="#e0e0e0", 
            highlightthickness=1,
            padx=40, 
            pady=40
        )
        card_frame.place(relx=0.5, rely=0.5, anchor="center")

        logo_frame = tk.Frame(card_frame, bg="#ffffff")
        logo_frame.pack(fill="x", pady=(0, 20))
        
        logo_label = tk.Label(logo_frame, image=self.logo_image, bg="#ffffff")
        logo_label.pack(pady=(0, 10))
        
        tk.Label(
            logo_frame, 
            text="Zanvar Group of Industries", 
            font=('Segoe UI', 18, 'bold'),
            bg="#ffffff", 
            fg=self.primary_color
        ).pack()
        
        tk.Label(
            logo_frame, 
            text="Login to your database", 
            font=('Segoe UI', 12),
            bg="#ffffff", 
            fg="gray"
        ).pack(pady=(5, 10))

        input_frame = tk.Frame(card_frame, bg="#ffffff")
        input_frame.pack(fill="x", pady=(10, 0))
        
        fields = [
            ("Server:", "server_entry", last_server or "localhost\\SQLEXPRESS"),
            ("Username:", "username_entry", last_username or "sa"),
            ("Password:", "password_entry", "")
        ]

        for i, (label_text, attr_name, default) in enumerate(fields):
            ttk.Label(
                input_frame, 
                text=label_text, 
                background="#ffffff", 
                font=self.font_label
            ).grid(row=i, column=0, sticky="w", padx=(5, 10), pady=8)
            
            entry = ttk.Entry(
                input_frame, 
                width=35, 
                font=self.font_normal
            )
            entry.insert(0, default)
            entry.grid(row=i, column=1, sticky="ew", padx=5, pady=5)
            setattr(self, attr_name, entry)
            if "password" in attr_name:
                entry.config(show="•")
        
        input_frame.grid_columnconfigure(1, weight=1)

        button_frame = tk.Frame(card_frame, bg="#ffffff")
        button_frame.pack(fill="x", pady=(20, 10))
        
        self.connect_button = ttk.Button(
            button_frame, 
            text="Connect", 
            style='Accent.TButton', 
            command=connect_command
        )
        self.connect_button.pack(side="left", padx=(0, 10), fill="x", expand=True)
        
        ttk.Button(
            button_frame, 
            text="Exit", 
            style='Warning.TButton', 
            command=quit_command
        ).pack(side="right", fill="x", expand=True)

        self.progress = ttk.Progressbar(
            card_frame, 
            orient="horizontal", 
            length=300, 
            mode="indeterminate", 
            style='Custom.Horizontal.TProgressbar'
        )
        self.progress.pack(pady=(10, 0), fill="x")
        self.progress.pack_forget()

        self.status_label = tk.Label(
            card_frame, 
            textvariable=status_text_var, 
            fg="gray", 
            bg="#ffffff", 
            font=self.font_bold
        )
        self.status_label.pack(fill="x", pady=(5, 0))

    def get_connection_inputs(self):
        return {
            'server': self.server_entry.get().strip(),
            'username': self.username_entry.get().strip(),
            'password': self.password_entry.get()
        }

    def update_connect_button_state(self, state):
        self.connect_button.config(state=state)

    def show_progress(self):
        self.progress.pack(pady=(10, 0), fill="x")
        self.progress.start()

    def hide_progress(self):
        self.progress.stop()
        self.progress.pack_forget()

    def build_main_ui(self, server, username, disconnect_command, query_run_command, clear_query_command, show_history_command, clear_results_command, save_log_command, on_db_select_command, toggle_select_all_command, checkbox_toggle_command):
        """Build the main application UI with enhanced styling."""
        if self.conn_frame:
            self.conn_frame.pack_forget()
        
        if self.main_frame:
            self.main_frame.destroy()

        self.main_frame = tk.Frame(self.root, bg=self.bg_color)
        self.main_frame.pack(fill="both", expand=True)
        
        self.build_main_ui_header(server, username, disconnect_command)

        paned = ttk.PanedWindow(self.main_frame, orient="horizontal")
        paned.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        left_panel = ttk.Frame(paned, padding=10)
        right_panel = ttk.Frame(paned, padding=10)
        paned.add(left_panel, weight=1)
        paned.add(right_panel, weight=3)

        self._build_db_explorer(left_panel, on_db_select_command, toggle_select_all_command, checkbox_toggle_command)
        self._build_query_view(right_panel, query_run_command, clear_query_command, show_history_command, clear_results_command, save_log_command)

    def build_main_ui_header(self, server, username, disconnect_command):
        """Builds a styled header for the main UI."""
        header_frame = tk.Frame(
            self.main_frame, 
            bg="#ffffff", 
            pady=10,
            highlightbackground="#e0e0e0",
            highlightthickness=1
        )
        header_frame.pack(fill="x", padx=10, pady=(10, 5))
        header_frame.grid_columnconfigure(0, weight=1)
        header_frame.grid_columnconfigure(1, weight=1)
        
        status_frame = tk.Frame(header_frame, bg="#ffffff")
        status_frame.grid(row=0, column=0, sticky="w", padx=10)

        server_label = ttk.Label(
            status_frame, 
            text=f"Connected to: ", 
            style='Header.TLabel',
            font=self.font_normal
        )
        server_label.pack(side="left", anchor="center")
        
        username_label = ttk.Label(
            status_frame, 
            text=f"{username} ", 
            style='Header.Bold.TLabel',
            font=self.font_bold
        )
        username_label.pack(side="left", anchor="center")
        
        server_name_label = ttk.Label(
            status_frame, 
            text=f"on {server}", 
            style='Header.TLabel',
            font=self.font_normal
        )
        server_name_label.pack(side="left", anchor="center")
        
        ttk.Button(
            header_frame, 
            text="Disconnect", 
            style='Red.TButton', 
            command=disconnect_command
        ).grid(row=0, column=1, sticky="e", padx=10)

    def _build_db_explorer(self, frame, on_db_select_command, toggle_select_all_command, checkbox_toggle_command):
        cf = tk.Frame(frame, bg=self.bg_color)
        cf.pack(fill="both", expand=True, pady=10)
        
        select_all_frame = tk.Frame(cf, bg=self.bg_color)
        select_all_frame.pack(fill="x", pady=(0,5))
        self.select_all_var = tk.BooleanVar()
        self.select_all_cb = tk.Label(
            select_all_frame, 
            text="☐ Select All Databases",
            font=self.font_database,
            bg=self.bg_color,
            fg=self.primary_color
        )
        self.select_all_cb.pack(side="left")
        self.select_all_cb.bind("<Button-1>", lambda e: [self.select_all_var.set(not self.select_all_var.get()), toggle_select_all_command(), self.update_select_all_symbol()])
        
        ttk.Label(
            cf, 
            text="Available Databases:", 
            style='Section.TLabel', 
            background=self.bg_color,
            font=self.font_header
        ).pack(anchor="w", pady=(5, 5))
        
        canvas = tk.Canvas(cf, bg=self.bg_color, highlightthickness=0)
        scrollbar = ttk.Scrollbar(cf, orient="vertical", command=canvas.yview)
        scroll_df = tk.Frame(canvas, bg=self.bg_color)
        scroll_df.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0,0), window=scroll_df, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        self.db_vars_frame = scroll_df

        table_df = tk.Frame(frame, bg=self.bg_color)
        table_df.pack(fill="x", pady=(10, 5))
        ttk.Label(
            table_df, 
            text="Select Database:", 
            style='Section.TLabel', 
            background=self.bg_color,
            font=self.font_header
        ).pack(side="left")
        self.db_var = tk.StringVar()
        self.db_dropdown = ttk.Combobox(
            table_df, 
            textvariable=self.db_var, 
            state="readonly", 
            width=25,
            font=self.font_normal
        )
        self.db_dropdown.pack(side="left", padx=5)
        self.db_dropdown.bind("<<ComboboxSelected>>", on_db_select_command)
        
        ttk.Label(
            frame, 
            text="Tables in Selected Database:", 
            style='Section.TLabel', 
            background=self.bg_color,
            font=self.font_header
        ).pack(anchor="w", pady=(5,5))
        
        tf = tk.Frame(frame, bg=self.bg_color)
        tf.pack(fill="both", expand=True)
        
        # Configure Treeview style
        style = ttk.Style()
        style.configure("Treeview.Heading", font=self.font_bold)
        style.configure("Treeview", font=self.font_normal, rowheight=25)
        
        self.tree = ttk.Treeview(
            tf, 
            columns=('type',), 
            show='tree headings',
            style="Treeview"
        )
        self.tree.heading('#0', text='Table Name', anchor='w')
        self.tree.heading('type', text='Type', anchor='w')
        self.tree.column('#0', width=200)
        self.tree.column('type', width=100)
        
        vsb = ttk.Scrollbar(tf, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tf, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0,column=0,sticky='nsew')
        vsb.grid(row=0,column=1,sticky='ns')
        hsb.grid(row=1,column=0,sticky='ew')
        tf.grid_rowconfigure(0,weight=1)
        tf.grid_columnconfigure(0,weight=1)

    def update_select_all_symbol(self):
        """Update Select All checkbox symbol based on its state."""
        if self.select_all_var.get():
            self.select_all_cb.configure(text="✔ Select All Databases")
        else:
            self.select_all_cb.configure(text="☐ Select All Databases")

    def populate_db_checkboxes(self, databases, checkbox_toggle_command):
        """Dynamically populate database checkboxes with custom symbols."""
        for widget in self.db_vars_frame.winfo_children():
            widget.destroy()
        self.db_vars = {}
        self.db_checkbuttons = {}

        for db in databases:
            var = tk.BooleanVar()
            cb = tk.Label(
                self.db_vars_frame, 
                text=f"☐ {db}",
                font=self.font_database,
                bg=self.bg_color,
                fg=self.primary_color
            )
            cb.pack(anchor="w", padx=8, pady=5)
            cb.bind("<Button-1>", lambda e, d=db: [var.set(not var.get()), checkbox_toggle_command(), self.update_checkbox_symbol(var, d)])
            self.db_vars[db] = var
            self.db_checkbuttons[db] = cb

    def update_checkbox_symbol(self, var, db_name):
        """Update checkbox symbol based on its state."""
        cb = self.db_checkbuttons[db_name]
        if var.get():
            cb.configure(text=f"✔ {db_name}")
        else:
            cb.configure(text=f"☐ {db_name}")

    def get_selected_databases(self):
        return [db for db, var in self.db_vars.items() if var.get()]

    def toggle_all_db_checkboxes(self, state):
        for var in self.db_vars.values():
            var.set(state)
        for db in self.db_vars.keys():
            self.update_checkbox_symbol(self.db_vars[db], db)
        self.update_select_all_symbol()

    def populate_db_dropdown(self, databases):
        self.db_dropdown['values'] = databases
        if databases:
            self.db_var.set(databases[0])

    def get_selected_db_from_dropdown(self):
        return self.db_var.get()

    def update_table_treeview(self, tables_data):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for name, ttype in tables_data:
            self.tree.insert('', 'end', text=name, values=(ttype,))

    def _build_query_view(self, frame, query_run_command, clear_query_command, show_history_command, clear_results_command, save_log_command):
        frame.grid_rowconfigure(1, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        # Query header
        query_header = tk.Frame(frame, bg=self.card_bg, relief="flat")
        query_header.grid(row=0, column=0, sticky="ew", pady=(0, 25))
        
        # Title section
        title_frame = tk.Frame(query_header, bg=self.card_bg)
        title_frame.pack(side="left", fill="y")
        
        title_label = tk.Label(
            title_frame, 
            text="⚡ SQL Query Editor", 
            font=self.font_subtitle, 
            fg=self.primary_color, 
            bg=self.card_bg
        )
        title_label.pack(anchor="w")
        
        desc_label = tk.Label(
            title_frame, 
            text="Write and execute SQL queries with real-time results and analytics", 
            font=self.font_small, 
            fg=self.muted_color, 
            bg=self.card_bg
        )
        desc_label.pack(anchor="w", pady=(5, 0))

        # Button section
        button_frame = tk.Frame(query_header, bg=self.card_bg)
        button_frame.pack(side="right", fill="y", pady=8)
        
        self.run_query_btn = ttk.Button(
            button_frame, 
            text="▶️ Execute Query", 
            style='Accent.TButton', 
            command=query_run_command
        )
        self.run_query_btn.pack(side="right", padx=5)

        clear_query_btn = ttk.Button(
            button_frame, 
            text="🧹 Clear Editor", 
            style='Warning.TButton',
            command=clear_query_command
        )
        clear_query_btn.pack(side="right", padx=5)

        history_btn = ttk.Button(
            button_frame, 
            text="📜 History", 
            style='Modern.TButton',
            command=show_history_command
        )
        history_btn.pack(side="right", padx=5)

        # Query pane
        query_pane = ttk.PanedWindow(frame, orient="vertical")
        query_pane.grid(row=1, column=0, rowspan=5, sticky="nsew")

        # Query editor
        query_editor_frame = tk.Frame(query_pane, bg=self.card_bg, relief="solid", bd=1,
                                    highlightbackground=self.border_color, highlightthickness=1)
        query_editor_container = tk.Frame(query_editor_frame, bg=self.card_bg, padx=12, pady=12)
        query_editor_container.pack(fill="both", expand=True)
        
        editor_header = tk.Frame(query_editor_container, bg=self.light_gray, relief="solid", bd=1)
        editor_header.pack(fill="x", pady=(0, 8))
        
        editor_title = tk.Label(
            editor_header,
            text="💻 Query Editor (Ctrl+Enter to execute, Ctrl+A to select all)",
            font=('Segoe UI', 11, 'bold'),
            bg=self.light_gray,
            fg=self.primary_color,
            pady=8
        )
        editor_title.pack(anchor="w", padx=12)
        
        self.query_text = scrolledtext.ScrolledText(
            query_editor_container, 
            wrap=tk.NONE, 
            height=12, 
            font=('Consolas', 12),
            undo=True, 
            maxundo=100,
            bg="#ffffff",
            fg="#2c3e50",
            selectbackground="#3498db",
            selectforeground="white",
            insertbackground="#2c3e50",
            relief="solid",
            bd=1,
            highlightbackground=self.border_color,
            highlightthickness=1,
            padx=8,
            pady=8
        )
        self.query_text.pack(fill="both", expand=True)
        self.query_text.bind("<Control-Return>", lambda e: query_run_command())
        query_pane.add(query_editor_frame, weight=4)

        # Result viewer
        result_viewer_frame = tk.Frame(query_pane, bg=self.card_bg, relief="solid", bd=1,
                                    highlightbackground=self.border_color, highlightthickness=1)
        result_viewer_container = tk.Frame(result_viewer_frame, bg=self.card_bg, padx=12, pady=12)
        result_viewer_container.pack(fill="both", expand=True)
        
        results_header = tk.Frame(result_viewer_container, bg=self.dark_bg, relief="solid", bd=1)
        results_header.pack(fill="x", pady=(0, 8))
        
        results_title = tk.Label(
            results_header,
            text="📊 Query Results & Output Console",
            font=('Segoe UI', 11, 'bold'),
            bg=self.dark_bg,
            fg="white",
            pady=8
        )
        results_title.pack(anchor="w", padx=12)
        
        self.result_text = scrolledtext.ScrolledText(
            result_viewer_container, 
            wrap=tk.NONE, 
            font=('Consolas', 11),
            bg="#1e1e1e",
            fg="#ffffff",
            selectbackground="#264f78",
            selectforeground="white",
            relief="solid",
            bd=1,
            highlightbackground=self.border_color,
            highlightthickness=1,
            state="disabled",
            padx=8,
            pady=8
        )
        self.result_text.pack(fill="both", expand=True)
        query_pane.add(result_viewer_frame, weight=6)

        # Results summary
        results_summary = tk.Frame(frame, bg="#f1f2f6", relief="solid", bd=1, padx=20, pady=15)
        results_summary.grid(row=6, column=0, sticky="ew", pady=(25, 0))

        summary_left = tk.Frame(results_summary, bg="#f1f2f6")
        summary_left.pack(side="left", fill="y")
        
        summary_title = tk.Label(
            summary_left, 
            text="📈 Execution Summary", 
            font=('Segoe UI', 13, 'bold'), 
            fg=self.primary_color, 
            bg="#f1f2f6"
        )
        summary_title.pack(side="left")

        self.row_count_label = tk.Label(
            summary_left, 
            text="", 
            bg="#f1f2f6", 
            font=('Segoe UI', 11, 'bold'),
            fg=self.success_color
        )
        self.row_count_label.pack(side="left", padx=25)

        self.exec_time_label = tk.Label(
            summary_left, 
            text="", 
            bg="#f1f2f6", 
            font=('Segoe UI', 11, 'bold'),
            fg=self.accent_color
        )
        self.exec_time_label.pack(side="left", padx=20)

        # Action buttons
        summary_right = tk.Frame(results_summary, bg="#f1f2f6")
        summary_right.pack(side="right")
        
        self.save_log_btn = ttk.Button(
            summary_right, 
            text="💾 Export Results", 
            style='Modern.TButton',
            command=save_log_command, 
            state='disabled'
        )
        self.save_log_btn.pack(side="right", padx=5)

        clear_results_btn = ttk.Button(
            summary_right, 
            text="🧹 Clear Results", 
            style='Warning.TButton',
            command=clear_results_command
        )
        clear_results_btn.pack(side="right", padx=5)

        # Status bar
        self.status_bar = tk.Label(
            frame, 
            text="Ready", 
            bd=1, 
            relief=tk.SUNKEN, 
            anchor=tk.W, 
            bg="#f0f0f0",
            fg="#333333",
            font=('Segoe UI', 10)
        )
        self.status_bar.grid(row=7, column=0, sticky="ew")

    def update_query_editor(self, text):
        self.query_text.config(state="normal")
        self.query_text.delete("1.0", tk.END)
        self.query_text.insert(tk.END, text)
        self.query_text.config(state="normal")

    def get_query_text(self):
        return self.query_text.get("1.0", tk.END).strip()

    def append_result_text(self, text):
        self.result_text.config(state="normal")
        self.result_text.insert(tk.END, text)
        self.result_text.config(state="disabled")
        self.result_text.see(tk.END)

    def clear_result_text(self):
        self.result_text.config(state="normal")
        self.result_text.delete("1.0", tk.END)
        self.result_text.config(state="disabled")

    def update_row_count_label(self, text):
        self.row_count_label.config(text=text)

    def update_exec_time_label(self, text):
        self.exec_time_label.config(text=text)

    def update_status_bar(self, text):
        self.status_bar.config(text=text)

    def update_query_buttons_state(self, state):
        self.run_query_btn.config(state=state)
        self.query_text.config(state="normal" if state == "normal" else "disabled")

    def update_save_log_button_state(self, state):
        self.save_log_btn.config(state=state)

    def clear_main_ui(self):
        if self.main_frame:
            self.main_frame.pack_forget()

class SQLToolApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Zanvar Group of Industries")
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
            from PIL import ImageFont
            font = ImageFont.truetype("arial.ttf", 36)
        except:
            from PIL import ImageFont
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
        
        self.style.configure('Modern.TButton', foreground='white', background='#2196F3', 
                            font=self.styles['font_normal'], padding=10, borderwidth=0, relief='flat')
        self.style.map('Modern.TButton', 
                       background=[('active', '#1976D2')], 
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
        
        self.root.configure(bg=self.styles['bg_color'])

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
        if self.ui_builder.db_vars:
            all_selected = all(var.get() for var in self.ui_builder.db_vars.values())
            any_selected = any(var.get() for var in self.ui_builder.db_vars.values())
            self.ui_builder.select_all_var.set(all_selected)
            self.ui_builder.update_select_all_symbol()

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
            success, message = self.config_manager.save_query_log(
                self.current_query_log, self.current_server_config
            )
            if success:
                SQLValidator.show_info("Success", message)
            else:
                SQLValidator.show_error("Error", message)
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
            preview = q.replace('\n', ' ')
            if len(preview) > 50:
                preview = preview[:47] + "..."
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