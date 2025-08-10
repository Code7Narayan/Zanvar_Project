import tkinter as tk
from tkinter import ttk, scrolledtext

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
        self.font_database = ('Segoe UI', 12, 'bold')  # Increased from 11 to 12
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
        self.db_vars = {}
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

        # Configure custom checkbox style
        self._configure_checkbox_style()

    def _configure_checkbox_style(self):
        """Configure custom checkbox style with proper checkmark symbol."""
        style = ttk.Style()
        
        # Configure custom checkbox style for database list
        style.configure(
            'Database.TCheckbutton',
            font=self.font_database,  # Bold and bigger font
            focuscolor='none',
            background=self.bg_color,
            foreground=self.primary_color
        )
        
        # Map states for proper checkbox appearance
        style.map('Database.TCheckbutton',
            background=[('active', self.bg_color), ('pressed', self.bg_color)],
            foreground=[('active', self.primary_color), ('pressed', self.primary_color)]
        )

    # [Rest of the methods remain exactly the same as in the previous version]

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
        ttk.Checkbutton(
            select_all_frame, 
            text="Select All Databases", 
            variable=self.select_all_var, 
            command=toggle_select_all_command,
            style='Bold.TCheckbutton'
        ).pack(side="left")
        
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

    def populate_db_checkboxes(self, databases, checkbox_toggle_command):
        """Dynamically populate database checkboxes with enhanced styling."""
        for widget in self.db_vars_frame.winfo_children():
            widget.destroy()
        self.db_vars = {}

        for db in databases:
            var = tk.BooleanVar()
            cb = ttk.Checkbutton(
                self.db_vars_frame, 
                text=db, 
                variable=var, 
                command=checkbox_toggle_command,
                style='Database.TCheckbutton'  # Changed to use the new custom style
            )
            cb.pack(anchor="w", padx=8, pady=5)  # Increased padding for better spacing
            self.db_vars[db] = var

    def get_selected_databases(self):
        return [db for db, var in self.db_vars.items() if var.get()]

    def toggle_all_db_checkboxes(self, state):
        for var in self.db_vars.values():
            var.set(state)

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