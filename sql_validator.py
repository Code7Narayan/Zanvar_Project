from tkinter import messagebox

class SQLValidator:
    @staticmethod
    def contains_dangerous_sql(query: str) -> bool:
        """Detect destructive SQL operations."""
        lowered = query.lower()
        destructive_keywords = [
            "drop table", "drop database", "truncate table",
            "delete from", "alter table", "update "
        ]
        return any(keyword in lowered for keyword in destructive_keywords)

    @staticmethod
    def confirm_dangerous_query():
        """Asks for confirmation before executing a dangerous query."""
        return messagebox.askyesno(
            "Confirm Destructive Query",
            "⚠️ This query may modify or delete data.\n\nDo you want to proceed?"
        )

    @staticmethod
    def show_input_error(title, message):
        messagebox.showwarning(title, message)

    @staticmethod
    def show_error(title, message):
        messagebox.showerror(title, message)
    
    @staticmethod
    def show_info(title, message):
        messagebox.showinfo(title, message)