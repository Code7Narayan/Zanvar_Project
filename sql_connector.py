import pyodbc
from contextlib import contextmanager

class SQLConnector:
    def __init__(self, server_config):
        self.server_config = server_config

    @contextmanager
    def get_connection(self, database="master"):
        """Context manager for database connections."""
        if not self.server_config:
            raise ValueError("Server configuration is not set.")

        conn = None
        try:
            conn = pyodbc.connect(
                f'DRIVER={{SQL Server}};SERVER={self.server_config["server"]};'
                f'DATABASE={database};UID={self.server_config["username"]};'
                f'PWD={self.server_config["password"]}',
                timeout=10
            )
            yield conn
        finally:
            if conn:
                conn.close()

    def test_connection(self):
        """Tests the connection to the server."""
        try:
            with self.get_connection("master") as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return True, "Connected successfully"
        except pyodbc.InterfaceError:
            return False, f"Connection failed: Invalid connection parameters."
        except pyodbc.OperationalError:
            return False, f"Connection failed."
        except Exception:
            return False, f"Unexpected error during connection test."

    def get_databases(self):
        """Fetches a list of available databases."""
        try:
            with self.get_connection("master") as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4 AND state = 0 ORDER BY name")
                return [r[0] for r in cursor.fetchall()]
        except Exception as e:
            raise Exception(f"Failed to fetch databases: {e}")

    def get_tables_for_database(self, db_name):
        """Fetches tables and views for a given database."""
        try:
            with self.get_connection(db_name) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT TABLE_NAME, TABLE_TYPE
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE IN ('BASE TABLE','VIEW')
                    ORDER BY TABLE_TYPE, TABLE_NAME
                """)
                return cursor.fetchall()
        except Exception as e:
            raise Exception(f"Failed to load tables from {db_name}: {e}")