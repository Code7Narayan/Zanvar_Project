import time
import pyodbc
import sqlparse
from datetime import datetime

class QueryExecutor:
    def __init__(self, sql_connector):
        self.sql_connector = sql_connector

    def execute_query(self, query, databases, message_queue):
        """
        Executes a SQL query against multiple databases in a separate thread.
        Sends progress and results to the message_queue.
        """
        start_time = time.time()
        total_rows = 0
        current_query_log = {
            'query': query,
            'databases': databases,
            'start_time': datetime.now(),
            'results': []
        }
        
        # Split into multiple SQL statements safely
        statements = [stmt.strip() for stmt in sqlparse.split(query) if stmt.strip()]

        for db in databases:
            try:
                message_queue.put(("status", f"Querying {db}..."))
                with self.sql_connector.get_connection(db) as conn:
                    cursor = conn.cursor()

                    for i, statement in enumerate(statements, 1):
                        formatted_sql = sqlparse.format(statement, reindent=True, keyword_case="upper")
                        try:
                            if not formatted_sql.strip():
                                continue  # Skip empty statements

                            cursor.execute(formatted_sql)
                            rows = []
                            if cursor.description:
                                while True:
                                    batch = cursor.fetchmany(1000)
                                    if not batch:
                                        break
                                    rows.extend(batch)
                            
                            formatted_result = self._format_query_results(cursor, rows, db, i)
                            message_queue.put(("result", formatted_result))
                            
                            row_count = len(rows) if cursor.description else cursor.rowcount
                            current_query_log['results'].append({
                                'database': db,
                                'statement_num': i,
                                'result': formatted_result,
                                'rowcount': row_count,
                                'success': True
                            })
                            if cursor.description:
                                total_rows += row_count
                                message_queue.put(("row_count", f"{row_count} rows from Query {i}"))
                                
                            try:
                                while cursor.nextset():
                                    pass
                            except pyodbc.ProgrammingError:
                                pass

                        except pyodbc.Error as e:
                            error_msg = f"\nError in Query {i} on {db}: {str(e)}\n"
                            message_queue.put(("result", error_msg))
                            current_query_log['results'].append({
                                'database': db,
                                'statement_num': i,
                                'result': error_msg,
                                'success': False,
                                'error': str(e)
                            })
                    conn.commit()

            except pyodbc.OperationalError as e:
                error_msg = f"\nConnection error with {db}: {str(e)}\n"
                message_queue.put(("result", error_msg))
                current_query_log['results'].append({
                    'database': db,
                    'statement_num': 0,
                    'result': error_msg,
                    'success': False,
                    'error': str(e)
                })

            except Exception as e:
                error_msg = f"\nUnexpected error with {db}: {str(e)}\n"
                message_queue.put(("result", error_msg))
                current_query_log['results'].append({
                    'database': db,
                    'statement_num': 0,
                    'result': error_msg,
                    'success': False,
                    'error': str(e)
                })

        exec_time = time.time() - start_time
        current_query_log['exec_time'] = exec_time
        current_query_log['total_rows'] = total_rows
        
        message_queue.put(("exec_time", f"Execution time: {exec_time:.2f}s"))
        message_queue.put(("total_rows", f"Total rows: {total_rows}"))
        message_queue.put(("done", "Query execution completed"))
        message_queue.put(("current_query_log", current_query_log))
        message_queue.put(("enable_log_button", True))

    def _format_query_results(self, cursor, rows, db_name, statement_num):
        """Helper to format query results for display."""
        if not cursor.description:
            return f"\n=== Query {statement_num} executed on {db_name} ===\nRows affected: {cursor.rowcount}\n"
        
        column_names = [c[0] for c in cursor.description]
        if not rows:
            return f"\n=== Results from Query {statement_num} on {db_name} ===\nNo rows returned\n"
        
        col_widths = [
            max(len(str(name)), *(len(str(row[i])) for row in rows))
            for i, name in enumerate(column_names)
        ]
        
        header = " | ".join(
            str(name).ljust(width) 
            for name, width in zip(column_names, col_widths)
        )
        sep = "-+-".join('-' * width for width in col_widths)
        body = "\n".join(
            " | ".join(
                str(value).ljust(col_widths[i]) 
                for i, value in enumerate(row)
            )
            for row in rows
        )
        return f"\n=== Results from Query {statement_num} on {db_name} ===\n{header}\n{sep}\n{body}\n"
