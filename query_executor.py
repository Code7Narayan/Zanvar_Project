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
        successful_executions = 0
        failed_executions = 0
        total_statements = 0
        
        current_query_log = {
            'query': query,
            'databases': databases,
            'start_time': datetime.now(),
            'results': []
        }
        
        # Split into multiple SQL statements safely
        statements = [stmt.strip() for stmt in sqlparse.split(query) if stmt.strip()]
        total_statements = len(statements)
        
        # Store all results to reorder them later
        all_results = []
        
        for db_index, db in enumerate(databases):
            db_start_time = time.time()
            db_successful_statements = 0
            db_failed_statements = 0
            db_total_rows = 0
            
            try:
                message_queue.put(("status", f"Querying {db}... ({db_index + 1}/{len(databases)})"))
                with self.sql_connector.get_connection(db) as conn:
                    cursor = conn.cursor()

                    db_results = []
                    for i, statement in enumerate(statements, 1):
                        statement_start_time = time.time()
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
                            
                            statement_exec_time = time.time() - statement_start_time
                            row_count = len(rows) if cursor.description else cursor.rowcount
                            
                            formatted_result = self._format_query_results(
                                cursor, rows, db, i, statement_exec_time, 
                                formatted_sql, row_count, True
                            )
                            
                            db_results.append(formatted_result)
                            db_successful_statements += 1
                            successful_executions += 1
                            
                            current_query_log['results'].append({
                                'database': db,
                                'statement_num': i,
                                'result': formatted_result,
                                'rowcount': row_count,
                                'execution_time': statement_exec_time,
                                'success': True
                            })
                            
                            if cursor.description:
                                total_rows += row_count
                                db_total_rows += row_count
                                message_queue.put(("row_count", f"{row_count} rows from Query {i} on {db}"))
                                
                            try:
                                while cursor.nextset():
                                    pass
                            except pyodbc.ProgrammingError:
                                pass

                        except pyodbc.Error as e:
                            statement_exec_time = time.time() - statement_start_time
                            error_msg = self._format_error_result(db, i, str(e), statement_exec_time, formatted_sql)
                            db_results.append(error_msg)
                            db_failed_statements += 1
                            failed_executions += 1
                            
                            current_query_log['results'].append({
                                'database': db,
                                'statement_num': i,
                                'result': error_msg,
                                'execution_time': statement_exec_time,
                                'success': False,
                                'error': str(e)
                            })
                    
                    conn.commit()
                    
                    # Create database summary
                    db_exec_time = time.time() - db_start_time
                    db_summary = self._format_database_summary(
                        db, db_successful_statements, db_failed_statements, 
                        db_total_rows, db_exec_time, len(statements)
                    )
                    
                    # Combine database summary with results
                    combined_db_result = db_summary + "\n".join(db_results)
                    all_results.append((db, combined_db_result, db_index))

            except pyodbc.OperationalError as e:
                db_exec_time = time.time() - db_start_time
                error_msg = self._format_connection_error(db, str(e), db_exec_time)
                all_results.append((db, error_msg, db_index))
                failed_executions += len(statements)
                
                current_query_log['results'].append({
                    'database': db,
                    'statement_num': 0,
                    'result': error_msg,
                    'execution_time': db_exec_time,
                    'success': False,
                    'error': str(e)
                })

            except Exception as e:
                db_exec_time = time.time() - db_start_time
                error_msg = self._format_unexpected_error(db, str(e), db_exec_time)
                all_results.append((db, error_msg, db_index))
                failed_executions += len(statements)
                
                current_query_log['results'].append({
                    'database': db,
                    'statement_num': 0,
                    'result': error_msg,
                    'execution_time': db_exec_time,
                    'success': False,
                    'error': str(e)
                })

        # Calculate final execution time
        exec_time = time.time() - start_time
        current_query_log['exec_time'] = exec_time
        current_query_log['total_rows'] = total_rows
        
        # Create and send overall summary
        overall_summary = self._format_overall_summary(
            databases, total_statements, successful_executions, 
            failed_executions, total_rows, exec_time
        )
        message_queue.put(("result", overall_summary))
        
        # Sort results so last database appears first, then others in reverse order
        all_results.sort(key=lambda x: x[2], reverse=True)
        
        # Send individual database results
        for db_name, db_result, _ in all_results:
            message_queue.put(("result", db_result))
        
        # Send final status updates
        message_queue.put(("exec_time", f"Total Execution Time: {exec_time:.2f}s"))
        message_queue.put(("total_rows", f"Total Rows Processed: {total_rows:,}"))
        message_queue.put(("done", "Query execution completed"))
        message_queue.put(("current_query_log", current_query_log))
        message_queue.put(("enable_log_button", True))

    def _format_overall_summary(self, databases, total_statements, successful_executions, failed_executions, total_rows, exec_time):
        """Format the overall execution summary with precise alignment."""
        total_executions = successful_executions + failed_executions
        success_rate = (successful_executions / total_executions * 100) if total_executions > 0 else 0
        
        # Fixed width for consistent formatting
        box_width = 82
        content_width = box_width - 4  # Account for "║ " and " ║"
        
        def format_line(text):
            """Helper to format a line with exact padding."""
            return f"║ {text:<{content_width}}"
        
        def format_two_column_line(left_text, right_text):
            """Helper to format two-column lines with exact alignment."""
            left_width = content_width // 2 - 1  # -1 for the separator
            right_width = content_width - left_width - 3  # -3 for " │ "
            return f"║ {left_text:<{left_width}} │ {right_text:<{right_width}}"
        
        # Create all lines with consistent formatting
        title_padding = (content_width - 27) // 2
        title_line = f"║{' ' * title_padding}📊 OVERALL EXECUTION SUMMARY{' ' * (content_width - 27 - title_padding)}"
        
        timestamp_line = format_line(f"Execution Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        line1 = format_two_column_line(f"Total Databases Queried: {len(databases)}", f"Total Statements: {total_statements}")
        line2 = format_two_column_line(f"Successful Executions: {successful_executions}", f"Failed Executions: {failed_executions}")
        line3 = format_two_column_line(f"Success Rate: {success_rate:.1f}%", f"Total Rows: {total_rows:,}")
        line4 = format_line(f"Total Execution Time: {exec_time:.2f}s")
        
        # Handle database list with proper truncation
        db_list = ', '.join(databases)
        max_db_length = content_width - 32  # Account for the prefix text
        if len(db_list) > max_db_length:
            db_list = db_list[:max_db_length - 3] + "..."
        db_line = format_line(f"Databases Executed (in order): {db_list}")
        
        
        
        # Create borders
        top_border = f"╔{'═' * box_width}"
        middle_border = f"╠{'═' * box_width}"
        bottom_border = f"╚{'═' * box_width}"
        
        summary = f"""
{top_border}
{title_line}
{middle_border}
{timestamp_line}
{line1}
{line2}
{line3}
{line4}
{middle_border}
{db_line}
{middle_border}
{bottom_border}
"""
        return summary

    def _format_database_summary(self, db_name, successful, failed, total_rows, exec_time, total_statements):
        """Format individual database execution summary with precise alignment."""
        box_width = 80
        content_width = box_width - 4  # Account for "│ " and " │"
        
        def format_line(text):
            """Helper to format a line with exact padding."""
            return f"│ {text:<{content_width}} "
        
        def format_three_column_line(col1, col2, col3):
            """Helper to format three-column lines with exact alignment."""
            # Calculate column widths to fit exactly in content_width
            col1_width = 25  # Fixed width for first column
            col2_width = 32  # Fixed width for second column  
            col3_width = content_width - col1_width - col2_width - 6  # Remaining space minus separators " │ " x2
            
            return f"│ {col1:<{col1_width}} │ {col2:<{col2_width}} │ {col3:<{col3_width}} "
        
        # Format the database name line
        db_text = f"🏢 DATABASE: {db_name.upper()}"
        db_line = format_line(db_text)
        
        # Format the statistics line
        exec_text = f"Execution Time: {exec_time:.2f}s"
        statements_text = f"Statements: {successful}/{total_statements} successful"
        rows_text = f"Rows: {total_rows:,}"
        
        stats_line = format_three_column_line(exec_text, statements_text, rows_text)
        
        # Create borders
        top_border = f"┌{'─' * box_width}"
        middle_border = f"├{'─' * box_width}"
        bottom_border = f"└{'─' * box_width}"
        
        return f"""
{top_border}
{db_line}
{middle_border}
{stats_line}
{bottom_border}
"""

    def _format_query_results(self, cursor, rows, db_name, statement_num, exec_time, sql_query, row_count, success):
        """Enhanced helper to format query results for display with precise alignment."""
        # Get first 50 characters of query for display
        query_preview = sql_query.replace('\n', ' ').strip()
        if len(query_preview) > 50:
            query_preview = query_preview[:47] + "..."
        
        # Fixed box width for consistent formatting
        box_width = 80
        content_width = box_width - 4  # Account for "│ " and " │"
        
        def format_line(text):
            """Helper to format a line with exact padding."""
            return f"│ {text:<{content_width}} "
        
        # Create dynamic header with proper border calculation
        header_text = f"Statement {statement_num} Results"
        header_padding = max(0, (box_width - len(header_text) - 6) // 2)  # 6 for "┌─ " and " ─"
        remaining_padding = box_width - len(header_text) - 6 - header_padding
        border_line = f"┌─ {header_text} {'─' * header_padding}{'─' * remaining_padding}"
        end_line = f"└{'─' * box_width}"
        
        # Format content lines with consistent width
        query_line = format_line(f"🎯 Query: {query_preview}")
        time_line = format_line(f"⏱️  Execution Time: {exec_time:.3f}s")
        
        if not cursor.description:
            status_line = format_line(f"✅ Status: SUCCESS - {cursor.rowcount} rows affected")
            type_line = format_line("📊 Result Type: Non-Query (INSERT/UPDATE/DELETE/CREATE/etc.)")
            
            return f"""
{border_line}
{query_line}
{time_line}
{status_line}
{type_line}
{end_line}
"""
        
        column_names = [c[0] for c in cursor.description]
        if not rows:
            status_line = format_line("✅ Status: SUCCESS - No rows returned")
            columns_preview = ', '.join(column_names[:5])
            if len(column_names) > 5:
                columns_preview += ', ...'
            columns_text = f"📊 Columns: {len(column_names)} columns ({columns_preview})"
            # Truncate if too long
            if len(columns_text) > content_width:
                columns_text = columns_text[:content_width - 3] + "..."
            columns_line = format_line(columns_text)
            
            return f"""
{border_line}
{query_line}
{time_line}
{status_line}
{columns_line}
{end_line}

No data rows to display.
"""
        
        status_line = format_line(f"✅ Status: SUCCESS - {row_count:,} rows returned")
        columns_line = format_line(f"📊 Columns: {len(column_names)} columns")
        
        # Format data table with precise column width calculations
        # Calculate available width for the data table
        available_width = 100  # Use a reasonable width for data display
        
        # Calculate column widths
        col_widths = []
        for i, name in enumerate(column_names):
            # Start with header width
            max_width = len(str(name))
            # Check data widths
            for row in rows:
                value_str = str(row[i]) if row[i] is not None else 'NULL'
                max_width = max(max_width, len(value_str))
            # Limit individual column width
            col_widths.append(min(max_width, 25))
        
        # Adjust column widths if total is too wide
        total_width = sum(col_widths) + len(col_widths) * 3 - 1  # 3 chars per separator, -1 for last
        if total_width > available_width:
            # Proportionally reduce all columns
            scale_factor = (available_width - len(col_widths) * 3 + 1) / sum(col_widths)
            col_widths = [max(5, int(w * scale_factor)) for w in col_widths]
        
        # Format table header with exact alignment
        header_parts = []
        for i, (name, width) in enumerate(zip(column_names, col_widths)):
            header_parts.append(str(name)[:width].ljust(width))
        header = " │ ".join(header_parts)
        
        # Create separator line with exact width
        sep_parts = ['─' * width for width in col_widths]
        sep = "─┼─".join(sep_parts)
        
        # Format data rows with exact alignment
        display_rows = rows[:100]  # Show first 100 rows max
        body_lines = []
        for row in display_rows:
            row_parts = []
            for i, (value, width) in enumerate(zip(row, col_widths)):
                value_str = str(value) if value is not None else 'NULL'
                row_parts.append(value_str[:width].ljust(width))
            body_lines.append(" │ ".join(row_parts))
        
        body = "\n".join(body_lines)
        truncation_note = f"\n[Note: Showing first 100 rows. Total rows: {len(rows):,}]" if len(rows) > 100 else ""
        
        return f"""
{border_line}
{query_line}
{time_line}
{status_line}
{columns_line}
{end_line}

{header}
{sep}
{body}{truncation_note}

"""

    def _format_error_result(self, db_name, statement_num, error_msg, exec_time, sql_query):
        """Format error results with precise alignment."""
        query_preview = sql_query.replace('\n', ' ').strip()
        if len(query_preview) > 50:
            query_preview = query_preview[:47] + "..."
        
        # Fixed box width for consistent formatting
        box_width = 80
        content_width = box_width - 4  # Account for "│ " and " │"
        
        def format_line(text):
            """Helper to format a line with exact padding."""
            return f"│ {text:<{content_width}} "
        
        # Create dynamic header with proper border calculation
        header_text = f"Statement {statement_num} Results"
        header_padding = max(0, (box_width - len(header_text) - 6) // 2)  # 6 for "┌─ " and " ─"
        remaining_padding = box_width - len(header_text) - 6 - header_padding
        border_line = f"┌─ {header_text} {'─' * header_padding}{'─' * remaining_padding}"
        end_line = f"└{'─' * box_width}"
        
        # Format content lines with consistent width
        query_line = format_line(f"🎯 Query: {query_preview}")
        time_line = format_line(f"⏱️  Execution Time: {exec_time:.3f}s")
        status_line = format_line("❌ Status: ERROR")
            
        return f"""
{border_line}
{query_line}
{time_line}
{status_line}
{end_line}

🚨 Error Details:
{error_msg}

"""

    def _format_connection_error(self, db_name, error_msg, exec_time):
        """Format connection error with precise alignment."""
        box_width = 80
        content_width = box_width - 4  # Account for "│ " and " │"
        
        def format_line(text):
            """Helper to format a line with exact padding."""
            return f"│ {text:<{content_width}} "
        
        # Create dynamic header with proper border calculation
        header_text = "Database Connection Error"
        header_padding = max(0, (box_width - len(header_text) - 6) // 2)  # 6 for "┌─ " and " ─"
        remaining_padding = box_width - len(header_text) - 6 - header_padding
        border_line = f"┌─ {header_text} {'─' * header_padding}{'─' * remaining_padding}"
        end_line = f"└{'─' * box_width}"
        
        # Format content lines with consistent width
        db_line = format_line(f"🏢 Database: {db_name}")
        time_line = format_line(f"⏱️  Attempted for: {exec_time:.3f}s")
        status_line = format_line("❌ Status: CONNECTION FAILED")
        
        return f"""
{border_line}
{db_line}
{time_line}
{status_line}
{end_line}

🚨 Connection Error Details:
{error_msg}

"""

    def _format_unexpected_error(self, db_name, error_msg, exec_time):
        """Format unexpected error with precise alignment."""
        box_width = 80
        content_width = box_width - 4  # Account for "│ " and " │"
        
        def format_line(text):
            """Helper to format a line with exact padding."""
            return f"│ {text:<{content_width}} "
        
        # Create dynamic header with proper border calculation
        header_text = "Unexpected Error"
        header_padding = max(0, (box_width - len(header_text) - 6) // 2)  # 6 for "┌─ " and " ─"
        remaining_padding = box_width - len(header_text) - 6 - header_padding
        border_line = f"┌─ {header_text} {'─' * header_padding}{'─' * remaining_padding}"
        end_line = f"└{'─' * box_width}"
        
        # Format content lines with consistent width
        db_line = format_line(f"🏢 Database: {db_name}")
        time_line = format_line(f"⏱️  Execution Time: {exec_time:.3f}s")
        status_line = format_line("❌ Status: UNEXPECTED ERROR")
        
        return f"""
{border_line}
{db_line}
{time_line}
{status_line}
{end_line}

🚨 Error Details:
{error_msg}

"""