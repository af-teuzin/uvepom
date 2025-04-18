import os
import asyncio
import pandas as pd
from io import StringIO
import time
import gc
from dotenv import load_dotenv
import numpy as np
import argparse
import psycopg
import csv
import json

load_dotenv()  # Load environment variables

def import_excel(file_path: str, chunk_size=10000, sheet_name=0):
    """
    Import large Excel file and split into chunks after loading
    Returns a list of dataframe chunks
    """
    print(f"Starting import of {file_path}")
    print("Loading Excel file into memory (this may take some time for large files)...")
    
    # Load the entire file - pandas doesn't support chunking for Excel files
    df = pd.read_excel(file_path, engine='openpyxl', sheet_name=sheet_name)
    
    total_rows = len(df)
    print(f"Excel file loaded: {total_rows} rows and {len(df.columns)} columns")
    
    # Manually split the dataframe into chunks
    chunks = []
    for i in range(0, total_rows, chunk_size):
        end_idx = min(i + chunk_size, total_rows)
        chunks.append(df.iloc[i:end_idx].copy())
        print(f"Created chunk {len(chunks)}: rows {i} to {end_idx}")
    
    print(f"Split into {len(chunks)} chunks")
    
    # Free up memory
    del df
    gc.collect()
    
    return chunks

def get_connection_string():
    """Get the database connection string"""
    return (f"host={os.getenv('DB_HOST')} "
            f"port={os.getenv('DB_PORT')} "
            f"dbname={os.getenv('DB_NAME')} "
            f"user={os.getenv('DB_USER')} "
            f"password={os.getenv('DB_PASSWORD')}")

async def get_connection():
    """Get a database connection"""
    return await psycopg.AsyncConnection.connect(get_connection_string())

def prepare_data_for_copy(df, table_name):
    """
    Prepare dataframe for COPY operation
    Handles null values properly
    """
    # Make a copy to avoid modifying the original dataframe
    df_copy = df.copy()
    
    # Clean data to prevent issues with formatting and encoding
    for col in df_copy.columns:
        # Handle strings
        if df_copy[col].dtype == 'object':
            # Replace problematic characters and handle NaN values
            df_copy[col] = df_copy[col].apply(
                lambda x: str(x).replace('\t', ' ').replace('\n', ' ').replace('\r', '') if pd.notna(x) else None
            )
    
    # Replace NaN with None to properly handle NULL values in PostgreSQL
    df_copy = df_copy.where(pd.notna(df_copy), None)
    
    # Create a string buffer
    buffer = StringIO()
    
    # Write the dataframe to the buffer as a CSV with proper escaping
    # Use tab as separator to avoid issues with commas in the data
    df_copy.to_csv(
        buffer, 
        index=False, 
        header=False, 
        na_rep='\\N',
        sep='\t',
        quoting=csv.QUOTE_NONNUMERIC,
        escapechar='\\'
    )
    
    # Reset buffer position
    buffer.seek(0)
    
    return buffer

async def get_table_columns(conn, table_name):
    """Get the list of columns in the database table"""
    table_without_schema = table_name.split('.')[-1]
    schema_query = await conn.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, [table_without_schema])
    
    columns = await schema_query.fetchall()
    return [col[0] for col in columns]

def match_columns(df, db_columns):
    """Match Excel columns with database columns, dropping any that don't exist in the DB"""
    # Convert all column names to lowercase for case-insensitive comparison
    df_columns = [col.lower() for col in df.columns]
    db_columns_lower = [col.lower() for col in db_columns]
    
    # Create a mapping from Excel columns to DB columns
    column_mapping = {}
    columns_to_drop = []
    
    for i, excel_col in enumerate(df_columns):
        # Check if column exists in database (case-insensitive)
        if excel_col in db_columns_lower:
            # Map to the correctly cased DB column name
            db_idx = db_columns_lower.index(excel_col)
            column_mapping[df.columns[i]] = db_columns[db_idx]
        else:
            columns_to_drop.append(df.columns[i])
            print(f"Warning: Column '{df.columns[i]}' in Excel does not exist in the database table - will be ignored")
    
    # Don't drop columns yet as we might need them for creating JSON fields
    
    if column_mapping:
        df = df.rename(columns=column_mapping)
    
    return df, columns_to_drop

def create_affiliate_json(row):
    """Create a JSON object for the affiliate column from affiliate_name, affiliate_email, and affiliate_doc"""
    affiliate = {}
    
    # Add fields if they exist and are not null
    if 'affiliate_name' in row and pd.notna(row['affiliate_name']):
        affiliate['name'] = str(row['affiliate_name']).strip()
    
    if 'affiliate_email' in row and pd.notna(row['affiliate_email']):
        affiliate['email'] = str(row['affiliate_email']).strip()
    
    if 'affiliate_doc' in row and pd.notna(row['affiliate_doc']):
        affiliate['doc'] = str(row['affiliate_doc']).strip()
    
    # Return None if empty object
    if not affiliate:
        return None
    
    return affiliate

async def bulk_insert_chunk(conn, df, table_name):
    """
    Bulk insert a dataframe chunk using COPY
    """
    # First, get the database table columns
    db_columns = await get_table_columns(conn, table_name)
    
    # Make a copy and match columns with the database
    df_copy = df.copy()
    df_copy, columns_to_drop = match_columns(df_copy, db_columns)
    
    # Check if we need to create an affiliate JSON field
    affiliate_columns = [col for col in df.columns if col.lower().startswith('affiliate_')]
    has_affiliate_fields = any(col in df.columns for col in ['affiliate_name', 'affiliate_email', 'affiliate_doc'])
    
    if has_affiliate_fields and 'affiliate' in db_columns:
        print("Creating affiliate JSON objects from affiliate_name, affiliate_email, and affiliate_doc fields")
        
        # Create a temporary dataframe to hold the affiliate data
        temp_df = df_copy.copy()
        
        # Create the affiliate JSON field
        temp_df['affiliate'] = temp_df.apply(lambda row: json.dumps(create_affiliate_json(row)), axis=1)
        
        # Update the dataframe with the new affiliate field
        df_copy['affiliate'] = temp_df['affiliate']
    
    # Now drop columns that don't exist in the database
    df_copy = df_copy.drop(columns=[col for col in columns_to_drop if col in df_copy.columns])
    
    # Log the columns after matching
    print(f"Columns after matching: {list(df_copy.columns)}")
    
    # Clean data to prevent issues with formatting and encoding
    for col in df_copy.columns:
        # Handle strings
        if df_copy[col].dtype == 'object':
            # Replace problematic characters and handle NaN values
            df_copy[col] = df_copy[col].apply(
                lambda x: str(x).replace('\t', ' ').replace('\n', ' ').replace('\r', '') if pd.notna(x) else None
            )
    
    # Replace NaN with None to properly handle NULL values in PostgreSQL
    df_copy = df_copy.where(pd.notna(df_copy), None)
    
    # Get column names for direct execute
    columns = [f'"{column}"' for column in df_copy.columns]
    column_str = ", ".join(columns)
    
    # Use smaller batches for better performance and memory usage
    batch_size = 100  # Smaller batch size to reduce memory usage
    total_rows = len(df_copy)
    rows_inserted = 0
    
    for i in range(0, total_rows, batch_size):
        end_idx = min(i + batch_size, total_rows)
        batch_df = df_copy.iloc[i:end_idx]
        
        # Build placeholders and values
        placeholders = []
        values = []
        
        for _, row in batch_df.iterrows():
            row_values = list(row)
            placeholders.append("(" + ", ".join(["%s"] * len(columns)) + ")")
            values.extend(row_values)
        
        # Execute the insert
        batch_placeholders = ", ".join(placeholders)
        sql = f"INSERT INTO {table_name} ({column_str}) VALUES {batch_placeholders}"
        
        try:
            await conn.execute(sql, values)
            rows_inserted += len(batch_df)
        except Exception as e:
            print(f"Error inserting batch rows {i}-{end_idx}: {e}")
            if i == 0:
                # Print the first row for debugging
                print(f"First row columns: {batch_df.columns.tolist()}")
                print(f"First row values: {batch_df.iloc[0].tolist()}")
            raise
    
    return rows_inserted

async def process_chunk(chunk_idx, chunk, table_name):
    """Process a single dataframe chunk"""
    start_time = time.time()
    print(f"Processing chunk {chunk_idx}: {len(chunk)} rows")
    
    try:
        conn = await get_connection()
        try:
            async with conn.transaction():
                try:
                    rows_inserted = await bulk_insert_chunk(conn, chunk, table_name)
                    
                    duration = time.time() - start_time
                    print(f"Chunk {chunk_idx} completed: {rows_inserted} rows in {duration:.2f}s ({rows_inserted/duration:.2f} rows/s)")
                    
                    return rows_inserted
                except Exception as e:
                    print(f"Error in bulk_insert_chunk for chunk {chunk_idx}: {e}")
                    # Re-raise to trigger transaction rollback
                    raise
        finally:
            await conn.close()
            
        # Help garbage collection
        del chunk
        gc.collect()
        
    except Exception as e:
        print(f"Error processing chunk {chunk_idx}: {e}")
        raise

async def upload_to_database(df_chunks, table_name, max_workers=4):
    """
    Upload dataframe chunks to database using batch processing to manage memory
    """
    total_start_time = time.time()
    
    # Process chunks in sequential batches to manage memory better
    batch_size = min(max_workers, 4)  # Limit concurrent tasks to avoid memory issues
    chunk_count = len(df_chunks)
    
    print(f"Processing {chunk_count} chunks in batches of {batch_size}")
    
    successful_rows = 0
    failed_chunks = 0
    
    # Process in batches
    for batch_start in range(0, chunk_count, batch_size):
        batch_end = min(batch_start + batch_size, chunk_count)
        print(f"Processing batch of chunks {batch_start} to {batch_end-1}")
        
        # Create tasks for this batch
        tasks = []
        for i in range(batch_start, batch_end):
            tasks.append(process_chunk(i, df_chunks[i], table_name))
        
        # Execute batch tasks and gather results
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process batch results
        for i, result in enumerate(batch_results):
            chunk_idx = batch_start + i
            if isinstance(result, Exception):
                print(f"Error in chunk {chunk_idx}: {result}")
                failed_chunks += 1
            else:
                successful_rows += result
        
        # Clear processed chunks to free memory
        for i in range(batch_start, batch_end):
            df_chunks[i] = None
        
        gc.collect()  # Force garbage collection between batches
    
    total_duration = time.time() - total_start_time
    
    print(f"Upload completed: {successful_rows} rows successfully inserted in {total_duration:.2f}s")
    print(f"Average speed: {successful_rows/total_duration:.2f} rows/s")
    
    if failed_chunks > 0:
        print(f"WARNING: {failed_chunks} chunks failed to insert")
    
    return successful_rows

async def test_connection(table_name):
    """Test database connection and print table schema"""
    print("Testing database connection...")
    
    try:
        conn = await get_connection()
        try:
            # Test connection with a simple query
            result = await conn.execute("SELECT 1")
            first_result = await result.fetchone()
            print(f"Connection test successful: {first_result}")
            
            # Get table schema
            table_without_schema = table_name.split('.')[-1]
            schema_query = await conn.execute("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, [table_without_schema])
            
            columns = await schema_query.fetchall()
            if columns:
                print(f"Table schema for {table_name}:")
                for col in columns:
                    print(f"  {col[0]}: {col[1]} (nullable: {col[2]})")
            else:
                print(f"Table {table_name} not found or has no columns")
            
            return True
        finally:
            await conn.close()
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Upload Excel data to PostgreSQL')
    parser.add_argument('--file', type=str, default="vendas_consolidadas.xlsx", help='Path to Excel file')
    parser.add_argument('--table', type=str, default="hytallo_soares.transactions_v3", help='Target table name')
    parser.add_argument('--chunk-size', type=int, default=10000, help='Chunk size for processing')
    parser.add_argument('--sheet', type=int, default=0, help='Sheet index to import (0-based)')
    parser.add_argument('--max-workers', type=int, default=4, help='Maximum number of parallel workers')
    parser.add_argument('--test-only', action='store_true', help='Only test the connection without uploading')
    parser.add_argument('--test-rows', type=int, default=0, help='Number of rows to process for testing (0 = all rows)')
    
    args = parser.parse_args()
    
    start_time = time.time()
    
    try:
        # Test connection first
        conn_ok = await test_connection(args.table)
        if not conn_ok:
            print("Aborting due to connection test failure")
            return 1
            
        if args.test_only:
            print("Connection test successful. Exiting without uploading data.")
            return 0
        
        # Import Excel file in chunks
        df_chunks = import_excel(args.file, chunk_size=args.chunk_size, sheet_name=args.sheet)
        
        # If test-rows is specified, only process the first few chunks
        if args.test_rows > 0:
            total_rows = 0
            test_chunks = []
            for chunk in df_chunks:
                test_chunks.append(chunk)
                total_rows += len(chunk)
                if total_rows >= args.test_rows:
                    break
            print(f"TEST MODE: Processing only {total_rows} rows from {len(test_chunks)} chunks")
            df_chunks = test_chunks
        
        # Upload chunks to database
        await upload_to_database(df_chunks, args.table, max_workers=args.max_workers)
        
    except Exception as e:
        print(f"ERROR: {e}")
        return 1
    finally:
        total_time = time.time() - start_time
        print(f"Total execution time: {total_time:.2f} seconds")
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    import sys
    sys.exit(exit_code)