#!/usr/bin/env python3
"""
Batch SQL Conversion Script

This script processes all Hive SQL files in a specified directory, converts them to BigQuery SQL,
and saves the results and a detailed report.
"""

import os
import sys
import glob
import time
import json
import logging
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Add project root to sys.path
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

# Load environment variables
load_dotenv(os.path.join(project_root, ".env"))

from src.agent.graph import run_conversion
from src.schemas.models import ConvertResponse, ConversionHistory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

INPUT_DIR = os.path.join(project_root, "tests", "sql", "new")
OUTPUT_DIR = os.path.join(project_root, "tests", "sql", "new") # Save in same directory as requested? 
# "文件名有原来的文件名加上成功或者失败的提示" -> suggests same directory or a result directory. 
# The user said "tests/sql/new 当前文件夹下... bq转化结果保存成文件" -> implies same directory or subfolder. 
# I'll output to the same directory to be safe, or maybe a 'results' subdirectory inside it to avoid clutter?
# User said: "文件名有原来的文件名加上成功或者失败的提示" (Filename has original filename plus success or failure hint)
# I will output to the same directory.

def generate_md_report(filename: str, result: Dict[str, Any], duration: float) -> str:
    """Generates a Markdown report for the conversion result."""
    
    status = "Success" if result.get("validation_success") and result.get("hive_valid") else "Failure"
    status_icon = "✅" if status == "Success" else "❌"
    
    md = f"# Conversion Report: {filename}\n\n"
    md += f"**Date**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
    md += f"**Status**: {status_icon} {status}\n"
    md += f"**Duration**: {duration:.2f}s\n"
    md += f"**Retries**: {result.get('retry_count', 0)}\n\n"
    
    md += "## 1. Validation Result\n"
    md += f"- **Hive Valid**: {'Yes' if result.get('hive_valid') else 'No'}\n"
    if result.get('hive_error'):
        md += f"  - Error: `{result.get('hive_error')}`\n"
    md += f"- **BigQuery Validation**: {'Passed' if result.get('validation_success') else 'Failed'}\n"
    md += f"- **Mode**: {result.get('validation_mode', 'N/A')}\n"
    if result.get('validation_error'):
        md += f"- **Error**: \n```\n{result.get('validation_error')}\n```\n"
        
    md += "\n## 2. Final BigQuery SQL\n"
    if result.get('bigquery_sql'):
        md += "```sql\n"
        md += result.get('bigquery_sql')
        md += "\n```\n"
    else:
        md += "*No BigQuery SQL generated.*\n"
        
    md += "\n## 3. Input Hive SQL\n"
    md += "```sql\n"
    md += result.get('hive_sql', '').strip()
    md += "\n```\n"
    
    md += "\n## 4. Conversion History\n"
    history = result.get('conversion_history', [])
    if not history:
        md += "*No history available.*\n"
    else:
        for entry in history:
            # Entry might be an object or dict depending on how run_conversion returns state
            # AgentState defines it as list[ConversionHistory] which are Pydantic models usually, 
            # but inside the graph state they might be dicts or objects. 
            # Let's handle both.
            if hasattr(entry, 'attempt'):
                attempt = entry.attempt
                error = entry.error
                sql = entry.bigquery_sql
            elif isinstance(entry, dict):
                attempt = entry.get('attempt')
                error = entry.get('error')
                sql = entry.get('bigquery_sql')
            else:
                attempt = "Unknown"
                error = str(entry)
                sql = ""

            md += f"### Attempt {attempt}\n"
            if error:
                md += f"**Error**:\n```\n{error}\n```\n"
            md += f"<details>\n<summary>Generated SQL</summary>\n\n```sql\n{sql}\n```\n\n</details>\n\n"
            
    return md

def process_file(filepath: str):
    filename = os.path.basename(filepath)
    logger.info(f"Processing {filename}...")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            hive_sql = f.read()
            
        start_time = time.time()
        result = run_conversion(hive_sql)
        duration = time.time() - start_time
        
        success = result.get("validation_success") and result.get("hive_valid")
        
        # Determine output filenames
        base_name = os.path.splitext(filename)[0]
        suffix = "_success" if success else "_failed"
        
        sql_output_path = os.path.join(OUTPUT_DIR, f"{base_name}{suffix}.sql")
        md_output_path = os.path.join(OUTPUT_DIR, f"{base_name}_report.md")
        
        # Write SQL output
        bq_sql = result.get("bigquery_sql")
        if bq_sql:
            with open(sql_output_path, 'w', encoding='utf-8') as f:
                f.write(bq_sql)
            logger.info(f"Saved SQL to {sql_output_path}")
        else:
            logger.warning(f"No SQL generated for {filename}")
            
        # Write MD report
        md_content = generate_md_report(filename, result, duration)
        with open(md_output_path, 'w', encoding='utf-8') as f:
            f.write(md_content)
        logger.info(f"Saved Report to {md_output_path}")
        
    except Exception as e:
        logger.error(f"Failed to process {filename}: {e}", exc_info=True)

def main():
    if not os.path.exists(INPUT_DIR):
        logger.error(f"Input directory not found: {INPUT_DIR}")
        return

    files = glob.glob(os.path.join(INPUT_DIR, "*.txt"))
    # Also include .sql files if any
    files.extend(glob.glob(os.path.join(INPUT_DIR, "*.sql")))
    
    files = sorted(list(set(files))) # deduplicate and sort
    
    logger.info(f"Found {len(files)} files in {INPUT_DIR}")
    
    for filepath in files:
        process_file(filepath)
        
    logger.info("Batch conversion completed.")

if __name__ == "__main__":
    main()
