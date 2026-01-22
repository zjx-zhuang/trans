"""Node implementations for LangGraph workflow."""

import json
import logging
import os
from typing import Any

from src.agent.state import AgentState
from src.prompts.templates import (
    FIX_BIGQUERY_PROMPT,
    HIVE_TO_BIGQUERY_PROMPT,
    HIVE_VALIDATION_PROMPT,
)
from src.schemas.models import ConversionHistory
from src.services.llm import get_llm
from src.services.sql_chunker import SQLChunker, ChunkedConverter
from src.services.table_mapping import get_table_mapping_service
from src.services.validation import validate_bigquery_sql

# Configure logger
logger = logging.getLogger(__name__)


def is_hive_validation_enabled() -> bool:
    """Check if Hive validation is enabled via environment variable.
    
    Returns:
        True if Hive validation is enabled, False otherwise.
    """
    mode = os.getenv("HIVE_VALIDATION_MODE", "enabled").lower()
    return mode not in ("disabled", "skip", "off", "false", "0")


def validate_hive_node(state: AgentState) -> dict[str, Any]:
    """Validate the input Hive SQL syntax.
    
    Args:
        state: Current agent state containing hive_sql.
        
    Returns:
        Updated state with hive_valid and hive_error.
    """
    logger.info("=" * 60)
    logger.info("[Node: validate_hive] Starting Hive SQL validation")
    logger.info(f"[Node: validate_hive] Input SQL: {len(state['hive_sql'])} chars")
    
    # Check if Hive validation is enabled
    if not is_hive_validation_enabled():
        logger.info("[Node: validate_hive] ⏭ Hive validation is DISABLED, skipping...")
        return {
            "hive_valid": True,
            "hive_error": None,
        }
    
    llm = get_llm()
    
    prompt = HIVE_VALIDATION_PROMPT.format(hive_sql=state["hive_sql"])
    response = llm.invoke(prompt)
    
    logger.debug(f"LLM raw response: {response.content}")
    
    # Parse the JSON response
    try:
        # Clean up response - remove markdown code blocks if present
        response_text = response.content.strip()
        if response_text.startswith("```"):
            # Remove markdown code block
            lines = response_text.split("\n")
            response_text = "\n".join(lines[1:-1])
        
        result = json.loads(response_text)
        is_valid = result.get("is_valid", False)
        error = result.get("error")
        
        if is_valid:
            logger.info("[Node: validate_hive] ✓ Hive SQL is valid")
        else:
            logger.warning(f"[Node: validate_hive] ✗ Hive SQL is invalid: {error}")
        
        return {
            "hive_valid": is_valid,
            "hive_error": error if not is_valid else None,
        }
    except json.JSONDecodeError:
        # If we can't parse the response, assume invalid with the raw response as error
        logger.error(f"[Node: validate_hive] Failed to parse LLM response: {response.content}")
        return {
            "hive_valid": False,
            "hive_error": f"Failed to validate Hive SQL: {response.content}",
        }


def table_mapping_node(state: AgentState) -> dict[str, Any]:
    """Replace Hive table names with BigQuery table names in the input SQL.
    
    Args:
        state: Current agent state containing hive_sql.
        
    Returns:
        Updated state with mapped hive_sql or mapping_error.
    """
    logger.info("=" * 60)
    logger.info("[Node: table_mapping] Starting table name mapping")
    
    hive_sql = state["hive_sql"]
    table_mapping_service = get_table_mapping_service()
    
    # Validate and replace table names
    mapped_sql, unmapped_tables = table_mapping_service.validate_and_replace(hive_sql)
    
    if unmapped_tables:
        error_msg = f"Mapping failed: No BigQuery mapping found for tables: {', '.join(unmapped_tables)}"
        logger.error(f"[Node: table_mapping] ✗ {error_msg}")
        return {
            "hive_sql": mapped_sql,
            "mapping_error": error_msg
        }
    
    if mapped_sql != hive_sql:
        logger.info("[Node: table_mapping] ✓ Table names mapped")
    else:
        logger.info("[Node: table_mapping] No table names needed mapping")
        
    return {
        "hive_sql": mapped_sql,
        "mapping_error": None
    }


def _convert_single_chunk(hive_sql: str) -> str:
    """Convert a single SQL chunk using LLM.
    
    Args:
        hive_sql: The Hive SQL chunk to convert.
        
    Returns:
        The converted BigQuery SQL.
    """
    llm = get_llm()
    
    prompt = HIVE_TO_BIGQUERY_PROMPT.format(
        hive_sql=hive_sql,
    )
    response = llm.invoke(prompt)
    
    # Clean up response - remove markdown code blocks if present
    bigquery_sql = response.content.strip()
    if bigquery_sql.startswith("```"):
        lines = bigquery_sql.split("\n")
        # Remove first line (```sql or ```) and last line (```)
        bigquery_sql = "\n".join(lines[1:-1]).strip()
    
    return bigquery_sql


def convert_node(state: AgentState) -> dict[str, Any]:
    """Convert Hive SQL to BigQuery SQL.
    
    For long SQL statements, this function will:
    1. Analyze the SQL structure (CTE, UNION, INSERT...SELECT, etc.)
    2. Split into manageable chunks
    3. Convert each chunk separately
    4. Merge the results
    
    Args:
        state: Current agent state containing hive_sql.
        
    Returns:
        Updated state with bigquery_sql.
    """
    logger.info("=" * 60)
    logger.info("[Node: convert] Starting Hive to BigQuery conversion")
    
    hive_sql = state['hive_sql']
    sql_length = len(hive_sql)
    sql_lines = hive_sql.count('\n')
    
    logger.info(f"[Node: convert] Input SQL: {sql_length} chars, {sql_lines} lines")
    
    # Check if chunking is needed
    chunker = SQLChunker(hive_sql)
    use_chunking = chunker.should_chunk()
    
    # Also check environment variable to force/disable chunking
    chunking_mode = os.getenv("SQL_CHUNKING_MODE", "auto").lower()
    if chunking_mode == "disabled":
        use_chunking = False
        logger.info("[Node: convert] SQL chunking disabled by configuration")
    elif chunking_mode == "always":
        use_chunking = True
        logger.info("[Node: convert] SQL chunking forced by configuration")
    
    if use_chunking:
        logger.info("[Node: convert] Using chunked conversion strategy")
        
        # Analyze and chunk
        chunks = chunker.analyze_and_chunk()
        
        if len(chunks) > 1:
            logger.info(f"[Node: convert] Split into {len(chunks)} chunks")
            
            # Create converter with the single-chunk converter function
            def converter_func(sql: str) -> str:
                return _convert_single_chunk(sql)
            
            chunked_converter = ChunkedConverter(converter_func)
            bigquery_sql = chunked_converter.convert_chunks(chunks)
            
            logger.info("[Node: convert] Chunked conversion completed")
        else:
            # Only one chunk, convert normally
            logger.info("[Node: convert] SQL analyzed but no chunking needed")
            bigquery_sql = _convert_single_chunk(hive_sql)
    else:
        # Direct conversion without chunking
        logger.info("[Node: convert] Using direct conversion (no chunking)")
        bigquery_sql = _convert_single_chunk(hive_sql)
    
    logger.info(f"[Node: convert] ✓ Conversion completed ({len(bigquery_sql)} chars)")
    
    return {
        "bigquery_sql": bigquery_sql,
        "retry_count": 0,
        "conversion_history": [],
    }


def validate_node(state: AgentState) -> dict[str, Any]:
    """Validate BigQuery SQL using configured validation mode.
    
    The validation mode is controlled by BQ_VALIDATION_MODE environment variable:
    - "dry_run": Use BigQuery API dry run (default)
    - "llm": Use LLM prompt-based validation
    
    Args:
        state: Current agent state containing bigquery_sql.
        
    Returns:
        Updated state with validation_success, validation_error, and updated conversion_history.
    """
    attempt = len(state.get("conversion_history", [])) + 1
    
    logger.info("=" * 60)
    logger.info(f"[Node: validate] Starting BigQuery SQL validation (attempt {attempt})")
    
    result = validate_bigquery_sql(state["bigquery_sql"])
    
    logger.info(f"[Node: validate] Validation mode: {result.validation_mode}")
    
    if result.success:
        logger.info(f"[Node: validate] ✓ BigQuery SQL validation passed")
    else:
        logger.error("=" * 60)
        logger.error(f"[Node: validate] ✗ BigQuery SQL validation FAILED (attempt {attempt})")
        logger.error(f"[Node: validate] Error Details:")
        logger.error("-" * 40)
        # 打印完整的错误信息，每行都打印
        for line in str(result.error_message).split('\n'):
            logger.error(f"  {line}")
        logger.error("-" * 40)
    
    # Update conversion history
    history = list(state.get("conversion_history", []))
    history.append(
        ConversionHistory(
            attempt=attempt,
            bigquery_sql=state["bigquery_sql"],
            error=result.error_message if not result.success else None,
        )
    )
    
    return {
        "validation_success": result.success,
        "validation_error": result.error_message,
        "validation_mode": result.validation_mode,
        "conversion_history": history,
    }


def fix_node(state: AgentState) -> dict[str, Any]:
    """Fix BigQuery SQL based on validation error.
    
    Args:
        state: Current agent state containing bigquery_sql and validation_error.
        
    Returns:
        Updated state with corrected bigquery_sql and incremented retry_count.
    """
    retry_count = state["retry_count"] + 1
    
    logger.info("=" * 60)
    logger.info(f"[Node: fix] Starting SQL fix (retry {retry_count})")
    logger.info(f"[Node: fix] Previous error: {state['validation_error']}")
    
    llm = get_llm()
    
    # Get table mapping information
    table_mapping_service = get_table_mapping_service()
    
    # Format conversion history for the prompt
    history_str = ""
    for entry in state.get("conversion_history", []):
        history_str += f"\nAttempt {entry.attempt}:\n"
        history_str += f"SQL: {entry.bigquery_sql}\n"
        if entry.error:
            history_str += f"Error: {entry.error}\n"
    
    if not history_str:
        history_str = "No previous attempts."
    
    prompt = FIX_BIGQUERY_PROMPT.format(
        hive_sql=state["hive_sql"],
        bigquery_sql=state["bigquery_sql"],
        error_message=state["validation_error"],
        conversion_history=history_str,
    )
    
    response = llm.invoke(prompt)
    
    # Clean up response - remove markdown code blocks if present
    fixed_sql = response.content.strip()
    if fixed_sql.startswith("```"):
        lines = fixed_sql.split("\n")
        fixed_sql = "\n".join(lines[1:-1]).strip()
    
    # Apply table name replacement as a safety net
    fixed_sql = table_mapping_service.replace_table_names(fixed_sql)
    
    logger.info(f"[Node: fix] ✓ SQL fix completed ({len(fixed_sql)} chars)")
    
    return {
        "bigquery_sql": fixed_sql,
        "retry_count": retry_count,
    }
