"""LangGraph workflow definition for Hive to BigQuery SQL conversion."""

import os
from typing import Literal

from langgraph.graph import END, StateGraph

from src.agent.nodes import (
    convert_node,
    validate_node,
    fix_node,
    validate_hive_node,
    table_mapping_node,
)
from src.agent.state import AgentState


def get_max_retries() -> int:
    """Get the maximum number of retries from environment variable.
    
    Returns:
        Maximum retry count, defaults to 1.
    """
    try:
        return int(os.getenv("MAX_RETRIES", "15"))
    except ValueError:
        return 1


def should_continue_after_hive_validation(state: AgentState) -> Literal["table_mapping", "end"]:
    """Determine if we should continue after Hive validation.
    
    Args:
        state: Current agent state.
        
    Returns:
        "table_mapping" if Hive SQL is valid, "end" otherwise.
    """
    if state["hive_valid"]:
        return "table_mapping"
    return "end"


def should_retry_after_validation(state: AgentState) -> Literal["fix", "end"]:
    """Determine if we should retry after BigQuery validation.
    
    Args:
        state: Current agent state.
        
    Returns:
        "fix" if validation failed and retries available, "end" otherwise.
    """
    if state["validation_success"]:
        return "end"
    
    # Check if we have retries left
    max_retries = state.get("max_retries", 15)
    if state["retry_count"] < max_retries:
        return "fix"
    
    return "end"


def should_continue_after_mapping(state: AgentState) -> Literal["convert", "end"]:
    """Determine if we should continue after table mapping.
    
    Args:
        state: Current agent state.
        
    Returns:
        "convert" if mapping was successful, "end" otherwise.
    """
    if state.get("mapping_error"):
        return "end"
    return "convert"


def create_sql_converter_graph() -> StateGraph:
    """Create the LangGraph workflow for SQL conversion.
    
    Returns:
        Compiled StateGraph for the SQL converter agent.
    """
    # Create the graph
    workflow = StateGraph(AgentState)
    
    # Add nodes
    workflow.add_node("validate_hive", validate_hive_node)
    workflow.add_node("table_mapping", table_mapping_node)
    workflow.add_node("convert", convert_node)
    workflow.add_node("validate", validate_node)
    workflow.add_node("fix", fix_node)
    
    # Set entry point
    workflow.set_entry_point("validate_hive")
    
    # Add conditional edge after Hive validation
    workflow.add_conditional_edges(
        "validate_hive",
        should_continue_after_hive_validation,
        {
            "table_mapping": "table_mapping",
            "end": END,
        }
    )
    
    # Add conditional edge after table mapping
    workflow.add_conditional_edges(
        "table_mapping",
        should_continue_after_mapping,
        {
            "convert": "convert",
            "end": END,
        }
    )
    
    # Add edge from convert to validate
    workflow.add_edge("convert", "validate")
    
    # Add conditional edge after validation
    workflow.add_conditional_edges(
        "validate",
        should_retry_after_validation,
        {
            "fix": "fix",
            "end": END,
        }
    )
    
    # Add edge from fix back to validate
    workflow.add_edge("fix", "validate")
    
    # Compile the graph
    return workflow.compile()


def run_conversion(hive_sql: str, max_retries: int | None = None) -> AgentState:
    """Run the SQL conversion workflow.
    
    Args:
        hive_sql: The Hive SQL to convert.
        max_retries: Maximum number of retry attempts for fixing BigQuery SQL.
                     If None, uses MAX_RETRIES environment variable (default: 1).
        
    Returns:
        Final agent state with conversion results.
    """
    if max_retries is None:
        max_retries = get_max_retries()
    
    graph = create_sql_converter_graph()
    
    initial_state: AgentState = {
        "hive_sql": hive_sql,
        "hive_valid": False,
        "hive_error": None,
        "mapping_error": None,
        "bigquery_sql": None,
        "validation_success": False,
        "validation_error": None,
        "validation_mode": None,
        "retry_count": 0,
        "max_retries": max_retries,
        "conversion_history": [],
    }
    
    # Run the graph
    final_state = graph.invoke(initial_state)
    
    return final_state
