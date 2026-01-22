"""Table mapping service for Hive to BigQuery table name conversion."""

import csv
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


@dataclass
class TableMapping:
    """Represents a Hive to BigQuery table mapping."""
    
    hive_table: str
    bigquery_table: str
    note: Optional[str] = None


class TableMappingService:
    """Service for managing Hive to BigQuery table name mappings."""
    
    _instance: Optional["TableMappingService"] = None
    _mappings: dict[str, str] = {}
    _loaded: bool = False
    
    def __new__(cls) -> "TableMappingService":
        """Singleton pattern to ensure mappings are loaded only once."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the service and load mappings if not already loaded."""
        if not self._loaded:
            self.load_mappings()
    
    def load_mappings(self, csv_path: Optional[str] = None) -> None:
        """Load table mappings from CSV file.
        
        Args:
            csv_path: Path to the CSV file. If not provided, uses default path.
        """
        if csv_path is None:
            # Default path: tests/data/hive2bq.csv relative to project root
            csv_path = os.getenv(
                "TABLE_MAPPING_CSV",
                str(Path(__file__).parent.parent.parent / "tests" / "data" / "hive2bq.csv")
            )
        
        if not os.path.exists(csv_path):
            logger.warning(f"Table mapping file not found: {csv_path}")
            return
        
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    hive_table = row.get("Hive 表名 (Original)", "").strip()
                    bq_table = row.get("BigQuery 表名 (Mapped)", "").strip()
                    
                    # Skip empty mappings or "无" entries
                    if hive_table and bq_table and bq_table != "无":
                        # Normalize hive table name to lowercase for case-insensitive matching
                        self._mappings[hive_table.lower()] = bq_table
                        
            logger.info(f"Loaded {len(self._mappings)} table mappings from {csv_path}")
            TableMappingService._loaded = True
            
        except Exception as e:
            logger.error(f"Failed to load table mappings: {e}")
    
    def get_bigquery_table(self, hive_table: str) -> Optional[str]:
        """Get the BigQuery table name for a Hive table.
        
        Args:
            hive_table: The Hive table name (e.g., "dim_hoteldb.dimhotel").
            
        Returns:
            The mapped BigQuery table name, or None if not found.
        """
        # Normalize to lowercase for case-insensitive matching
        normalized = hive_table.lower().strip()
        return self._mappings.get(normalized)
    
    def get_all_mappings(self) -> dict[str, str]:
        """Get all table mappings.
        
        Returns:
            Dictionary of Hive table names to BigQuery table names.
        """
        return self._mappings.copy()
    
    def validate_and_replace(self, sql: str) -> tuple[str, list[str]]:
        """Validate table names and replace them with BigQuery equivalents using sqlglot.
        
        Args:
            sql: The SQL statement with Hive table names.
            
        Returns:
            Tuple containing:
            - The modified SQL (or original if parsing failed)
            - List of unmapped table names found in the SQL
        """
        if not sql:
            return "", []
            
        try:
            # Parse SQL using Hive dialect
            parsed = sqlglot.parse(sql, read="hive")
            if not parsed:
                return sql, []
                
            unmapped_tables = set()
            modified = False
            
            for statement in parsed:
                for table in statement.find_all(exp.Table):
                    # Skip tables in USE statements (these are databases, not tables)
                    if table.find_ancestor(exp.Use):
                        continue
                        
                    # Extract full table name (db.table or just table)
                    full_name = table.name
                    if table.db:
                        full_name = f"{table.db}.{full_name}"
                    
                    # Check if table exists in mapping
                    lookup_name = full_name.lower()
                    
                    if lookup_name in self._mappings:
                        bq_table = self._mappings[lookup_name]
                        # Create new table expression
                        # We use sqlglot.to_table to parse the BQ table name
                        new_table = sqlglot.to_table(bq_table)
                        table.replace(new_table)
                        modified = True
                    else:
                        # Collect unmapped table
                        unmapped_tables.add(full_name)
            
            # If we modified the AST, generate new SQL
            # We use 'hive' dialect to stay close to original structure, 
            # but with BQ table names inserted.
            if modified:
                # Use default dialect but ensure we don't change too much structure
                # Note: sqlglot might reformat the SQL.
                new_sql = ";\n".join(s.sql(dialect="hive") for s in parsed)
                return new_sql, list(unmapped_tables)
            else:
                return sql, list(unmapped_tables)
                
        except Exception as e:
            logger.error(f"Failed to parse SQL with sqlglot: {e}")
            # Fallback to regex replacement if parsing fails
            # But since we need validation, we can't reliably validate with regex
            # So we return original SQL and a warning in unmapped tables if possible
            # or just proceed with regex replacement and empty unmapped list (risky but safer than crashing)
            
            # Let's try regex replacement as fallback
            logger.info("Falling back to regex replacement")
            return self._regex_replace(sql), []

    def _regex_replace(self, sql: str) -> str:
        """Fallback regex-based replacement (original implementation)."""
        result = sql
        sorted_mappings = sorted(
            self._mappings.items(),
            key=lambda x: len(x[0]),
            reverse=True
        )
        
        for hive_table, bq_table in sorted_mappings:
            patterns = [
                (rf'`{re.escape(hive_table)}`', f'`{bq_table}`'),
                (rf'(?i)(?<=\bFROM\s)({re.escape(hive_table)})(?=\s|$|,|\))', bq_table),
                (rf'(?i)(?<=\bJOIN\s)({re.escape(hive_table)})(?=\s|$|,|\))', bq_table),
                (rf'(?i)(?<=\bINTO\s)({re.escape(hive_table)})(?=\s|$|,|\))', bq_table),
                (rf'(?i)(?<=\bUPDATE\s)({re.escape(hive_table)})(?=\s|$|,|\))', bq_table),
                (rf'(?i)(?<=\bTABLE\s)({re.escape(hive_table)})(?=\s|$|,|\))', bq_table),
            ]
            for pattern, replacement in patterns:
                result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        return result
    
    def replace_table_names(self, sql: str) -> str:
        """Replace all Hive table names in SQL with BigQuery table names.
        
        Args:
            sql: The SQL statement with Hive table names.
            
        Returns:
            SQL statement with BigQuery table names.
        """
        new_sql, _ = self.validate_and_replace(sql)
        return new_sql
    
    def get_mapping_info_for_prompt(self) -> str:
        """Generate a formatted string of table mappings for use in prompts.
        
        Returns:
            Formatted string listing all table mappings.
        """
        if not self._mappings:
            return "No table mappings available."
        
        lines = ["## Table Name Mappings (Hive → BigQuery):"]
        for hive_table, bq_table in sorted(self._mappings.items()):
            lines.append(f"- {hive_table} → `{bq_table}`")
        
        return "\n".join(lines)


def get_table_mapping_service() -> TableMappingService:
    """Get the singleton table mapping service instance.
    
    Returns:
        TableMappingService instance.
    """
    return TableMappingService()
