from sqlalchemy import create_engine, inspect
from typing import List, Dict, Any

class MySQLSchemaInspector:
    def __init__(self, connection_string: str):
        """
        Initialize the MySQL inspector with a source database connection string.
        Example: mysql+pymysql://user:password@host:3306/dbname
        """
        self.engine = create_engine(connection_string)

    def get_table_names(self) -> List[str]:
        """Retrieve all table names from the source MySQL database."""
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Reflect a specific table's columns, data types, nullability, 
        and primary key constraints dynamically.
        """
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name)
        pk_constraint = inspector.get_pk_constraint(table_name)
        pk_cols = pk_constraint.get("constrained_columns", [])

        schema_details = []
        for col in columns:
            schema_details.append({
                "name": col["name"],
                "type": str(col["type"]),
                "nullable": col["nullable"],
                "default": str(col.get("default", "")),
                "is_primary_key": col["name"] in pk_cols
            })
        
        return schema_details

    def close(self):
        """Dispose of the connection engine."""
        self.engine.dispose()