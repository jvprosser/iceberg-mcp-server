## Copyright (c) 2025 Cloudera, Inc. All Rights Reserved.
##
## This file is licensed under the Apache License Version 2.0 (the "License").
## You may not use this file except in compliance with the License.
## You may obtain a copy of the License at http:##www.apache.org/licenses/LICENSE-2.0.
##
## This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
## OF ANY KIND, either express or implied. Refer to the License for the specific
## permissions and limitations governing your use of the file.

import os
from fastmcp import FastMCP
from dotenv import load_dotenv

load_dotenv()

from iceberg_mcp_server.tools import impala_tools

mcp = FastMCP(name="Cloudera Iceberg MCP Server via Impala")


# Register functions as MCP tools
@mcp.tool()
def execute_query(query: str) -> str:
    """
    Execute a SQL query on the Impala database and return results as JSON.
    """
    return impala_tools.execute_query(query)


@mcp.tool()
def get_schema() -> str:
    """
    Retrieve the list of table names in the current Impala database.
    """
    return impala_tools.get_schema()

@mcp.tool()
def get_upstream_lineage(entity_id: str) -> dict:    
    """                                                                                                                                                                    
    Retrieve the 
    """
    return impala_tools.get_upstream_lineage(entity_id)


@mcp.tool()
def get_downstream_lineage(entity_id: str) -> dict:    
"""
    Extracts the structural ontology of the data environment. 
    Use this when a user asks about the types of systems, platforms, data assets, 
    or relationships that exist in the ecosystem.
    """
    return impala_tools.get_downstream_lineage()

@mcp.tool()
def get_ontology_schema() -> dict:    """                                                                                                                                                                    
    Retrieves the dict of downstream data assets and ETL jobs that rely on the given entity_id.
    Use this for impact analysis.
    """
    return impala_tools.get_downstream_lineage(entity_id)

def main():
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    print(f"Starting Iceberg MCP Server via transport: {transport}")
    mcp.run(transport=transport)

if __name__ == "__main__":
    main()
