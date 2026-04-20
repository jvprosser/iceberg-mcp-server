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
#from minimcp import MiniMCP, Context
from dotenv import load_dotenv

load_dotenv()

from iceberg_mcp_server.tools import impala_tools

mcp = FastMCP(name="Cloudera Lineage MCP Server via Impala")

# ==========================================
# 2. RESOURCES: Static Knowledge
# ==========================================
@mcp.resource("lineage://schema/views")
def get_view_definitions() -> str:
    """
    Provides the exact SQL DDL definitions of the Impala views.
    The agent can read this resource to understand the exact column names.
    """
    return """
    VIEW lineage_entities: 
      - entity_id (string, URN format)
      - entity_name (string)
      - entity_type (string, e.g., TABLE, VIEW, JOB)
      - system_name (string, e.g., SNOWFLAKE)
      - category (string, e.g., DATABASE, ETL_PROCESS)
      
    VIEW lineage_relationships: 
      - source_id (string, URN format)
      - target_id (string, URN format)
      - relationship_type (string)
      - relationship_category (string)
    """


# ==========================================
# 3. PROMPTS: Pre-built Agentic Workflows
# ==========================================
@mcp.prompt("analyze_impact")
def prompt_impact_analysis(entity_id: str) -> str:
    """A template to instruct the agent on how to perform Impact Analysis."""
    return f"""
    You are the Enterprise Ontology Agent. The data engineering team wants to modify or drop the following entity: '{entity_id}'.
    
    Your instructions:
    1. Use the `get_downstream_lineage` tool on '{entity_id}'.
    2. Identify all critical ETL jobs, staging tables, and BI layers that will break.
    3. Summarize the blast radius clearly for a non-technical business stakeholder.
    """

@mcp.prompt("trace_root_cause")
def prompt_root_cause(entity_id: str) -> str:
    """A template to instruct the agent on how to perform Root Cause Analysis."""
    return f"""
    You are investigating missing or anomalous data appearing in the following entity: '{entity_id}'.
    
    Your instructions:
    1. Use the `get_upstream_lineage` tool to trace the graph backward.
    2. Identify the original source database tables and the intermediate dbt/ETL jobs involved.
    3. Formulate a hypothesis on where the data pipeline might have failed.
    """

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

@mcp.tool(
    name="get_upstream_lineage",
    description="Finds all upstream data assets and ETL jobs that feed into the given entity_id. Use this for Root Cause Analysis."
)
def get_upstream_lineage(entity_id: str) -> dict:    
    """                                                                                                                                                                    
    Retrieve the 
    """
    return impala_tools.get_upstream_lineage(entity_id)


@mcp.tool(
    name="get_downstream_lineage",
    description="Finds all downstream data assets and ETL jobs that rely on the given entity_id. Use this for Impact Analysis."
)
def get_downstream_lineage(entity_id: str) -> dict:    
    """
    Extracts the structural ontology of the data environment. 
    Use this when a user asks about the types of systems, platforms, data assets, 
    or relationships that exist in the ecosystem.
    """
    return impala_tools.get_downstream_lineage()

@mcp.tool(
    name="get_ontology_schema",
    description="Extracts the high-level structural ontology (types of systems, assets, and connections) of the data environment."
)
def get_ontology_schema() -> dict:
    """                                                                                                                                                                    
    Retrieves the dict of downstream data assets and ETL jobs that rely on the given entity_id.
    Use this for impact analysis.
    """
    return impala_tools.get_ontology_schema()

def main():
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    print(f"Starting Iceberg MCP Server via transport: {transport}")
    mcp.run(transport=transport)

if __name__ == "__main__":
    main()
