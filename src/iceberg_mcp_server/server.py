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
from fastmcp import FastMCP, Context
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

# ==========================================
# TOOLS: Active Capabilities
# ==========================================
@mcp.tool()
def get_ontology_schema(ctx: Context) -> dict:
    """
    Extracts the high-level structural ontology of the data environment.
    Use this when a user asks about the types of systems, platforms, data assets, 
    or relationships that exist in the ecosystem.
    """
    ctx.info("Extracting ontology schema from Impala...")
    cursor = get_db_cursor()
    
    # Extract Node Ontology
    cursor.execute("""
        SELECT category, system_name, entity_type, COUNT(*) 
        FROM lineage_entities 
        GROUP BY category, system_name, entity_type
    """)
    node_results = cursor.fetchall()
    
    # Extract Edge Ontology
    cursor.execute("""
        SELECT relationship_category, relationship_type, COUNT(*) 
        FROM lineage_relationships 
        GROUP BY relationship_category, relationship_type
    """)
    edge_results = cursor.fetchall()
    
    return {
        "status": "success",
        "nodes": [{"category": r[0], "platform": r[1], "type": r[2], "count": r[3]} for r in node_results],
        "edges": [{"flow": r[0], "relationship": r[1], "count": r[2]} for r in edge_results]
    }

@mcp.tool()
def get_downstream_lineage(entity_id: str, ctx: Context) -> dict:
    """
    Finds all downstream data assets and ETL jobs that rely on a given entity.
    Use this for Impact Analysis (e.g., what breaks if I drop this table?).
    
    Args:
        entity_id: The exact URN of the entity to trace. 
                   For databases, format MUST be: 'db:<system_name>:<database>:<schema>:<object_name>'. 
                   For ETLs, format MUST be: 'etl:<system_name>:<etl_type>:<job_name>'.
    """
    ctx.info(f"Running downstream lineage for: {entity_id}")
    cursor = get_db_cursor()
    
    visited_nodes = set()
    queue = [entity_id]
    edges = []
    
    while queue:
        current_node = queue.pop(0)
        
        if current_node in visited_nodes:
            continue
        visited_nodes.add(current_node)
        
        cursor.execute(f"SELECT target_id, relationship_type FROM lineage_relationships WHERE source_id = '{current_node}'")
        for row in cursor.fetchall():
            target_id, rel_type = row[0], row[1]
            edges.append({
                "source": current_node, 
                "target": target_id, 
                "relationship": rel_type
            })
            
            if target_id not in visited_nodes:
                queue.append(target_id)
                
    return {"status": "success", "edges": edges, "total_impacted_nodes": len(visited_nodes) - 1}

@mcp.tool()
def get_upstream_lineage(entity_id: str, ctx: Context) -> dict:
    """
    Finds all upstream data assets and ETL jobs that feed into a given entity.
    Use this for Root Cause Analysis (e.g., where does this data come from?).
    
    Args:
        entity_id: The exact URN of the entity to trace. 
                   For databases, format MUST be: 'db:<system_name>:<database>:<schema>:<object_name>'. 
                   For ETLs, format MUST be: 'etl:<system_name>:<etl_type>:<job_name>'.
    """
    ctx.info(f"Running upstream lineage for: {entity_id}")
    cursor = get_db_cursor()
    
    visited_nodes = set()
    queue = [entity_id]
    edges = []
    
    while queue:
        current_node = queue.pop(0)
        
        if current_node in visited_nodes:
            continue
        visited_nodes.add(current_node)
        
        cursor.execute(f"SELECT source_id, relationship_type FROM lineage_relationships WHERE target_id = '{current_node}'")
        for row in cursor.fetchall():
            source_id, rel_type = row[0], row[1]
            edges.append({
                "source": source_id, 
                "target": current_node, 
                "relationship": rel_type
            })
            
            if source_id not in visited_nodes:
                queue.append(source_id)
                
    return {"status": "success", "edges": edges, "total_upstream_nodes": len(visited_nodes) - 1}

def main():
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    print(f"Starting Iceberg MCP Server via transport: {transport}")
    mcp.run(transport=transport)

if __name__ == "__main__":
    main()
