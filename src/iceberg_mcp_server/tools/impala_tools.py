## Copyright (c) 2025 Cloudera, Inc. All Rights Reserved.
##
## This file is licensed under the Apache License Version 2.0 (the "License").
## You may not use this file except in compliance with the License.
## You may obtain a copy of the License at http:##www.apache.org/licenses/LICENSE-2.0.
##
## This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
## OF ANY KIND, either express or implied. Refer to the License for the specific
## permissions and limitations governing your use of the file.

import json
import os
from impala.dbapi import connect


# Helper to get Impala connection details from env vars
def get_db_connection():
    host = os.getenv("IMPALA_HOST", "coordinator-default-impala.example.com")
    port = int(os.getenv("IMPALA_PORT", "443"))
    user = os.getenv("IMPALA_USER", "username")
    password = os.getenv("IMPALA_PASSWORD", "password")
    database = os.getenv("IMPALA_DATABASE", "default")
    auth_mechanism = os.getenv("IMPALA_AUTH_MECHANISM", "LDAP")
    use_http_transport = os.getenv("IMPALA_USE_HTTP_TRANSPORT", "true")
    http_path = os.getenv("IMPALA_HTTP_PATH", "cliservice")
    use_ssl = os.getenv("IMPALA_USE_SSL", "true")

    return connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        auth_mechanism=auth_mechanism,
        use_http_transport=use_http_transport,
        http_path=http_path,
        use_ssl=use_ssl,
    )


def execute_query(query: str) -> str:
    conn = None

    # Implement rudimentary SQL injection prevention
    # In this case, we only allow read-only queries
    # This is a very basic check and should be improved for production use
    readonly_prefixes = ["select", "show", "describe", "with"]

    if not query.strip().lower().split()[0] in readonly_prefixes:
        return "Only read-only queries are allowed."

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query)
        if cur.description:
            rows = cur.fetchall()
            result = json.dumps(rows, default=str)
        else:
            conn.commit()
            result = "Query executed successfully."
            cur.close()
        return result
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        if conn:
            conn.close()
            

def get_schema() -> str:
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SHOW TABLES")
        tables = cur.fetchall()
        schema = [table[0] for table in tables]
        return json.dumps(schema)
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        if conn:
            conn.close()
            
def get_ontology_schema() -> dict:
    """
    Extracts the structural ontology of the data environment. 
    Use this when a user asks about the types of systems, platforms, data assets, 
    or relationships that exist in the ecosystem.
    """
    try:
        conn = get_db_connection()
    
        cursor = conn.cursor()
        
        # 1. Extract Node Ontology (The "What")
        node_query = """
            SELECT category, system_name, entity_type, COUNT(*) as entity_count
            FROM lineage_entities 
            GROUP BY category, system_name, entity_type
            ORDER BY category, system_name
        """
        cursor.execute(node_query)
        node_results = cursor.fetchall()

        ontology_nodes = []
        for row in node_results:
            ontology_nodes.append({
                "high_level_category": row[0], # e.g., DATABASE or ETL_PROCESS
                "system_platform": row[1],     # e.g., SNOWFLAKE or DBt files
                "asset_type": row[2],          # e.g., TABLE, VIEW, PROCEDURE, JOB
                "total_count": row[3]          # Gives a sense of scale
            })

        # 2. Extract Edge Ontology (The "Rules")
        edge_query = """
            SELECT relationship_category, relationship_type, COUNT(*) as edge_count
            FROM lineage_relationships
            GROUP BY relationship_category, relationship_type
        """
        cursor.execute(edge_query)
        edge_results = cursor.fetchall()

        ontology_edges = []
        for row in edge_results:
            ontology_edges.append({
                "connection_flow": row[0],     # e.g., db_to_etl
                "relationship_type": row[1],   # e.g., DataFlow
                "total_count": row[2]
            })

        return {
            "status": "success",
            "description": "This is the meta-model of the current data ecosystem.",
            "defined_entities": ontology_nodes,
            "defined_relationships": ontology_edges
        }
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        if conn:
            conn.close()        
            
def get_upstream_lineage(entity_id: str) -> dict:
    """
    Finds all upstream data assets and ETL jobs that feed into the given entity_id.
    Use this for root cause analysis to see where data originates.
    """
    conn = None

    try:
        conn = get_db_connection()
    
        cursor = conn.cursor()
    
        visited_nodes = set()
        queue = [entity_id]
        lineage_graph = []
        
        while queue:
            current_node = queue.pop(0)
            
            # Prevent infinite loops if there are circular dependencies
            if current_node in visited_nodes:
                continue
            visited_nodes.add(current_node)
            
            # Query Impala for the immediate preceding steps
            query = f"""
                SELECT source_id, relationship_type, relationship_category 
                FROM lineage_relationships 
                WHERE target_id = '{current_node}'
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            for row in results:
                source_id = row[0]
                rel_type = row[1]
                rel_category = row[2]
                
                # Record the edge. 
                # Notice we map 'source' to source_id and 'target' to current_node
                # so the LLM still sees the true direction of the data flow.
                lineage_graph.append({
                    "source": source_id,
                    "target": current_node,
                    "relationship": rel_type,
                    "category": rel_category
                })
                
                # Add the newly discovered source to the queue to find its parents
                if source_id not in visited_nodes:
                    queue.append(source_id)
                    
        # Return the graph, subtracting 1 from the total nodes to exclude the starting node itself
        return {
            "status": "success", 
            "edges": lineage_graph, 
            "total_upstream_nodes_found": len(visited_nodes) - 1 
        }
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        if conn:
            conn.close()
    
            
def get_downstream_lineage(entity_id: str) -> dict:
    """
    Finds all downstream data assets and ETL jobs that rely on the given entity_id.
    Use this for impact analysis.
    """
    conn = None
    try:
        conn = get_db_connection()
    
        cursor = conn.cursor()
    
        visited_nodes = set()
        queue = [entity_id]
        lineage_graph = []
        
        while queue:
            current_node = queue.pop(0)
            
            # Prevent infinite loops if there are circular dependencies
            if current_node in visited_nodes:
                continue
            visited_nodes.add(current_node)
            
            # Query Impala for the immediate next steps
            query = f"""
                SELECT target_id, relationship_type, relationship_category 
                FROM lineage_relationships 
                WHERE source_id = '{current_node}'
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            for row in results:
                target_id = row[0]
                rel_type = row[1]
                rel_category = row[2]
                
                # Record the edge
                lineage_graph.append({
                    "source": current_node,
                    "target": target_id,
                    "relationship": rel_type,
                    "category": rel_category
                })
                
                # Add the newly discovered target to the queue to find its children
                if target_id not in visited_nodes:
                    queue.append(target_id)
                
                return {"status": "success", "edges": lineage_graph, "total_nodes_impacted": len(visited_nodes)}
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        if conn:
            conn.close()
