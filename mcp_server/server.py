"""Health Determinants KG — MCP Server.

Exposes the health determinants knowledge graph as MCP tools
for AI agents and LLM tool use.

Usage:
    python -m mcp_server.server
    python -m mcp_server.server --url http://localhost:8080
    python -m mcp_server.server --data-dir data --phases worldbank hdi
"""

from __future__ import annotations

import argparse
import os


def main(argv=None):
    parser = argparse.ArgumentParser(description="Health Determinants KG MCP Server")
    parser.add_argument("--url", help="Remote Samyama server URL")
    parser.add_argument("--data-dir", default="data", help="Data directory")
    parser.add_argument("--phases", nargs="*", default=None, help="Phases to load")
    parser.add_argument("--tenant", default="default", help="Tenant name")
    args = parser.parse_args(argv)

    from samyama import SamyamaClient

    if args.url:
        client = SamyamaClient.connect(args.url)
    else:
        client = SamyamaClient.embedded()
        from etl.loader import load_health_determinants
        load_health_determinants(client, args.data_dir, args.phases, args.tenant)

    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")

    try:
        from samyama_mcp import SamyamaMCPServer, ToolConfig
        config = ToolConfig.from_yaml(config_path)
        server = SamyamaMCPServer(client, config=config, tenant=args.tenant)
        server.run()
    except ImportError:
        print("samyama-mcp-serve not available. Install with: pip install samyama[mcp]")
        print("Falling back to interactive REPL...")
        while True:
            try:
                cypher = input("cypher> ").strip()
                if not cypher or cypher.lower() in ("exit", "quit"):
                    break
                result = client.query(cypher, args.tenant)
                for row in result.records:
                    print(dict(zip(result.columns, row)))
            except (EOFError, KeyboardInterrupt):
                break


if __name__ == "__main__":
    main()
