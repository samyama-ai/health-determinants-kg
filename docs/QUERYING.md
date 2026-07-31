# Querying the Health Determinants KG

Three ways to ask the graph questions, once it's loaded into the `health-determinants` tenant on a running
engine (see [GETTING_STARTED.md](../GETTING_STARTED.md)). All examples below were run live and return real
results.

> **Heads-up:** Country / indicator nodes carry vector embeddings, so `keys(n)` / `properties(n)` come back
> empty. **Explicit property access works** (`c.name`, `i.indicator_name`, `i.value`, `i.year`) — write
> queries with named properties, not whole-node returns.

---

## 1. Claude, over MCP (natural language)

```bash
# register this KG's MCP server with Claude Code (once), pointed at the running engine:
claude mcp add health-determinants -- python -m mcp_server.server --url http://localhost:8080 --tenant health-determinants

# start a new Claude Code session (MCP servers load at session start), then just ask:
#   "which countries spend the most on health per person?"   → United States, Switzerland, ...
#   "how many indicators does each country have in 2023?"
```

*(No engine? `python -m mcp_server.server --data-dir data` loads a graph in-memory and serves it.)*

## 2. HTTP API (`POST /api/query`)

```bash
curl -s -X POST http://localhost:8080/api/query -H 'Content-Type: application/json' -d '{
  "graph": "health-determinants",
  "query": "MATCH (c:Country)-[:HAS_INDICATOR]->(i:SocioeconomicIndicator) WHERE i.indicator_name = \"Current health expenditure per capita (current US$)\" AND i.year = 2023 RETURN c.name AS country, i.value AS usd ORDER BY i.value DESC LIMIT 5"
}'
```
```json
{"columns":["country","usd"],
 "records":[["United States",13473.19],["Switzerland",11783.67],["Liechtenstein",11494.96],
            ["Norway",8296.36],["Monaco",8004.17]]}
```

## 3. Samyama CLI (Redis wire protocol, `:6379`)

```bash
redis-cli -p 6379 GRAPH.QUERY health-determinants \
  "MATCH (c:Country)-[:HAS_INDICATOR]->(i:SocioeconomicIndicator) WHERE i.year = 2023 RETURN c.name, count(i) AS indicators ORDER BY indicators DESC LIMIT 3"
# 1) "Armenia"   13
# 2) "Guatemala" 13
# 3) "Bolivia"   13
```

---

## More queries
See the MCP tools (`--list-tools`) — `hdi_ranking`, `poverty_health_correlation`,
`environmental_risk_hotspots`, `water_stress_analysis`, `country_vulnerability_profile` — for
higher-level, ready-made questions.
