# Health Determinants Knowledge Graph

Health determinants knowledge graph — World Bank WDI, WHO Air Quality, FAO AQUASTAT, UNDP HDI on Samyama.

> Part of the **Samyama** ecosystem — loaded into and queried via the graph engine at [samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph).
> This repo holds the loader and source-data specifics for the KG; `etl/` has the ingest scripts, `schema/` the node/edge shapes, `mcp_server/` the MCP exposure.

![Health-determinants vulnerability demo](demo/health-determinants.gif)

## Demo

A narrated walkthrough (load World Bank WDI + WHO Air Quality + FAO AQUASTAT →
heaviest air-pollution burden → least safely-managed drinking water → cross the
drivers for double-burdened countries):

```bash
python -m demo.demo                                                     # run live
asciinema rec -c "python -m demo.demo" demo/health-determinants.cast    # re-record
```

## Documentation

New here? Start with the guides:

| Guide | What it covers |
|-------|----------------|
| **[GETTING_STARTED.md](GETTING_STARTED.md)** | prerequisites (Python ≥ 3.10) · install · run the engine (Docker) · load the graph · first query |
| **[docs/QUERYING.md](docs/QUERYING.md)** | ask questions via **MCP (Claude)**, the **HTTP API**, or the **Samyama CLI** |

## Schema

**7 node labels** — Country, Region, SocioeconomicIndicator, DemographicProfile, EnvironmentalFactor, WaterResource, NutritionIndicator

**6 edge types** — HAS_INDICATOR, DEMOGRAPHIC_OF, ENVIRONMENT_OF, WATER_RESOURCE_OF, NUTRITION_STATUS, IN_REGION

**Data sources** — World Bank WDI, WHO Air Quality, FAO AQUASTAT, UNDP HDI. `Country.iso_code` bridges to surveillance-kg and health-systems-kg.

## Quick Start

**Full walkthrough → [GETTING_STARTED.md](GETTING_STARTED.md).** Needs **Python ≥ 3.10** and **Docker**:

```bash
pip install -r requirements.txt
docker run --rm -p 8080:8080 -p 6379:6379 public.ecr.aws/f9f6l5u4/samyama-graph:1.1.0

curl -LO https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v6/health-determinants.sgsnap
curl -X POST http://localhost:8080/api/tenants -H 'Content-Type: application/json' -d '{"id":"health-determinants","name":"Health Determinants KG"}'
curl -X POST http://localhost:8080/api/tenants/health-determinants/snapshot/import -F "file=@health-determinants.sgsnap"
```

Prefer to build from source? See [GETTING_STARTED.md](GETTING_STARTED.md) §4B. To query it (Claude / HTTP / CLI), see [docs/QUERYING.md](docs/QUERYING.md).

## Use with Claude (MCP)

```bash
python -m mcp_server.server --url http://localhost:8080 --tenant health-determinants   # against a running engine
python -m mcp_server.server --data-dir data                                            # embedded, loads on startup
```

Register it with Claude and ask in natural language — full steps in **[docs/QUERYING.md](docs/QUERYING.md)**.
