# Getting Started — Health Determinants Knowledge Graph

From `git clone` to your first answer. The **snapshot path** is the fastest (a few minutes).

---

## 1. Prerequisites

- **Python ≥ 3.10** (required by the `samyama` SDK; macOS ships 3.9 — use `python3.10`+).
- **git**
- **Docker** — to run the Samyama engine (needed for the snapshot import and for serving MCP / CLI / API).

## 2. Install

```bash
git clone https://github.com/samyama-ai/health-determinants-kg.git
cd health-determinants-kg
python3 -m venv .venv && source .venv/bin/activate     # Python >= 3.10
pip install -r requirements.txt
```

## 3. Run the engine (Docker)

```bash
docker run --rm -p 8080:8080 -p 6379:6379 public.ecr.aws/f9f6l5u4/samyama-graph:1.1.0
```

## 4. Load the graph — into the `health-determinants` tenant

### Option A — snapshot (recommended, ~seconds)
```bash
curl -LO https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v6/health-determinants.sgsnap
curl -X POST http://localhost:8080/api/tenants -H 'Content-Type: application/json' \
  -d '{"id":"health-determinants","name":"Health Determinants KG"}'
curl -X POST http://localhost:8080/api/tenants/health-determinants/snapshot/import -F "file=@health-determinants.sgsnap"
```

### Option B — build from source (downloads World Bank / WHO AQ / FAO / UNDP)
```bash
python -m etl.download_worldbank --data-dir data
python -m etl.download_who_airquality --data-dir data
python -m etl.download_fao --data-dir data
python -m etl.download_undp --data-dir data
python -m etl.loader --data-dir data --url http://localhost:8080     # all phases → health-determinants tenant
```
*(The loader defaults to the `health-determinants` tenant; override with `--tenant`. Omit `--url` to build
an in-memory graph instead. Load a subset with `--phases worldbank hdi`.)*

## 5. Ask your first question

Fastest is **Claude over MCP** — see **[docs/QUERYING.md](docs/QUERYING.md)**. Quick check over HTTP —
highest health spend per person (2023):

```bash
curl -s -X POST http://localhost:8080/api/query -H 'Content-Type: application/json' -d '{
  "graph": "health-determinants",
  "query": "MATCH (c:Country)-[:HAS_INDICATOR]->(i:SocioeconomicIndicator) WHERE i.indicator_name = '"'"'Current health expenditure per capita (current US$)'"'"' AND i.year = 2023 RETURN c.name AS country, i.value AS usd ORDER BY i.value DESC LIMIT 5"
}'
# → United States (13,473), Switzerland (11,784), Liechtenstein (11,495), Norway (8,296), Monaco (8,004)
```

> **Note:** indicator/country nodes carry vector embeddings, so `keys(n)` / `properties(n)` come back
> empty — but **explicit property access works** (`c.name`, `i.indicator_name`, `i.value`, `i.year`).
> Write queries with named properties rather than whole-node returns.

## 6. The ETL pipeline

- Data sources: **World Bank WDI, WHO Air Quality, FAO AQUASTAT, UNDP HDI**.
- `etl/download_*.py` — one downloader per source.
- `etl/loader.py` — orchestrates the phases (`worldbank`, `airquality`, `aquastat`, `hdi`) into the graph
  (Country, Region, SocioeconomicIndicator, DemographicProfile, EnvironmentalFactor, WaterResource,
  NutritionIndicator). Run `python -m etl.loader --help`.

## Next
- **[docs/QUERYING.md](docs/QUERYING.md)** — MCP (Claude), HTTP API, and the Samyama CLI
