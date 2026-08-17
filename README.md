# Health Determinants Knowledge Graph

Health determinants knowledge graph — World Bank WDI, WHO Air Quality, FAO AQUASTAT, UNDP HDI on Samyama.

> Part of the **Samyama** ecosystem — loaded into and queried via the graph engine at [samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph).
> This repo holds the loader and source-data specifics for the KG; `etl/` has the ingest scripts, `schema/` the node/edge shapes, `mcp_server/` the MCP exposure.

<a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-blue" alt="License"></a>

> ⚠️ **The Apache-2.0 badge covers the code in this repository, not the data.** This graph
> combines four upstream sources under four different licences, one of which is
> **non-commercial**. See [Data sources and licences](#data-sources-and-licences) before
> redistributing anything built from it.

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

## Data sources and licences

The Apache-2.0 licence in [`LICENSE`](LICENSE) covers the **code** in this repository — the
ETL, the schema and the MCP server. It says nothing about the data, which comes from four
independent sources under four different licences. **The data's licence is the one that
governs redistribution**, and here they do not all agree.

| Source | Feeds | Licence | Commercial use / redistribution |
|--------|-------|---------|--------------------------------|
| [World Bank WDI](https://datacatalog.worldbank.org/public-licenses) | `SocioeconomicIndicator`, `DemographicProfile`, `NutritionIndicator`, and most `EnvironmentalFactor` / `WaterResource` rows | **CC-BY-4.0** | ✅ Permitted, with attribution |
| [UNDP Human Development Index](https://hdr.undp.org/copyright-and-terms-use) | `SocioeconomicIndicator` (HDI values) | **CC BY 3.0 IGO** | ✅ Permitted — "even commercially" |
| [FAO AQUASTAT](https://www.fao.org/aquastat/en/) | `WaterResource` | ⚠️ **Not confirmed** | Read FAO's terms before redistributing |
| [WHO Air Quality](https://www.who.int/about/policies/publishing/data-policy) | `EnvironmentalFactor` rows with a `city` (`indicator_code = AIR_QUALITY`) | **Non-commercial** | ❌ **Not permitted for commercial use** |

### The WHO restriction

WHO's data policy makes data available on terms allowing

> "non-commercial, not-for-profit use of the Data for public health purposes"

so **a commercial organisation may not redistribute the WHO-derived portion of this graph.**
WHO grants exceptions on request; without one, treat those rows as non-redistributable.

The WHO portion is small and cleanly identifiable — **1,820 nodes**, every one carrying
`indicator_code = 'AIR_QUALITY'` and a `city`. They are the only city-level rows in the
graph; every other source is country-level. To exclude them:

```cypher
MATCH (e:EnvironmentalFactor {indicator_code: 'AIR_QUALITY'})
DETACH DELETE e
```

Note the node labels do **not** map one-to-one onto sources — `etl/worldbank_loader.py` also
writes environmental, nutrition, water and socioeconomic nodes, so `EnvironmentalFactor`
holds both WHO city-level air quality and World Bank country-level indicators. The `city`
property is what separates them.

### Attribution

CC-BY-4.0 and CC BY 3.0 IGO both require attribution, and it travels to anything you
redistribute or build on:

> Contains data from the **World Bank World Development Indicators**
> ([CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/)), the
> **UNDP Human Development Report**
> ([CC BY 3.0 IGO](https://creativecommons.org/licenses/by/3.0/igo/)), and
> **FAO AQUASTAT**.

## License

**Code:** Apache 2.0 — see [`LICENSE`](LICENSE).

**Data:** mixed, and not all of it is redistributable. See
[Data sources and licences](#data-sources-and-licences) above. In particular, the
WHO Air Quality portion is restricted to non-commercial use.
