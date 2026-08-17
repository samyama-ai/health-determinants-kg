# Health Determinants Knowledge Graph

Health determinants knowledge graph — World Bank WDI, UNDP HDI and WHO GHO on Samyama.

> Part of the **Samyama** ecosystem — loaded into and queried via the graph engine at [samyama-ai/samyama-graph](https://github.com/samyama-ai/samyama-graph).
> This repo holds the loader and source-data specifics for the KG; `etl/` has the ingest scripts, `schema/` the node/edge shapes, `mcp_server/` the MCP exposure.

<a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-blue" alt="License"></a>

> ⚠️ **The Apache-2.0 badge covers the code in this repository, not the data.** This graph
> combines three upstream sources under different licences, and **16% of the graph is
> non-commercial**. See [Data sources and licences](#data-sources-and-licences) before
> redistributing anything built from it.

![Health-determinants vulnerability demo](demo/health-determinants.gif)

## Demo

A narrated walkthrough (load World Bank WDI + WHO air quality + WHO water/sanitation →
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

**Data sources** — World Bank WDI, UNDP HDI, and WHO GHO (air quality, plus water and sanitation via `etl/download_fao.py`, which despite its name does not use FAO). `Country.iso_code` bridges to surveillance-kg and health-systems-kg. See [Data sources and licences](#data-sources-and-licences) — the WHO portion is non-commercial.

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
ETL, the schema and the MCP server. It says nothing about the data, which comes from three
independent sources under different licences. **The data's licence is the one that governs
redistribution**, and here they do not agree.

| Source | Feeds | Licence | Commercial use / redistribution |
|--------|-------|---------|--------------------------------|
| [World Bank WDI](https://datacatalog.worldbank.org/public-licenses) | `DemographicProfile`, `NutritionIndicator`, `SocioeconomicIndicator`, and the country-level `EnvironmentalFactor` / `WaterResource` rows — **239,802 nodes** | **CC-BY-4.0** | ✅ Permitted, with attribution |
| [UNDP Human Development Index](https://hdr.undp.org/copyright-and-terms-use) | `SocioeconomicIndicator` (HDI values) | **CC BY 3.0 IGO** | ✅ Permitted — "even commercially" |
| [WHO GHO — air quality](https://www.who.int/about/policies/publishing/data-policy) | `EnvironmentalFactor` where `indicator_code = 'AIR_QUALITY'` — **1,820 nodes** | **Non-commercial** | ❌ **Not permitted for commercial use** |
| [WHO GHO — water & sanitation](https://www.who.int/about/policies/publishing/data-policy) | `WaterResource` where `indicator_code` is `basic_water`, `basic_sanitation`, `safely_managed_water` or `safely_managed_sanitation` — **44,013 nodes** | **Non-commercial** | ❌ **Not permitted for commercial use** |

> **FAO AQUASTAT is named in this README but is not actually a source.**
> `etl/download_fao.py` says so in its own docstring: *"The FAO AQUASTAT portal requires
> manual download, so we use equivalent WHO GHO indicators."* It fetches from
> `https://ghoapi.azureedge.net/api` — WHO's Global Health Observatory. The `WaterResource`
> nodes it produces are therefore **WHO data under a FAO-shaped filename**, and they inherit
> WHO's non-commercial terms. The module name is misleading and should be renamed.

### The WHO restriction

WHO's data policy makes data available on terms allowing

> "non-commercial, not-for-profit use of the Data for public health purposes"

so **a commercial organisation may not redistribute the WHO-derived portion of this graph.**
WHO grants exceptions on request; without one, treat those rows as non-redistributable.

The WHO portion is **45,833 nodes of 285,635 — 16.0%**, in two places:

| Where | `indicator_code` | Nodes |
|-------|------------------|------:|
| `WaterResource` | `basic_water`, `basic_sanitation`, `safely_managed_water`, `safely_managed_sanitation` | 44,013 |
| `EnvironmentalFactor` | `AIR_QUALITY` | 1,820 |

Both are cleanly identifiable, so they can be excluded:

```cypher
MATCH (w:WaterResource)
WHERE w.indicator_code IN ['basic_water', 'basic_sanitation',
                           'safely_managed_water', 'safely_managed_sanitation']
DETACH DELETE w;

MATCH (e:EnvironmentalFactor {indicator_code: 'AIR_QUALITY'})
DETACH DELETE e;
```

That leaves **239,802 nodes** under World Bank CC-BY-4.0 and UNDP CC BY 3.0 IGO, both of
which permit commercial redistribution with attribution.

**Node labels do not map onto sources**, which is what makes this easy to get wrong:

- `etl/worldbank_loader.py` loads across five categories, so `EnvironmentalFactor` and
  `WaterResource` each hold **both** World Bank and WHO rows.
- `etl/download_fao.py` is named for FAO but fetches WHO GHO (see above).

The `indicator_code` is the discriminator: World Bank rows carry World Bank codes
(`ER.H2O.FWTL.ZS`, `SH.H2O.BASW.ZS`, `AG.LND.FRST.ZS`), WHO rows carry the friendly names
listed in the table above.

### Attribution

CC-BY-4.0 and CC BY 3.0 IGO both require attribution, and it travels to anything you
redistribute or build on:

> Contains data from the **World Bank World Development Indicators**
> ([CC-BY-4.0](https://creativecommons.org/licenses/by/4.0/)) and the
> **UNDP Human Development Report**
> ([CC BY 3.0 IGO](https://creativecommons.org/licenses/by/3.0/igo/)).

## License

**Code:** Apache 2.0 — see [`LICENSE`](LICENSE).

**Data:** mixed, and not all of it is redistributable. See
[Data sources and licences](#data-sources-and-licences) above. In particular, the
WHO Air Quality portion is restricted to non-commercial use.
