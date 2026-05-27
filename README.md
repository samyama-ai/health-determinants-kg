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

This README is a stub; see `docs/` for the full data-source references and ingest pipeline.
