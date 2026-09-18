---
license: other
pretty_name: Health Determinants Knowledge Graph
tags:
  - knowledge-graph
  - samyama
  - property-graph
  - health
language:
  - en
---

# Dataset Card for `health-determinants-kg`

**Health determinants knowledge graph — World Bank WDI, WHO Air Quality, FAO AQUASTAT, UNDP HDI on Samyama.**

> Part of the **Samyama** ecosystem. This card describes the dataset; the repository
> holds the loader and source-data specifics.

## Structure

_Not recorded in this repository's README._ Node labels, edge types and per-label
counts should be added here — a dataset card without them cannot be used to decide
whether the data fits a question.

## Provenance and licence

Apache 2.0 covers the loader. World Bank WDI is CC BY 4.0, but the WHO air-quality data is
**CC BY-NC-SA 3.0 IGO**, so the joined graph is non-commercial and share-alike. FAO and
UNDP terms are unverified. See [`DATA-LICENSES.md`](DATA-LICENSES.md).


## Reproducing

The loader in this repository rebuilds the graph from the upstream source. See the
README's Quick Start for the snapshot download and the from-source build.

## Known limitations

- Counts here are those stated by the repository README at the time this card was
  written; they are not re-measured by the card.
- Where a field above says *not recorded*, that is a gap in this repository rather
  than a property of the data.
