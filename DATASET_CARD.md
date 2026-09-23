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


## Freshness

**Refresh cadence:** The three real upstreams (README.md's "Data sources and
licences" corrects this repo's card: **FAO AQUASTAT is not actually a source** --
`etl/download_fao.py` fetches WHO GHO water/sanitation indicators despite its
name) each publish on their own schedule, and none of it is refreshed
automatically here:
- **World Bank WDI** is revised multiple times a year as countries report new
  figures (the World Bank does not batch WDI into one annual release).
- **UNDP HDI** values are published once a year, as part of UNDP's annual Human
  Development Report.
- **WHO GHO** indicators (air quality; water and sanitation) are each updated on
  their own per-indicator schedule, typically annually, not on one shared
  calendar.

Rebuilding requires manually re-running the `etl/download_*.py` scripts; there is
no scheduled job that does this.

**Data as of:** The loader code (`etl/`) was last modified 2026-07-31 (`git log`).
README.md's source/licence table was corrected 2026-08-17 ("FAO is not a source,
and WHO is 16% not 0.6%"), which is the last point the source list itself was
re-checked against the loader's actual behaviour. `DATASET_CARD.md` and
`DATA-LICENSES.md` record their upstream licence-terms pages as checked
2026-09-18 for World Bank and WHO (FAO/UNDP rows say "not checked" there) --
that is a licence-check date, not a data re-fetch date. No record exists of when
the underlying indicator values were last actually pulled from World Bank,
UNDP or WHO's live APIs; treat 2026-07-31 (last loader change) as the upper
bound on how current a from-source rebuild would be.

## Reproducing

The loader in this repository rebuilds the graph from the upstream source. See the
README's Quick Start for the snapshot download and the from-source build.

## Known limitations

- Counts here are those stated by the repository README at the time this card was
  written; they are not re-measured by the card.
- Where a field above says *not recorded*, that is a gap in this repository rather
  than a property of the data.

## Citation

Please cite this repository if you use it. See [`CITATION.cff`](CITATION.cff) for
machine-readable metadata (CFF 1.2.0).

```bibtex
@misc{health_determinants_kg_2026,
  title        = {Health Determinants Knowledge Graph},
  author       = {Samyama},
  year         = {2026},
  howpublished = {\url{https://git.samyama.ai/Samyama.ai/health-determinants-kg}}
}
```

**No DOI.** This release has not been deposited to Zenodo, so there is no DOI to
cite. Getting one is open work -- it requires a human to make the Zenodo deposit
(KG-06).
