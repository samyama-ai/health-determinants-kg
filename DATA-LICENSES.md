# Data licences

`LICENSE` in this repository covers the **loader code**. It says nothing about the
upstream data this repository reads and, where a snapshot is published, redistributes.
That gap is what this file closes (samyama-cloud#97).

Each row records what the source's **own terms page** says, with the URL and the date it
was read. Where a source could not be re-verified it says so rather than guessing: an
unverified licence written down as fact is worse than the silence it replaces.

| Source | What we load | What its terms page says | Checked |
|---|---|---|---|
| [World Bank World Development Indicators](https://datacatalog.worldbank.org/public-licenses) | Country indicators | **CC BY 4.0.** Any use including commercial, with credit to the World Bank and a note of changes made. | 2026-09-18 |
| [WHO Air Quality database](https://www.who.int/about/policies/publishing/copyright) | Air-quality measures | **CC BY-NC-SA 3.0 IGO.** Copy, adapt and redistribute for **non-commercial** purposes, crediting WHO, with adaptations under the same terms. WHO's logo needs written permission and the data may not be used to promote a product or organisation. | 2026-09-18 |
| [FAO AQUASTAT](https://www.fao.org/contact-us/terms/en/) | Water and sanitation indicators | **Not re-verified.** FAO data is generally CC BY-NC-SA 3.0 IGO; confirm on FAO's terms page before redistributing. | not checked |
| [UNDP Human Development Index](http://hdr.undp.org/en/copyright-and-terms-use) | HDI values | **Not re-verified.** UNDP states its own terms of use per dataset; confirm before redistributing. | not checked |

**Non-commercial and share-alike, because of WHO.** The World Bank half is CC BY 4.0 and
would be freely redistributable on its own; joined with WHO air-quality data the graph
becomes CC BY-NC-SA 3.0 IGO — non-commercial, share-alike, credit WHO and the World Bank.

**If a permissive snapshot is wanted**, build it from the World Bank layer alone and name it
so nobody assumes otherwise (samyama-cloud#122). The FAO and UNDP rows must be settled
first either way.

## How to read the "derived graph" line

A graph built from several sources carries **all** of their terms at once. The
restrictive ones win: one non-commercial source makes the join non-commercial, one
share-alike source makes the join share-alike. That is why the derived licence below is
not simply the most permissive source in the table.

## If you redistribute

- Keep the attributions named above with the data.
- State which snapshot version you took, so a reader can check it against the source.
- Re-read the terms pages: licences change, and the dates in this table are when we last
  looked.
