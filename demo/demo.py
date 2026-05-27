"""Narrated terminal demo: Health-determinants vulnerability on Samyama.

Record with asciinema:
    asciinema rec -c "python -m demo.demo" demo/health-determinants.cast

Loads World Bank WDI + WHO Ambient Air Quality + FAO AQUASTAT into a Samyama
graph and walks through the question public-health teams ask: "why are
populations vulnerable?" — the social, environmental and water/sanitation
drivers of poor health.
"""

from __future__ import annotations

import time

from rich.console import Console
from rich.panel import Panel
from samyama import SamyamaClient

from etl.loader import load_health_determinants

console = Console()
G = "default"


def pause(s: float = 1.4) -> None:
    time.sleep(s)


def step(title: str) -> None:
    console.print()
    console.rule(f"[bold cyan]{title}")
    pause(0.6)


def run(client, q, label):
    console.print(f"  [dim]cypher>[/dim] [yellow]{q}[/yellow]")
    rows = client.query(q, G).records
    one = len(rows) == 1 and len(rows[0]) == 1
    console.print(f"  [green]→[/green] {label}: [bold]{rows[0][0] if one else rows}[/bold]")
    pause()
    return rows


def main() -> None:
    console.print(Panel.fit(
        "[bold]Samyama · Health-Determinants Knowledge Graph[/bold]\n"
        "\"Why are populations vulnerable?\" — air, water & social drivers of health\n"
        "[dim]data: World Bank WDI + WHO Air Quality + FAO AQUASTAT · public indicators[/dim]",
        border_style="cyan",
    ))
    pause(1.2)

    step("1 · Load World Bank + WHO Air Quality + FAO AQUASTAT into Samyama")
    stats = load_health_determinants(client := SamyamaClient.embedded(), "data")
    console.print(f"  [green]loaded[/green] {stats['total_nodes']} nodes, "
                  f"{stats['total_edges']} edges")
    run(client, "MATCH (c:Country) RETURN count(c) AS countries", "countries covered")
    run(client, "MATCH (e:EnvironmentalFactor) RETURN count(e) AS aq",
        "air-quality observations")
    run(client, "MATCH (w:WaterResource) RETURN count(w) AS wsh",
        "water/sanitation observations")

    step("2 · Where is the ambient air-pollution burden heaviest?")
    run(
        client,
        "MATCH (c:Country)-[:ENVIRONMENT_OF]->(e:EnvironmentalFactor) "
        "WHERE e.year = 2019 "
        "RETURN c.name AS country, round(avg(e.value)) AS air_burden "
        "ORDER BY air_burden DESC LIMIT 5",
        "highest WHO ambient air-pollution burden (2019)",
    )

    step("3 · Who has the least safely-managed drinking water?")
    run(
        client,
        "MATCH (c:Country)-[:WATER_RESOURCE_OF]->(w:WaterResource "
        "{indicator_name: \"WSH_WATER_SAFELY_MANAGED\"}) "
        "RETURN c.name AS country, round(min(w.value)) AS pct_safe_water "
        "ORDER BY pct_safe_water ASC LIMIT 5",
        "lowest % population with safely-managed water",
    )

    step("4 · Cross the drivers: dirty air AND poor sanitation")
    console.print("  [dim]join WHO air-pollution burden with FAO sanitation coverage per country…[/dim]")
    pause()
    run(
        client,
        "MATCH (c:Country)-[:ENVIRONMENT_OF]->(e:EnvironmentalFactor) "
        "WHERE e.year = 2019 "
        "WITH c, max(e.value) AS air_burden "
        "MATCH (c)-[:WATER_RESOURCE_OF]->(w:WaterResource "
        "{indicator_name: \"WSH_SANITATION_SAFELY_MANAGED\"}) "
        "WITH c, air_burden, max(w.value) AS sanitation "
        "WHERE air_burden > 35.0 AND sanitation < 50.0 "
        "RETURN c.name AS country, round(air_burden) AS air_burden, "
        "round(sanitation) AS pct_safe_sanitation "
        "ORDER BY air_burden DESC LIMIT 5",
        "double-burdened: dirty air + scarce safe sanitation",
    )

    console.print()
    console.print(Panel.fit(
        "[bold green]One Country.iso_code joins this to health-systems-kg & "
        "surveillance-kg[/bold green] — social/environmental drivers, response\n"
        "capacity, and outbreak signal answered on a single engine, one Cypher query.",
        border_style="green",
    ))
    pause(1.5)


if __name__ == "__main__":
    main()
