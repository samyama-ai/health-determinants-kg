"""Health Determinants KG — Main orchestrator.

Loads all phases (worldbank, airquality, aquastat, hdi) sequentially,
sharing a single Registry for cross-phase deduplication.

Usage:
    python -m etl.loader --data-dir data
    python -m etl.loader --data-dir data --phases worldbank hdi
    python -m etl.loader --url http://localhost:8080 --data-dir data
"""

from __future__ import annotations

import argparse
import time

from etl.helpers import Registry

ALL_PHASES = ["worldbank", "airquality", "aquastat", "hdi"]


def _run_phase(phase: str, client, data_dir: str, registry: Registry, *, tenant: str = "default") -> dict:
    """Run a single phase and return its stats dict."""
    if phase == "worldbank":
        from etl.worldbank_loader import load_worldbank_data
        return load_worldbank_data(client, data_dir, registry, tenant)
    elif phase == "airquality":
        from etl.airquality_loader import load_air_quality
        return load_air_quality(client, data_dir, registry, tenant)
    elif phase == "aquastat":
        from etl.aquastat_loader import load_aquastat
        return load_aquastat(client, data_dir, registry, tenant)
    elif phase == "hdi":
        from etl.hdi_loader import load_hdi
        return load_hdi(client, data_dir, registry, tenant)
    else:
        raise ValueError(f"Unknown phase: {phase}")


def load_health_determinants(
    client,
    data_dir: str = "data",
    phases: list[str] | None = None,
    tenant: str = "default",
) -> dict:
    """Load all health determinants data into the graph.

    Args:
        client: SamyamaClient instance
        data_dir: root data directory
        phases: list of phases to run (default: all)
        tenant: graph tenant name

    Returns:
        Combined stats dict
    """
    if phases is None:
        phases = ALL_PHASES

    print(f"\n{'='*60}")
    print("Health Determinants Knowledge Graph")
    print(f"Phases: {', '.join(phases)}")
    print(f"{'='*60}\n")

    t0 = time.time()
    registry = Registry()
    all_stats = []

    for phase in phases:
        if phase not in ALL_PHASES:
            print(f"  [WARN] Unknown phase '{phase}', skipping")
            continue
        phase_t0 = time.time()
        stats = _run_phase(phase, client, data_dir, registry, tenant=tenant)
        stats["phase_elapsed_s"] = round(time.time() - phase_t0, 1)
        all_stats.append(stats)
        print()

    elapsed = time.time() - t0

    # Aggregate
    total_nodes = sum(
        v for s in all_stats for k, v in s.items()
        if k.endswith("_nodes") and isinstance(v, int)
    )
    total_edges = sum(
        v for s in all_stats for k, v in s.items()
        if k.endswith("_edges") and isinstance(v, int)
    )

    print(f"{'='*60}")
    print(f"DONE: {total_nodes} nodes, {total_edges} edges in {elapsed:.1f}s")
    print(f"Registry: {len(registry.countries)} countries, {len(registry.regions)} regions")
    print(f"{'='*60}\n")

    return {
        "phases": [s.get("source", "") for s in all_stats],
        "total_nodes": total_nodes,
        "total_edges": total_edges,
        "elapsed_s": round(elapsed, 1),
        "phase_stats": all_stats,
    }


def main():
    parser = argparse.ArgumentParser(description="Load Health Determinants KG")
    parser.add_argument("--data-dir", default="data", help="Data directory")
    parser.add_argument("--phases", nargs="*", default=None,
                        help=f"Phases to run (default: all). Choices: {ALL_PHASES}")
    parser.add_argument("--url", default=None, help="Remote Samyama server URL")
    parser.add_argument("--tenant", default="default", help="Tenant name")
    args = parser.parse_args()

    from samyama import SamyamaClient

    if args.url:
        client = SamyamaClient.connect(args.url)
    else:
        client = SamyamaClient.embedded()

    load_health_determinants(client, args.data_dir, args.phases, args.tenant)


if __name__ == "__main__":
    main()
