"""Manual smoke test against the real REData API.

Not part of the pytest suite (no auth needed, but still a live network call).
Run manually: python scripts/smoke_redata.py
"""

from __future__ import annotations

from datetime import date, timedelta

from seip.ingestion.redata_client import (
    RedataClientError,
    fetch_balance_electrico,
    fetch_evolucion_renovable_no_renovable,
    fetch_generacion_estructura,
    fetch_intercambios,
)


def main() -> None:
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    today = date.today().isoformat()

    print("-- generacion/estructura-generacion --")
    values = fetch_generacion_estructura(yesterday, today)
    titles = sorted({v["title"] for v in values})
    print(f"{len(values)} values, sources: {titles}")

    print("\n-- generacion/evolucion-renovable-no-renovable --")
    values = fetch_evolucion_renovable_no_renovable(yesterday, today)
    print(f"{len(values)} values, titles: {sorted({v['title'] for v in values})}")

    print("\n-- balance/balance-electrico (checks nested `content` parser) --")
    values = fetch_balance_electrico(yesterday, today)
    print(f"{len(values)} values, titles: {sorted({v['title'] for v in values})[:5]}...")

    print("\n-- intercambios/francia-frontera (optional/unstable endpoint) --")
    try:
        values = fetch_intercambios("francia", yesterday, today)
        print(f"{len(values)} values")
    except RedataClientError as exc:
        print(f"Failed as expected sometimes (non-critical endpoint): {exc}")


if __name__ == "__main__":
    main()
