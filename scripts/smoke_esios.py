"""Manual smoke test against the real ESIOS API.

Not part of the pytest suite (needs a real credential, never available in CI).
Run manually: python scripts/smoke_esios.py

Requires ESIOS_API_TOKEN either as an environment variable, or in a `.env`
or `token.env` file at the repo root (both gitignored) with a line:
    ESIOS_API_TOKEN=your-token-here
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

from seip.ingestion.esios_client import fetch_indicator_values, get_indicator_metadata


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    _load_dotenv(repo_root / ".env")
    _load_dotenv(repo_root / "token.env")
    token = os.environ.get("ESIOS_API_TOKEN")
    if not token:
        raise SystemExit("Set ESIOS_API_TOKEN in a .env file at the repo root or as an env var.")

    print("-- metadata for indicator 1001 (PVPC) --")
    meta = get_indicator_metadata(1001, api_key=token)
    print("magnitud:", meta.get("magnitud"))
    print("tiempo:", meta.get("tiempo"))

    yesterday = date.today() - timedelta(days=1)
    print(f"\n-- values for indicator 1001, {yesterday} --")
    values = fetch_indicator_values(
        1001,
        start_date=yesterday.isoformat(),
        end_date=date.today().isoformat(),
        geo_id=8741,
        api_key=token,
    )
    print(f"{len(values)} values returned")
    if values:
        print("first value:", values[0])


if __name__ == "__main__":
    main()
