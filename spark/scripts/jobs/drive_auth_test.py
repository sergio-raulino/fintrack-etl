#!/usr/bin/env python3
from __future__ import annotations

from fintrack_etl.config import settings
from fintrack_etl.logging_conf import setup_logging
from fintrack_etl.integrations.gdrive.auth import load_credentials


def main() -> None:
    setup_logging()

    # Tudo vem do settings (.env.spark carregado pelo dotenv dentro do config.py)
    creds = load_credentials(settings.gdrive_credentials, settings.gdrive_token)

    print("OAuth OK. Token salvo em:", settings.gdrive_token)
    print("Scopes:", getattr(creds, "scopes", None))


if __name__ == "__main__":
    main()

