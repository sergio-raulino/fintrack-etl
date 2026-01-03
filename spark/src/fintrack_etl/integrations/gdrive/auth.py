# fintrack_etl/integrations/gdrive/auth.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# Escopo único, completo, para Drive
SCOPES = ["https://www.googleapis.com/auth/drive"]

def load_credentials(credentials_path: str, token_path: str) -> Credentials:
    """
    - Se token.json existir: carrega direto (escopos lidos do próprio arquivo)
    - Se não existir: roda fluxo interativo (InstalledAppFlow) e salva o token
      (usando SCOPES).
    """
    token_file = Path(token_path)

    if token_file.exists():
        # 👉 Aqui NÃO passo SCOPES, deixo o token.json dizer quais são
        return Credentials.from_authorized_user_file(str(token_file))

    creds_file = Path(credentials_path)
    if not creds_file.exists():
        raise FileNotFoundError(
            f"credentials.json não encontrado em {creds_file}"
        )

    flow = InstalledAppFlow.from_client_secrets_file(str(creds_file), SCOPES)
    creds = flow.run_local_server(port=0)

    token_file.parent.mkdir(parents=True, exist_ok=True)
    token_file.write_text(creds.to_json(), encoding="utf-8")

    return creds

