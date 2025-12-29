from __future__ import annotations

from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow

# Caminho baseado na raiz do projeto
ROOT_DIR = Path(__file__).resolve().parent
SECRETS_DIR = ROOT_DIR / "spark" / "secrets"

CREDS_PATH = SECRETS_DIR / "credentials.json"
TOKEN_PATH = SECRETS_DIR / "token.json"

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def main() -> None:
    if not CREDS_PATH.exists():
        raise FileNotFoundError(
            f"❌ credentials.json não encontrado em {CREDS_PATH}\n"
            "Coloque o arquivo em spark/secrets/"
        )

    SECRETS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"🔐 Usando credentials: {CREDS_PATH}")
    print("🌐 Abrindo fluxo OAuth no navegador...")

    flow = InstalledAppFlow.from_client_secrets_file(str(CREDS_PATH), SCOPES)

    # abre o navegador e trata callback automaticamente
    creds = flow.run_local_server(port=0)

    TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")
    print(f"\n✅ Novo token salvo em: {TOKEN_PATH}")


if __name__ == "__main__":
    main()
