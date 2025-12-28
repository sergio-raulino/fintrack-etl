from typing import List, Dict, Optional
import requests
import time

from config import (
    tenant_id,
    client_id,
    client_secret,
    site_hostname,
    site_path,
    lists_config,
)


class SharePointListClient:
    """
    Cliente para ler listas do SharePoint via Microsoft Graph.

    Responsabilidades:
    - Obter token de acesso (client_credentials).
    - Descobrir o site_id a partir de hostname + path.
    - Localizar listas pelo displayName (ex.: 'Orçamento e Finanças').
    - Buscar itens paginando via @odata.nextLink.
    - Buscar metadados de colunas.
    """

    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        site_hostname: str,
        site_path: str,
    ) -> None:
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.site_hostname = site_hostname
        self.site_path = site_path

        self._access_token: Optional[str] = None
        self._site_id: Optional[str] = None

    # ----------------- Autenticação -----------------

    def _get_access_token(self) -> str:
        if self._access_token:
            return self._access_token

        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        resp = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "https://graph.microsoft.com/.default",
            },
        )

        if resp.status_code != 200:
            raise RuntimeError(
                f"❌ Falha ao obter token do Azure AD: {resp.status_code} - {resp.text}"
            )

        self._access_token = resp.json()["access_token"]
        return self._access_token

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Accept": "application/json",
        }

    # ----------------- Site / Listas -----------------

    def _get_site_id(self) -> str:
        if self._site_id:
            return self._site_id

        site_api_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_hostname}:{self.site_path}"
        resp = requests.get(site_api_url, headers=self.headers)

        if resp.status_code != 200:
            raise RuntimeError(
                f"❌ Erro ao obter o site do SharePoint: {resp.status_code} - {resp.text}"
            )

        data = resp.json()
        self._site_id = data["id"]
        print(f"✅ Site localizado: {data.get('displayName')} (id={self._site_id})")
        return self._site_id

    def _resolve_list_display_name(self, list_key_or_display_name: str) -> str:
        """
        Se 'list_key_or_display_name' existir em lists_config, usa o display_name cadastrado.
        Caso contrário, assume que já é o displayName exato da lista.
        """
        cfg = lists_config.get(list_key_or_display_name)
        if cfg and cfg.get("display_name"):
            return cfg["display_name"]
        return list_key_or_display_name

    def _get_list_id(self, list_display_name: str) -> str:
        site_id = self._get_site_id()
        lists_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
        resp = requests.get(lists_url, headers=self.headers)

        if resp.status_code != 200:
            raise RuntimeError(
                f"❌ Erro ao listar listas do site: {resp.status_code} - {resp.text}"
            )

        listas = resp.json().get("value", [])
        alvo = list_display_name.lower()

        for l in listas:
            name = (l.get("name") or "").lower()
            display = (l.get("displayName") or "").lower()

            if name == alvo or display == alvo:
                print(
                    f"✅ Lista localizada: {l.get('displayName')} "
                    f"(name={l.get('name')}, id={l.get('id')})"
                )
                return l["id"]

        nomes = [
            f"{l.get('name')} / displayName={l.get('displayName')}"
            for l in listas
        ]
        raise RuntimeError(
            f"❌ Lista '{list_display_name}' não encontrada no site.\n"
            f"Listas disponíveis:\n- " + "\n- ".join(nomes)
        )

    # ----------------- Itens (com paginação) -----------------

    def fetch_items(
        self,
        list_key_or_display_name: str,
        page_size: int = 1000,
    ) -> List[Dict]:
        """
        Busca todos os itens da lista, já expandindo 'fields' e paginando via @odata.nextLink.

        Retorna uma lista de dicionários correspondentes a 'fields' de cada item.
        """
        display_name = self._resolve_list_display_name(list_key_or_display_name)
        site_id = self._get_site_id()
        lista_id = self._get_list_id(display_name)

        items_url = (
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{lista_id}/items"
            f"?expand=fields&top={page_size}"
        )

        todos_itens: List[Dict] = []
        pagina = 1

        while items_url:
            print(f"🔎 Página {pagina} - GET {items_url}")
            resp = requests.get(items_url, headers=self.headers, timeout=60)

            if resp.status_code != 200:
                raise RuntimeError(
                    f"❌ Erro ao obter itens da lista na página {pagina}: "
                    f"{resp.status_code} - {resp.text}"
                )

            data_json = resp.json()
            page_items = [item["fields"] for item in data_json.get("value", [])]
            todos_itens.extend(page_items)

            print(
                f"✅ Página {pagina} - {len(page_items)} registros coletados "
                f"(acumulado: {len(todos_itens)})"
            )

            # paginação via @odata.nextLink
            items_url = data_json.get("@odata.nextLink")
            if not items_url:
                print("🏁 Coleta completa de todas as páginas.")
                break

            pagina += 1
            # opcional: pequena pausa para não sobrecarregar
            time.sleep(0.2)

        return todos_itens

    # ----------------- Metadados das colunas -----------------

    def fetch_columns(
        self,
        list_key_or_display_name: str,
    ) -> List[Dict]:
        """
        Retorna a lista de colunas da lista (metadata), útil para montar
        o mapa displayName → internalName, etc.
        """
        display_name = self._resolve_list_display_name(list_key_or_display_name)
        site_id = self._get_site_id()
        lista_id = self._get_list_id(display_name)

        columns_url = (
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{lista_id}/columns"
        )
        resp = requests.get(columns_url, headers=self.headers)

        if resp.status_code != 200:
            raise RuntimeError(
                f"❌ Erro ao obter colunas da lista: {resp.status_code} - {resp.text}"
            )

        cols_json = resp.json().get("value", [])
        print(f"🧱 Total de colunas retornadas: {len(cols_json)}")
        return cols_json


# -------------------------------------
# Instância padrão usando o config
# -------------------------------------

default_client = SharePointListClient(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret,
    site_hostname=site_hostname,
    site_path=site_path,
)


# -------------------------------------
# Funções helper (interface tipo api_client)
# -------------------------------------

def fetch_all(list_key_or_display_name: str, page_size: int = 1000) -> List[Dict]:
    """
    Função helper para manter uma interface parecida com o antigo api_client.fetch_all,
    mas agora especializada para listas SharePoint.

    Exemplos:
        fetch_all("orcamento_financas")
        fetch_all("Orçamento e Finanças")
    """
    print(f"📋 Lendo itens da lista SharePoint: {list_key_or_display_name}")
    return default_client.fetch_items(list_key_or_display_name, page_size=page_size)


def fetch_columns(list_key_or_display_name: str) -> List[Dict]:
    """
    Helper para obter metadados das colunas da lista.

    Exemplos:
        fetch_columns("orcamento_financas")
        fetch_columns("Orçamento e Finanças")
    """
    print(f"📋 Lendo metadados de colunas da lista: {list_key_or_display_name}")
    return default_client.fetch_columns(list_key_or_display_name)

