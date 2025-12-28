from __future__ import annotations

import io
import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from .auth import load_credentials

from typing import List, Dict, Any, Optional

def list_pdfs_in_folder(
    folder_id: str,
    credentials_path: str,
    token_path: str,
) -> List[Dict[str, Any]]:
    """
    Lista PDFs dentro de uma pasta do Google Drive (não recursivo).
    Retorna [{id, name, mimeType, modifiedTime, size}, ...]
    """

    creds = load_credentials(credentials_path=credentials_path, token_path=token_path)
    service = build("drive", "v3", credentials=creds)

    q = f"'{folder_id}' in parents and trashed = false and mimeType = 'application/pdf'"

    files: List[Dict[str, Any]] = []
    page_token: Optional[str] = None

    while True:
        resp = service.files().list(
            q=q,
            fields="nextPageToken, files(id, name, mimeType, modifiedTime, size)",
            pageSize=1000,
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()

        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    # ordena por nome (opcional)
    files.sort(key=lambda x: x.get("name", "").lower())
    return files

def download_file(file_id: str, out_path: str, credentials_path: str, token_path: str) -> str:
    if not file_id:
        raise ValueError("file_id vazio. Configure GDRIVE_FILE_ID no .env")

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    creds = load_credentials(credentials_path, token_path)
    service = build("drive", "v3", credentials=creds)

    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(out_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            print(f"Download: {int(status.progress() * 100)}%")

    return out_path
