"""GCP Application Default Credentials helpers for local and Docker runs."""

from __future__ import annotations

import os


def ensure_service_account_file_exists_if_configured() -> None:
    """Fail fast when GOOGLE_APPLICATION_CREDENTIALS points at a missing file.

    Typical mistake: a Windows path is set in `.env`, but the process runs in Linux
    (Docker) where that path does not exist — mount the JSON and use a container path.

    Another mistake: the bind mount source is a directory (e.g. GCP_SA_KEY_HOST points at a folder),
    which makes the container path a directory and triggers IsADirectoryError in google-auth.
    """
    path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not path:
        return
    if os.path.isdir(path):
        raise ValueError(
            f"GOOGLE_APPLICATION_CREDENTIALS={path!r} is a directory, not a JSON file. "
            "Use a real .json file (not a folder). With Docker Compose, save the key as "
            "docker/service-account.json and use GOOGLE_APPLICATION_CREDENTIALS=/run/gcp/service-account.json."
        )
    if os.path.isfile(path):
        return
    raise ValueError(
        f"GOOGLE_APPLICATION_CREDENTIALS is set to {path!r}, but that file does not exist here. "
        "With Docker Compose, save the key as docker/service-account.json (host) — mounted at /run/gcp/ — "
        "and set GOOGLE_APPLICATION_CREDENTIALS=/run/gcp/service-account.json (see docker-compose.yml)."
    )


ADC_DOCKER_HINT = (
    "Set GOOGLE_APPLICATION_CREDENTIALS to a service account JSON file with access to Pub/Sub and BigQuery. "
    "Docker Compose: save the key as docker/service-account.json; the docker/ folder is mounted at /run/gcp "
    "(see docker-compose.yml)."
)
