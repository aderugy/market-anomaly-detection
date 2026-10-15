"""Télécharge les données brutes dans data/raw/ et met à jour data/raw/SOURCES.md.

Usage :
    uv run scripts/download_data.py            # télécharge ce qui manque
    uv run scripts/download_data.py --force    # retélécharge tout

Téléchargement via kagglehub (dépendance : `uv add kagglehub`).
kagglehub télécharge dans son cache (~/.cache/kagglehub), puis le script copie
les fichiers dans data/raw/<source>/. Si Kaggle demande une authentification :
    - fichier ~/.kaggle/kaggle.json, ou
    - variables d'environnement KAGGLE_USERNAME et KAGGLE_KEY.
Ne jamais mettre le token dans le dépôt.
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
SOURCES_MD = RAW / "SOURCES.md"

# Registre des sources : une entrée par jeu de données.
KAGGLE_SOURCES = [
    {
        "name": "beat_the_bookie",
        "slug": "austro/beat-the-bookie-worldwide-football-dataset",
        "url": "https://www.kaggle.com/datasets/austro/beat-the-bookie-worldwide-football-dataset",
    },
]


def sha256(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


def download_kaggle(source: dict, force: bool) -> None:
    dest = RAW / source["name"]
    if dest.exists() and any(dest.iterdir()) and not force:
        print(f"[skip] {source['name']} déjà présent dans {dest}")
        return

    import kagglehub

    print(f"[download] {source['slug']}")
    cache_path = Path(kagglehub.dataset_download(source["slug"], force_download=force))
    print(f"[cache] {cache_path}")

    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(cache_path, dest)

    # Le cache kagglehub est rangé en .../versions/<N> : on garde N pour la provenance.
    source["version"] = cache_path.name if cache_path.name.isdigit() else "inconnue"
    source["downloaded_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[ok] copié dans {dest.relative_to(ROOT)}")


def _read_previous_meta() -> dict[str, dict[str, str]]:
    """Relit SOURCES.md pour conserver date et version quand on saute un téléchargement."""
    meta: dict[str, dict[str, str]] = {}
    if not SOURCES_MD.exists():
        return meta
    current = None
    for line in SOURCES_MD.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            current = line[3:].strip()
            meta[current] = {}
        elif current and line.startswith("- Téléchargé le :"):
            meta[current]["date"] = line.split(":", 1)[1].replace("(UTC)", "").strip()
        elif current and line.startswith("- Version Kaggle :"):
            meta[current]["version"] = line.split(":", 1)[1].strip()
    return meta


def write_sources_md() -> None:
    """Régénère SOURCES.md à partir du contenu réel de data/raw/."""
    previous = _read_previous_meta()
    lines = [
        "# Sources des données brutes",
        "",
        "Fichiers téléchargés par `scripts/download_data.py`. Ne jamais modifier à la main.",
        "",
    ]
    for source in KAGGLE_SOURCES:
        folder = RAW / source["name"]
        if not folder.exists():
            continue
        old = previous.get(source["name"], {})
        date = source.get("downloaded_at") or old.get("date", "inconnue")
        version = source.get("version") or old.get("version", "inconnue")
        lines += [
            f"## {source['name']}",
            "",
            f"- Source : [{source['slug']}]({source['url']}) (Kaggle, via kagglehub)",
            f"- Version Kaggle : {version}",
            f"- Téléchargé le : {date} (UTC)",
            "",
            "| Fichier | Taille (octets) | SHA-256 |",
            "| --- | ---: | --- |",
        ]
        for f in sorted(p for p in folder.rglob("*") if p.is_file()):
            rel = f.relative_to(RAW)
            lines.append(f"| `{rel}` | {f.stat().st_size} | `{sha256(f)}` |")
        lines.append("")
    SOURCES_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"[ok] {SOURCES_MD.relative_to(ROOT)} mis à jour")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--force", action="store_true", help="retélécharger même si présent")
    args = parser.parse_args()

    RAW.mkdir(parents=True, exist_ok=True)
    for source in KAGGLE_SOURCES:
        download_kaggle(source, args.force)
    write_sources_md()


if __name__ == "__main__":
    main()