"""Étape 02 : producteur du flux simulé.

Publie les CSV mensuels de data/landing/ dans data/stream_in/, un fichier toutes les N secondes,
du plus ancien au plus récent. Chaque copie imite la publication d'un nouveau mois par le HCP.

Deux précautions, car Spark lit les nouveaux fichiers dans l'ordre de leur DATE DE MODIFICATION :
- dates de modification strictement croissantes (os.utime), même si on publie très vite ;
- écriture dans un fichier caché (".nom.tmp", ignoré par Spark) puis renommage atomique :
  Spark ne voit jamais un fichier à moitié copié.

Lancement : python scripts/02_producteur_flux.py [--intervalle 5] [--reset]
  --intervalle 0 : tout publier d'un coup (rejouer l'historique rapidement)
  --reset        : vider data/stream_in avant de commencer
Relancé sans --reset, il reprend là où il s'était arrêté.
"""
import argparse
import os
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import LANDING, STREAM_IN


def publier(source, derniere_date_modif):
    temporaire = STREAM_IN / f".{source.name}.tmp"
    shutil.copyfile(source, temporaire)
    date_modif = max(time.time(), derniere_date_modif + 1)
    os.utime(temporaire, (date_modif, date_modif))
    os.replace(temporaire, STREAM_IN / source.name)  # renommage atomique, garde la date de modification
    return date_modif


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--intervalle", type=float, default=5, help="secondes entre deux publications")
    parser.add_argument("--reset", action="store_true", help="vider stream_in avant de commencer")
    args = parser.parse_args()

    # Le nom ipc_AAAA_MM.csv se trie dans l'ordre chronologique
    fichiers = sorted(LANDING.glob("ipc_*.csv"))
    if not fichiers:
        sys.exit("Aucun CSV dans data/landing : lancer d'abord scripts/01_collecte.py")

    if args.reset:
        shutil.rmtree(STREAM_IN, ignore_errors=True)
    STREAM_IN.mkdir(parents=True, exist_ok=True)

    deja_publies = list(STREAM_IN.glob("ipc_*.csv"))
    noms_publies = {f.name for f in deja_publies}
    a_publier = [f for f in fichiers if f.name not in noms_publies]
    derniere_date_modif = max((f.stat().st_mtime for f in deja_publies), default=0)
    print(f"{len(noms_publies)} mois déjà publiés, {len(a_publier)} à publier, "
          f"un toutes les {args.intervalle:g} s")

    for numero, source in enumerate(a_publier, 1):
        derniere_date_modif = publier(source, derniere_date_modif)
        print(f"[{numero}/{len(a_publier)}] publié {source.name}", flush=True)
        if numero < len(a_publier):
            time.sleep(args.intervalle)
    print("Producteur terminé : tous les mois sont publiés.")
