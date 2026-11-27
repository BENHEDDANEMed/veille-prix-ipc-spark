"""Étape 01 : collecte.

1. Télécharge le fichier Excel du HCP depuis data.gov.ma (s'il n'est pas déjà dans data/raw).
2. Vérifie sa structure (titre, en-têtes, mois, valeurs numériques).
3. Passe du format large (une colonne par mois) au format long (une ligne par observation).
4. Écrit 1 CSV par mois dans data/landing/ : ipc_AAAA_MM.csv (ville, produit, mois, indice).
   Chaque fichier joue le rôle d'une publication mensuelle du HCP pour le flux simulé.
5. Relit les CSV et contrôle les chiffres clés (nombre de lignes, inflation nationale).

Lancement : python scripts/01_collecte.py [--force]   (--force = re-télécharger le fichier)
"""
import sys
import urllib.request
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import FICHE_URL, FICHIER_XLSX, LANDING, MOIS_FR, SOURCE_URL


def telecharger(force=False):
    if FICHIER_XLSX.exists() and not force:
        print(f"Fichier déjà présent : {FICHIER_XLSX}")
        return
    FICHIER_XLSX.parent.mkdir(parents=True, exist_ok=True)
    print(f"Téléchargement depuis {SOURCE_URL}")
    requete = urllib.request.Request(SOURCE_URL, headers={"User-Agent": "ipc-bigdata/1.0"})
    with urllib.request.urlopen(requete, timeout=60) as reponse:
        contenu = reponse.read()
    # Un xlsx est une archive zip : s'il ne commence pas par "PK", le site a renvoyé une page d'erreur
    if not contenu.startswith(b"PK"):
        raise RuntimeError("Le fichier reçu n'est pas un Excel (page d'erreur du site ?)")
    FICHIER_XLSX.write_bytes(contenu)
    print(f"{len(contenu):,} octets enregistrés dans {FICHIER_XLSX}")


def mois_vers_annee_mois(libelle):
    """'Juill-2026' -> (2026, 7). Erreur claire si le HCP change ses libellés."""
    nom, annee = libelle.split("-")
    if nom not in MOIS_FR:
        raise ValueError(f"Libellé de mois inconnu : {libelle!r}")
    return int(annee), MOIS_FR[nom]


def lire_format_long():
    # Ligne 0 = titre, ligne 1 = en-têtes : on lit avec header=1
    large = pd.read_excel(FICHIER_XLSX, header=1)
    colonne_ville, colonne_produit, *colonnes_mois = large.columns
    for libelle in colonnes_mois:
        mois_vers_annee_mois(libelle)  # contrôle : tous les libellés sont des mois connus

    long = large.melt(
        id_vars=[colonne_ville, colonne_produit], var_name="mois", value_name="indice"
    ).rename(columns={colonne_ville: "ville", colonne_produit: "produit"})
    long["indice"] = pd.to_numeric(long["indice"], errors="raise").astype(float)
    return long, colonnes_mois


def ecrire_csv_mensuels(long):
    LANDING.mkdir(parents=True, exist_ok=True)
    for libelle, publication in long.groupby("mois", sort=False):
        annee, mois = mois_vers_annee_mois(libelle)
        publication.to_csv(LANDING / f"ipc_{annee}_{mois:02d}.csv", index=False, encoding="utf-8")


def controler():
    """Relit les CSV écrits (pas la mémoire) pour vérifier ce que le flux va réellement recevoir."""
    fichiers = sorted(LANDING.glob("ipc_*.csv"))
    tout = pd.concat((pd.read_csv(f) for f in fichiers), ignore_index=True)
    print(f"\nFichiers CSV       : {len(fichiers)}  ({fichiers[0].name} -> {fichiers[-1].name})")
    print(f"Observations       : {len(tout):,}")
    print(f"Valeurs manquantes : {int(tout.isna().sum().sum())}")
    print(f"Zones / produits   : {tout['ville'].nunique()} / {tout['produit'].nunique()}")
    print(f"Séries             : {tout.groupby(['ville', 'produit']).ngroups:,}")
    print(f"Lignes par fichier : {sorted({len(pd.read_csv(f)) for f in fichiers})}")

    # Inflation nationale sur 12 mois = indice(m) / indice(m - 12) - 1, sur l'indice général
    general = tout[(tout["ville"] == "National") & (tout["produit"] == "(GR) GENERAL")].copy()
    general[["annee", "num_mois"]] = general["mois"].map(mois_vers_annee_mois).tolist()
    indice = general.set_index(["annee", "num_mois"])["indice"]
    attendus = {(2018, 1): 2.01, (2023, 2): 10.08, (2026, 8): -0.33}
    print("\nInflation nationale sur 12 mois (indice général) :")
    for (annee, mois), attendu in attendus.items():
        calcule = round((indice[annee, mois] / indice[annee - 1, mois] - 1) * 100, 2)
        etat = "OK" if calcule == attendu else "ÉCART"
        print(f"  {mois:02d}/{annee} : {calcule:6.2f} %   attendu {attendu:6.2f} %   {etat}")
        assert calcule == attendu, f"Inflation {mois:02d}/{annee} : {calcule} au lieu de {attendu}"

    assert len(fichiers) == 116 and len(tout) == 318_304 and tout.isna().sum().sum() == 0


if __name__ == "__main__":
    print(f"Source : {FICHE_URL}")
    telecharger(force="--force" in sys.argv)
    long, colonnes_mois = lire_format_long()
    print(f"Format large lu : {long.groupby(['ville', 'produit']).ngroups:,} séries x "
          f"{len(colonnes_mois)} mois ({colonnes_mois[-1]} -> {colonnes_mois[0]})")
    ecrire_csv_mensuels(long)
    controler()
    print("\nCollecte terminée.")
