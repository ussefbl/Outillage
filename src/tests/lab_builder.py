# lab_builder.py
# -*- coding: utf-8 -*-
import argparse
from pathlib import Path
from datetime import datetime
import shutil

def write_text(p: Path, content: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")

def write_bytes(p: Path, content: bytes) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def clean_dir(p: Path) -> None:
    if p.exists():
        shutil.rmtree(p)
    p.mkdir(parents=True, exist_ok=True)

def build_csv(csv_path: Path, rows: list) -> None:
    # CSV séparateur ';' (comme ton script)
    headers = set()
    for r in rows:
        headers |= set(r.keys())
    headers = list(headers)
    # Option: ordering stable
    preferred = ["type", "source", "destination", "prefix01", "prefix02", "extension01", "extension02",
                 "exclude_prefix01", "date_policy"]
    ordered = []
    for h in preferred:
        if h in headers:
            ordered.append(h)
    for h in headers:
        if h not in ordered:
            ordered.append(h)

    lines = [";".join(ordered)]
    for r in rows:
        line = []
        for h in ordered:
            line.append((r.get(h, "") or "").replace(";", ","))
        lines.append(";".join(line))
    write_text(csv_path, "\n".join(lines) + "\n")

def main():
    ap = argparse.ArgumentParser(
        description="Génère une arborescence LAB pour tester distribution_par_webdav.py"
    )
    ap.add_argument("--interfaces_root", required=True,
                    help="Chemin racine interfaces (ex: C:/lab/interfaces)")
    ap.add_argument("--webdav_root", required=True,
                    help="Chemin racine webdav (ex: C:/lab/webdav). Le script créera webdav/tech/<DATE>/pars/..")
    ap.add_argument("--out_dir", required=True,
                    help="Dossier de sortie pour CSV + README (ex: C:/lab/out)")
    ap.add_argument("-d", "--date", default=datetime.now().strftime("%Y%m%d"),
                    help="Date AAAAMMJJ utilisée sous webdav/tech/<DATE> (défaut: aujourd'hui)")
    ap.add_argument("--domain", choices=["CCO", "DSN"], default="CCO",
                    help="Domaine pour WAIT/DONE (défaut: CCO)")
    ap.add_argument("--reset", action="store_true",
                    help="Si présent: supprime et recrée interfaces_root/webdav_root/out_dir")
    args = ap.parse_args()

    interfaces_root = Path(args.interfaces_root).resolve()
    webdav_root = Path(args.webdav_root).resolve()
    out_dir = Path(args.out_dir).resolve()
    dateplan = args.date
    domain = args.domain

    # Répertoires cibles
    # Ton script en prod a WEBDAV_HOME = .../tech/, donc on crée webdav/tech/<date>/...
    webdav_tech = webdav_root / "tech"
    wait_dir = webdav_tech / dateplan / "pars" / domain / "WAIT"
    done_dir = webdav_tech / dateplan / "pars" / domain / "DONE"

    # Sources
    src_flow = interfaces_root / "in" / "flow"
    src_excl = interfaces_root / "in" / "flow_excl"
    src_arch = interfaces_root / "in" / "arch"

    if args.reset:
        clean_dir(interfaces_root)
        clean_dir(webdav_root)
        clean_dir(out_dir)
    else:
        ensure_dir(interfaces_root)
        ensure_dir(webdav_root)
        ensure_dir(out_dir)

    # --- 1) Création des sources ---
    # Cas simple : 2 .par
    write_bytes(src_flow / "A.par", b"A\n")
    write_bytes(src_flow / "B.par", b"B\n")
    # Cas déjà en .txt
    write_bytes(src_flow / "C.par.txt", b"C-TXT\n")

    # Cas exclusion (fnmatch)
    write_bytes(src_excl / "keep.par", b"KEEP\n")
    write_bytes(src_excl / "bad_DSN-999.par", b"BAD\n")

    # Cas archives YYYYMMDD (LATEST_YYYYMMDD)
    (src_arch / "20250101").mkdir(parents=True, exist_ok=True)
    (src_arch / "20251201").mkdir(parents=True, exist_ok=True)
    write_bytes(src_arch / "20250101" / "ARCH.par", b"OLD\n")
    write_bytes(src_arch / "20251201" / "ARCH.par", b"NEW\n")

    # Cas _endtime_ (clé logique)
    write_bytes(src_flow / "AAA_endtime_20251223_010203.par", b"ENDTIME\n")

    # --- 2) Création des destinations WAIT/DONE ---
    ensure_dir(wait_dir)
    ensure_dir(done_dir)

    # DONE contient déjà A.par.txt -> pour tester skip WAIT (doublon)
    write_bytes(done_dir / "A.par.txt", b"A\n")

    # WAIT contient déjà un fichier (pour tester skip copie incrémentale)
    write_bytes(wait_dir / "B.par.txt", b"ALREADY_IN_WAIT\n")

    # --- 3) Génération CSV mapping ---
    # IMPORTANT: dans ton script, destination dans le CSV est relative et join avec (webdav_root/tech)/<date>/
    # On met donc 'pars/<domain>/WAIT' etc.
    mapping_rows = [
        # Copie simple: tous les .par de flow vers WAIT
        {
            "type": "CLEVA",
            "source": "in/flow",
            "destination": f"pars/{domain}/WAIT",
            "prefix01": "*.par",
        },
        # Exclusion
        {
            "type": "CLEVA",
            "source": "in/flow_excl",
            "destination": f"pars/{domain}/WAIT",
            "prefix01": "*.par",
            "exclude_prefix01": "*DSN-*.par",
        },
        # Latest YYYYMMDD
        {
            "type": "CLEVA",
            "source": "in/arch",
            "destination": f"pars/{domain}/WAIT",
            "prefix01": "*.par",
            "date_policy": "LATEST_YYYYMMDD",
        },
        # Exemple DONE direct
        {
            "type": "CLEVA",
            "source": "in/flow",
            "destination": f"pars/{domain}/DONE",
            "prefix01": "C.par.txt",   # déjà .txt -> pas de .txt ajouté
        },
    ]

    csv_path = out_dir / "distribution_webdav_lab.csv"
    build_csv(csv_path, mapping_rows)

    # --- 4) README usage ---
    readme = f"""LAB généré ✅

Chemins:
- interfaces_root : {interfaces_root}
- webdav_root     : {webdav_root}
- webdav_tech     : {webdav_tech}
- dateplan        : {dateplan}
- WAIT            : {wait_dir}
- DONE            : {done_dir}
- CSV mapping     : {csv_path}

Contenu créé:
- Sources:
  - {src_flow} : A.par, B.par, C.par.txt, AAA_endtime_20251223_010203.par
  - {src_excl} : keep.par, bad_DSN-999.par (à exclure via *DSN-*.par)
  - {src_arch} : 20250101/ARCH.par (OLD), 20251201/ARCH.par (NEW) -> LATEST_YYYYMMDD doit prendre NEW

- Destinations:
  - DONE contient déjà A.par.txt (pour tester la policy WAIT vs DONE)
  - WAIT contient déjà B.par.txt (pour tester le SKIP incrémental)

Commande de test (adapte le chemin vers ton script):
python distribution_par_webdav.py -d {dateplan} --ref_mapping "{csv_path}" --interfaces_path "{interfaces_root}/" --webdav_path "{webdav_tech}/"

Astuce:
- Si tu veux tester DSN au lieu de CCO, relance lab_builder.py avec --domain DSN
"""
    write_text(out_dir / "README_LAB.txt", readme)

    print("✅ LAB créé")
    print(f"CSV: {csv_path}")
    print(f"README: {out_dir / 'README_LAB.txt'}")
    print(f"WAIT: {wait_dir}")
    print(f"DONE: {done_dir}")


if __name__ == "__main__":
    main()
