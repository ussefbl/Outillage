import sys
from pathlib import Path

# Ajoute .../Outillage/src au sys.path pour que "import distribution_par_webdav" fonctionne
SRC = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SRC))
