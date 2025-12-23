# -*- coding: utf-8 -*-
"""
================================================================================
Script: distribustion_par_webdav.py
Objet
-----
Copier des fichiers PARS issus des repertoires d'interface (CLEVA / DSN) vers WebDAV,
selon un fichier de mapping CSV (prefixes/extensions/exclusions).

Fonctionnalites majeures
------------------------
- Lecture d'un CSV de mapping et generation d'un plan de copie
- Matching par combinaisons prefix/extension et motifs d'exclusion
- Politique date: 'LATEST_YYYYMMDD' pour descendre dans le dernier sous-dossier horodaté
- Purge optionnelle des fichiers du dossier destination (purge=YES)
- Ajout automatique de l'extension .txt si le fichier source n'est pas deja en .txt
- Controle doublon apres copie dans DONE/WAIT
- Renommage optionnel en cas de doublon (feature flag, desactive par defaut)
- Arret au premier echec critique (mkdir, purge, copie)

Contexte d'execution
--------------------
- OS cible : Red Hat 8
- Python   : 3.8
- Droits requis : lecture sur les sources serveur, ecriture sur la racine WebDAV

Parametres
--------------
Obligatoires:
  -d AAAAMMJJ            Date plan utilisee pour le chemin destination
  --ref_mapping <path>   CSV de mapping des fichiers a copier

Optionnels:
  --mode_copie           MODECLEVADSN pour traiter CLEVADSN (sinon CLEVA/DSN par defaut)
  --webdav_path          Racine WebDAV (par defaut WEBDAV_HOME)
  --interfaces_path      Racine des flux sources (sinon CLEVA/DSN par defaut)
  --logshell_path        Dossier explicite des logs du script
  -v                     Niveau de log: debug | info | warn | error

CSV de mapping
--------------
Colonnes obligatoires:
  type, source, destination

Colonnes dynamiques optionnelles:
  prefix##         ex. prefix01, prefix02, ...
  extension##      ex. extension01, extension02, ...
  exclude_prefix## motifs d'exclusion (fnmatch) ex. *_DSN-*.par

Autres colonnes optionnelles:
  purge            YES pour purger le dossier destination avant copie
  date_policy      LATEST_YYYYMMDD pour descendre dans le sous-dossier date le plus recent

Codes retour (un seul sys.exit() dans main)
-------------------------------------------
0 RC_OK                  Execution OK
1 RC_BAD_ARGS            Arguments invalides
2 RC_CONFIG_NOT_FOUND    Fichier --ref_mapping introuvable
3 RC_CONFIG_INVALID      CSV illisible ou colonnes obligatoires manquantes
4 RC_NOTHING_TO_DO       Rien a copier
5 RC_RUNTIME_ERROR       Erreur d'execution (mkdir/purge/copie...)
# =============================================================================
"""

import os
import shutil
import sys
import pprint
import re

import pandas as pd
import argparse
import glob
import logging
from datetime import datetime
import getpass
from fnmatch import fnmatch

# =============================================================================
# === IDENTITE ET HORODATAGE D'EXECUTION ========================
# =============================================================================
VERSION = '1.0.0'
RUNTIME_DATE = '%s' % datetime.now().strftime('%Y.%m.%d')
RUNTIME_TIME = '%s' % datetime.now().strftime('%H:%M:%S')
PROGRAM_NAME = os.path.basename(__file__)
ThisProgramVersion = '%s version %s - Current Runtime: %s - %s' \
                     % (PROGRAM_NAME, VERSION, RUNTIME_DATE, RUNTIME_TIME)

# =============================================================================
# === JOURNALISATION : NOMS ET CHEMINS DE LOG =================================
# =============================================================================
THIS_PROGRAM = os.path.splitext(os.path.basename(__file__))[0]
SELF_LOG_DATETIME = datetime.now().strftime('%Y-%m-%d--%H-%M-%S')
# CLEVA
CLEVA_BATCH_PREFIX_LOG_FILENAME = "rapport-clevacol-batch"
CLEVA_BATCH_TECHNIC_LOG_PATH = "/data/share/interfaces/log/"
# DSN
DSN_BATCH_PREFIX_LOG_FILENAME = "rapport-dsncol-batch"
DSN_BATCH_TECHNIC_LOG_PATH = f"/data/share/dsncol/appdsn/batch/log/"
logger = None  # handle du logger initialise par startLogger()

# =============================================================================
# === OPTIONS / FEATURE FLAGS ==================================================
# =============================================================================
ENABLE_RENAME = False  # Renommage en cas de doublon (desactive par defaut)

# =============================================================================
# === MESSAGES FIXES ===========================================================
# =============================================================================
PREFIX_MSG = "Copie Pars sur webdav :"

# =============================================================================
# === ENVIRONNEMENT : CHEMINS PAR DEFAUT (SURCHARGEABLES) =============
# =============================================================================
CURRENT_DATETIME_YYYYMMDD = datetime.now().strftime('%Y%m%d')
MODE_COPY_PAR = "CLEVA"  # 'CLEVA' ou 'DSN'
CLEVA_DATA_HOME = "/data/share/interfaces/"
DSN_DATA_HOME = "/data/share/dsncol/"
WEBDAV_HOME = "/data/share/batchs/tech/"

# =============================================================================
# === REFERENTIEL : NOMS DES COLONNES ATTENDUES =======================================
# =============================================================================
PAR_FILE_TYPE = "type"  # type de copie : CLEVA, DSN, ModeCLEVADSN
PAR_SOURCE_PATH = "source"  # chemin source local serveur
PAR_DESTINATION_PATH = "destination"  # chemin destination sous WebDAV
PAR_PREFIX_BASE = "prefix"  # colonnes dynamiques: prefix01, prefix02, ...
PAR_EXCLUDE_PREFIX = "exclude_prefix"  # colonnes dynamiques: exclude_prefix01, ...
PAR_EXTENSION_BASE = "extension"  # colonnes dynamiques: extension01, extension02, ...
PAR_PURGE = "purge"  # yes/no pour purge du dossier destination
DATE_POLICY = "date_policy"  # identification sous-dossier d'archivage

# =============================================================================
# === PARAMETRES RUNTIME RENSEIGNES ================================
# =============================================================================
param_log_verbose = "INFO"
init_log_msg = ""  # buffer des traces emises avant l'initialisation du logger
this_program_log_path = ""  # chemin retenu pour les logs du script
param_date_traitement = ""  # valeur de dateplan -d AAAAMMJJ
param_mode_copie = ""
param_webdav_path = ""
param_interface_path = ""
param_logshell_path = ""
param_ref_mapping_path = ""

# =============================================================================
# === REPERTOIRE WEBDAV WAIT/DONE PAR DOMAINE ================================
# =============================================================================
DOMAIN_WAIT_DONE_PATHS = {
    "CCO": {
        "WAIT": "pars/CCO/WAIT",
        "DONE": "pars/CCO/DONE",
    },
    "DSN": {
        "WAIT": "pars/DSN/WAIT",
        "DONE": "pars/DSN/DONE",
    },
}
# =============================================================================
# === CODES DE RETOUR ========================================
# =============================================================================
RC_OK = 0
RC_BAD_ARGS = 1
RC_CONFIG_NOT_FOUND = 2
RC_CONFIG_INVALID = 3
RC_NOTHING_TO_DO = 4
RC_RUNTIME_ERROR = 5
# =============================================================================
# === FONCTIONS UTILITAIRES PRE-LOGGER (NE PAS MODIFIER) ======================
# =============================================================================
def log_before_logger(msg: str) -> None:
    global init_log_msg
    print(msg)
    init_log_msg += '\n' + msg
# =============================================================================
def add_path_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else path + "/"
# =============================================================================
def logger_path_generation() -> None:
    global this_program_log_path, param_logshell_path

    log_before_logger(f"Init: Identification du chemin de log du self python")
    param_logshell_path = add_path_trailing_slash(param_logshell_path)

    # Check des répertoires standards
    log_before_logger(f"Init: Check existence [{REFERENCE_BATCH_TECHNIC_LOG_PATH}]")
    standard_unix_logpath_found = False
    standard_unix_logpath = ''

    if os.path.exists(REFERENCE_BATCH_TECHNIC_LOG_PATH):
        standard_unix_logpath_found = True
        standard_unix_logpath = add_path_trailing_slash(REFERENCE_BATCH_TECHNIC_LOG_PATH)
        log_before_logger(f"Init: Check existence [{standard_unix_logpath}]: OK")
    else:
        log_before_logger(f"Init: Check existence [{REFERENCE_BATCH_TECHNIC_LOG_PATH}]: KO")

    # Vérification du chemin de log python s'il est indiqué
    ## Chemin de log en paramètre de ligne de commande
    if len(param_logshell_path) > 1 and os.path.exists(param_logshell_path):
        this_program_log_path = param_logshell_path
    ## Chemin stantard des serveurs unix
    elif standard_unix_logpath_found:
        this_program_log_path = standard_unix_logpath
    else:
        log_before_logger('Init: Logshell_path usage du standard [./]')
        this_program_log_path = './'  ### current working directory
# =============================================================================
def startLogger():
    global logger

    formatter = logging.Formatter('[%(levelname)s] %(asctime)s : %(message)s')
    logger_filename = f"{REFERENCE_BATCH_PREFIX_LOG_FILENAME}-{THIS_PROGRAM}-{SELF_LOG_DATETIME}.log"
    logger_fullpath = os.path.join(this_program_log_path, logger_filename)

    log_before_logger(f"Init: Chemin du log : [{logger_fullpath}]")
    fileHandler = logging.FileHandler(logger_fullpath, mode='a')
    fileHandler.setFormatter(formatter)
    fileHandler.setLevel(getattr(logging, param_log_verbose))

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)

    log = logging.getLogger(THIS_PROGRAM)
    log.setLevel(getattr(logging, param_log_verbose))
    log.addHandler(fileHandler)
    log.addHandler(consoleHandler)

    logger = log
    logger.info(init_log_msg)
# =============================================================================
# === FONCTIONS SPECIFIQUE SCRIPT ============================
# =============================================================================
def set_reference_batch_vars(mode_copy_par) -> None:
    global REFERENCE_BATCH_PREFIX_LOG_FILENAME, REFERENCE_BATCH_TECHNIC_LOG_PATH
    if mode_copy_par == "CLEVA":
        REFERENCE_BATCH_PREFIX_LOG_FILENAME = CLEVA_BATCH_PREFIX_LOG_FILENAME
        REFERENCE_BATCH_TECHNIC_LOG_PATH = CLEVA_BATCH_TECHNIC_LOG_PATH
    else:
        REFERENCE_BATCH_PREFIX_LOG_FILENAME = DSN_BATCH_PREFIX_LOG_FILENAME
        REFERENCE_BATCH_TECHNIC_LOG_PATH = DSN_BATCH_TECHNIC_LOG_PATH
# =============================================================================
def parseArgs():
    """
    Analyse les arguments et renseigne les parametres globaux.
    Valide la date -d au format AAAAMMJJ. Les messages d'init sont emis via
    log_before_logger (avant l'initialisation du logger).
    """
    global \
        param_date_traitement, param_mode_copie, param_ref_mapping_path, \
        param_webdav_path, param_interface_path, param_logshell_path, param_log_verbose

    parser = argparse.ArgumentParser(
        prog=THIS_PROGRAM,
        epilog='''(*) Param_DatePlan obligatoire''',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=
        '''
        >> Permet la consultation par la MOA sur Webdav des fichiers .Par issus de la quotidienne.
        >> Le transfert est automatique à 16h00;19h00;04h00;15h45

        ****************************************************************************************
        ***  Test Unitaire : Usage d'une arborescence locale simulant Webdav et /interfaces/
        ****************************************************************************************
        *) Indiquer un repertoire equivalent a webdav_root : 
            --webdav_path ./myCopyOfWebdavDir/             contenant ./tech/yyyymmdd/pars/*
            --webdav_path D:\Data\DevNovaTools\LogWebdav\  chemin relatif ou absolu

        *) Indiquer un repertoire contenant directement les flux a copier :
            --interfaces_path ./myInterfacesDirectory      contenant ./in/* (les flux)

        *) Indiquer un repertoire destinataire du log de ce programme python :
            --logshell_path ./outLogDirectory

        ****************************************************************************************
        *** Usage Production : -d DatePlan --ref_mapping ./distibution_webdav.csv
        *** Chemin standard : This_Script_LOG_PATH = '/data/package/clevacol/*/log/shell/'
        *** Chemin standard : Flux a copier        = '/data/share/interfaces/'
        ****************************************************************************************
        ''')
    ## aide en dev
    # --interfaces_path D:\Data\DevNovaTools\share_interfaces --webdav_path D:\Data\DevNovaTools\webdav_copie
    # --ref_mapping C:\Users\GS1060\PycharmProjects\CLEVACOL-BATCHS-TECHNIQUES\src\main\batch_custom\distribution_par_webdav\resources\distribution_webdav.csv

    # # Parametres obligatoires ##
    parser.add_argument('-d', type=str, metavar='dateTraitement', required=True,
                        help="Date de planification au format AAAAMMJJ")

    parser.add_argument('--ref_mapping', type=str, metavar='refMappingPath', required=True,
                        help='(*)Chemin du referentiel des fichiers a traiter')

    # # Parametres optionnels ##
    parser.add_argument("--mode_copie", type=str, metavar='modeCopie',
                        nargs='?', const='', default='',
                        help="Optionnel : CLEVADSN pour full copie CLEVA & DSN"
                             " (sinon CLEVA ou DSN par defaut selon le domaine)")

    parser.add_argument('--webdav_path', type=str,
                        help='Racine Webdav contenant ./tech/')

    parser.add_argument('--interfaces_path', type=str,
                        help='Chemin direct contenant ./in/')

    parser.add_argument('--logshell_path', type=str,
                        help='Chemin explicite du log python')

    parser.add_argument('-v', type=str, metavar='Log_Level', nargs='?', const='info',
                        choices=['debug', 'info', 'warn', 'error', 'critical'], default='info',
                        help='Definition du niveau de logging,\n debug | info | warning | error | critical')

    input_args = parser.parse_args()
    log_before_logger('Init: %s' % str(input_args))
    log_before_logger('Init: Chemin d\'execution [%s]' % os.getcwd())
    log_before_logger('Init: Contexte utilisateur [%s]' % getpass.getuser())

    if input_args.d:
        param_date_traitement = str(input_args.d).strip()
        try:
            datetime.strptime(param_date_traitement, '%Y%m%d')
        except ValueError:
            log_before_logger('Init: Date Plan invalide [%s]' % param_date_traitement)

        log_before_logger('Init: Date Plan [%s]' % param_date_traitement)

    if input_args.ref_mapping:
        param_ref_mapping_path = str(input_args.ref_mapping).strip()
        log_before_logger('Init: Fichier CSV [%s]' % param_ref_mapping_path)

    if input_args.mode_copie:
        param_mode_copie = str(input_args.mode_copie).strip().upper()
        if param_mode_copie == "CLEVADSN":
            log_before_logger('Init: Mode copie FULL active [%s]' % param_mode_copie)
    else:
        log_before_logger('Init: Mode copie [%s] active' % MODE_COPY_PAR)

    if input_args.webdav_path:
        if input_args.webdav_path and not input_args.webdav_path.replace('\\', '/').endswith('/'):
            input_args.webdav_path = input_args.webdav_path + '/'
        param_webdav_path = input_args.webdav_path.replace('\\', '/')
        log_before_logger('Init: Mode [%s] active [%s]' % ('Webdav_root_path', param_webdav_path))

    if input_args.interfaces_path:
        if input_args.interfaces_path and not input_args.interfaces_path.replace('\\', '/').endswith('/'):
            input_args.interfaces_path = input_args.interfaces_path + '/'
        param_interface_path = input_args.interfaces_path.replace('\\', '/')
        log_before_logger('Init: Mode [%s] active [%s]' % ('Batchlog_path', param_interface_path))

    if input_args.logshell_path:
        if input_args.logshell_path and not input_args.logshell_path.replace('\\', '/').endswith('/'):
            input_args.logshell_path = input_args.logshell_path + '/'
        param_logshell_path = input_args.logshell_path.replace('\\', '/')
        log_before_logger('Init: Mode [%s] active [%s]' % ('Logshell_path', param_logshell_path))

    if input_args.v:
        param_log_verbose = input_args.v.upper()
    return input_args
# =============================================================================
def resolve_duplicate_name(destination_dir: str,
                           dest_filename: str,
                           purged_destinations: set,
                           relance_tag: str = "-Doublon",
                           allow_sequence: bool = True) -> str:
    """
    Fonction desactivée par defaut (ENABLE_RENAME=False)
    Retourne un nom de fichier sur pour la destination si un doublon existe.
    - Si 'destination_dir' a deja ete purge pendant ce run -> on garde le nom original.
    - Sinon, si un fichier du meme nom existe -> ajoute un suffixe (ex: '-Doublon')
      avant l'extension, puis numerote si necessaire.

    Parametres:
        destination_dir (str): dossier de destination
        dest_filename (str):   nom de fichier cible (avec extension)
        purged_destinations (set): destinations deja purgees dans ce run
        relance_tag (str):     suffixe ajoute en cas de doublon
        allow_sequence (bool): si True, numerote -Doublon-1, -Doublon-2, ...

    Retour:
        str: nom a utiliser (possiblement renomme)
    """
    # Si la destination a ete purgee pendant ce run, pas de risque d'ecrasement
    if destination_dir in purged_destinations:
        return dest_filename

    candidate = dest_filename
    candidate_path = os.path.join(destination_dir, candidate)

    if os.path.exists(candidate_path):
        # Inserer avant la premiere extension
        first_ext = dest_filename.find(".")
        if first_ext == 1:
            base, ext = dest_filename, ""
        else:
            base, ext = dest_filename[:first_ext], dest_filename[first_ext:]

        candidate = f"{base}{relance_tag}{ext}"
        candidate_path = os.path.join(destination_dir, candidate)

        # S'il existe encore, numeroter
        if allow_sequence:
            i = 1
            while os.path.exists(candidate_path):
                candidate = f"{base}{relance_tag}-{i}{ext}"
                candidate_path = os.path.join(destination_dir, candidate)
                i += 1

            logger.warning("Fichier existant, renommage [%s]", candidate)

    return candidate
# =============================================================================
def extract_header_columns(df: pd.DataFrame, base_name_header: str) -> list:
    """
    Retourne la liste des colonnes dont le nom commence par 'base_name_header'
    (comparaison insensible a la casse). Loggue les colonnes detectees
    pour le debug.
    """
    matching_headers = []
    logger.debug('Colonne dynamique trouvee:')
    for header in df.columns:
        if header.lower().startswith(base_name_header.lower()):
            matching_headers.append(header)

    for matching_header in matching_headers:
        logger.debug('   - [%s]' % matching_header)

    return matching_headers
# =============================================================================
def find_latest_yyyymmdd_subdir(source_arch_par_base: str) -> str or None:
    """
    Parcourt les sous-dossiers directs de 'source_arch_par_base' et retourne
    le chemin du sous-dossier au format YYYYMMDD le plus recent. Retourne None
    si aucun ne correspond.

    Parametres:
        source_arch_par_base (str): chemin du dossier parent

    Retour:
        str | None: chemin du dernier sous-dossier YYYYMMDD, ou None
    """
    try:
        archive_par_paths = os.listdir(source_arch_par_base)
    except Exception as error:
        logger.warning('Erreur listage sur source_root [%s] (%s)' % (source_arch_par_base, str(error)))
        return None

    latest_arch_par_dir_name = None
    for arch_par_dir_name in archive_par_paths:
        arch_par_path = os.path.join(source_arch_par_base, arch_par_dir_name)
        if not os.path.isdir(arch_par_path):
            continue
        if len(arch_par_dir_name) != 8:
            continue
        if not arch_par_dir_name.isdigit():
            continue
        try:
            datetime.strptime(arch_par_dir_name, '%Y%m%d')
        except ValueError:
            continue
        if latest_arch_par_dir_name is None or arch_par_dir_name > latest_arch_par_dir_name:
            latest_arch_par_dir_name = arch_par_dir_name

    if latest_arch_par_dir_name is None:
        logger.debug('Aucun sous-dossier date trouve sous [%s]' % source_arch_par_base)
        return None

    latest_full_path = os.path.join(source_arch_par_base, latest_arch_par_dir_name)
    logger.info('Sous-dossier le plus recent selectionne [%s]' % latest_full_path)
    return latest_full_path
# =============================================================================
def compute_logical_key(filename: str) -> str:
    """
    Construit la cle logique utilisee pour detecter des doublons WAIT/DONE.
    - si '_endtime_' present : prefixe + '.par.txt'
    - sinon : nom complet
    """
    logical_key = filename
    idx = logical_key.find('_endtime_')
    if idx != -1:
        prefix = logical_key[:idx]
        logical_key = prefix + '.par.txt'
    return logical_key
# =============================================================================
def files_are_different_streaming(wait_path: str, done_path: str, chunk_size: int = 1024 * 1024) -> bool:
    try:
        sw = os.path.getsize(wait_path)
        sd = os.path.getsize(done_path)
    except Exception:
        return True

    if sw != sd:
        return True

    try:
        with open(wait_path, 'rb') as fw, open(done_path, 'rb') as fd:
            while True:
                bw = fw.read(chunk_size)
                bd = fd.read(chunk_size)
                if not bw and not bd:
                    return False
                if bw != bd:
                    return True
    except Exception:
        return True
# =============================================================================
def get_base_webdav_and_base_dir() -> tuple:
    base_webdav = (param_webdav_path if param_webdav_path else WEBDAV_HOME)
    base_dir = os.path.join(base_webdav, param_date_traitement)
    return base_webdav, base_dir
# =============================================================================
def match_domain_destination(destination_dir: str) -> tuple:
    _, base_dir = get_base_webdav_and_base_dir()
    destination_norm = destination_dir.replace("\\", "/").rstrip("/")

    for domain, paths in DOMAIN_WAIT_DONE_PATHS.items():
        wait_dir = os.path.join(base_dir, paths["WAIT"]).replace("\\", "/").rstrip("/")
        done_dir = os.path.join(base_dir, paths["DONE"]).replace("\\", "/").rstrip("/")
        if destination_norm == wait_dir:
            return domain, "WAIT", wait_dir, done_dir
        if destination_norm == done_dir:
            return domain, "DONE", wait_dir, done_dir

    return None, None, None, None
# =============================================================================
def build_done_index(done_dir: str) -> dict:
    done_index = {}
    try:
        for done_file in os.listdir(done_dir):
            done_path = os.path.join(done_dir, done_file)
            if not os.path.isfile(done_path):
                continue
            key = compute_logical_key(done_file)
            if key not in done_index:
                done_index[key] = done_path
    except Exception as error:
        logger.warning('Erreur indexation DONE [%s] (%s)' % (done_dir, str(error)))
    return done_index
# =============================================================================
def copy_task_priority(copy_task: dict) -> int:
    destination_path = copy_task.get("destination", "").replace("\\", "/")
    if "/DONE" in destination_path:
        return 0
    if "/WAIT" in destination_path:
        return 1
    return 2
# =============================================================================
def prepare_copy_plan_from_reference() -> tuple:
    """
    Lit le CSV de reference, applique les regles de filtrage et construit une
    liste d'instructions de copie (plan de copie) vers WebDAV.

    Retour:
        (copy_plan, rc) :
            copy_plan (list[dict]):
        Une liste de dicts : [
            {"source": ..., "destination": ..., "files": [...] , "purge": bool},
            ...
        ]
        rc (int): code retour (RC_OK / RC_CONFIG_NOT_FOUND / RC_CONFIG_INVALID / RC_NOTHING_TO_DO)
    """
    # Fichier de configuration (mapping) present
    if not os.path.exists(param_ref_mapping_path):
        logger.error('Fichier configuration introuvable [%s]' % param_ref_mapping_path)
        return [], RC_CONFIG_NOT_FOUND

    try:
        logger.info('Lecture fichier reference [%s]' % param_ref_mapping_path)
        reference_df = pd.read_csv(param_ref_mapping_path, sep=";", dtype=str)
        reference_df = reference_df.fillna('')
        reference_df.columns = reference_df.columns.str.strip()
    except Exception as error:
        logger.error('Fichier reference invalide [%s] (%s)' % (param_ref_mapping_path, str(error)))
        return [], RC_CONFIG_INVALID

    # Colonnes obligatoires
    required_cols = {PAR_FILE_TYPE, PAR_SOURCE_PATH, PAR_DESTINATION_PATH}
    missing_req = [c for c in required_cols if c not in reference_df.columns]
    if missing_req:
        logger.error('Colonnes obligatoires manquantes dans le CSV [%s]' % ','.join(missing_req))
        return [], RC_CONFIG_INVALID

    # Selection par type (CLEVA par defaut, ou CLEVADSN si mode full)
    mode_full = (param_mode_copie.upper() == "CLEVADSN")
    rows = []
    for _, row in reference_df.iterrows():
        row_type = (row[PAR_FILE_TYPE] or '').strip().upper()
        if mode_full:
            if row_type != "CLEVADSN":
                continue
        else:
            if row_type != MODE_COPY_PAR:
                continue
        rows.append(row)

    # Colonnes dynamiques
    prefix_columns = extract_header_columns(reference_df, base_name_header=PAR_PREFIX_BASE)
    extension_columns = extract_header_columns(reference_df, base_name_header=PAR_EXTENSION_BASE)
    exclude_prefix_columns = extract_header_columns(reference_df, base_name_header=PAR_EXCLUDE_PREFIX)

    copy_plan = []

    # Construction du plan
    for row in rows:
        file_type = (row[PAR_FILE_TYPE] or '').strip().upper()

        # Base source selon type
        if param_interface_path:
            source_base = param_interface_path
        else:
            if file_type == "CLEVA":
                source_base = CLEVA_DATA_HOME
            elif file_type == "DSN":
                source_base = DSN_DATA_HOME
            elif file_type == "CLEVADSN":
                source_base = CLEVA_DATA_HOME
            else:
                logger.error('Type fichier invalide [%s]' % file_type)
                return [], RC_CONFIG_INVALID

        source_dir = (row[PAR_SOURCE_PATH] or '').strip()
        destination_dir = (row[PAR_DESTINATION_PATH] or '').strip().lstrip("/")

        source_path = os.path.join(source_base, source_dir)

        destination_base = param_webdav_path if param_webdav_path else WEBDAV_HOME
        destination_path = os.path.join(
            destination_base, param_date_traitement, destination_dir
        ).replace("\\", "/")

        if not os.path.exists(source_path):
            logger.warning('Repertoire source introuvable [%s]' % source_path)
            # on ignore cette ligne et on continue a construire le plan
            continue

        # Extraction des valeurs dynamiques
        prefixes = [
            str(row[col]).strip()
            for col in prefix_columns
            if col in row and str(row[col]).strip()
        ]
        extensions = [
            str(row[col]).strip()
            for col in extension_columns
            if col in row and str(row[col]).strip()
        ]
        exclude_prefixes = [
            str(row[col]).strip()
            for col in exclude_prefix_columns
            if col in row and str(row[col]).strip()
        ]

        # Normalisation liste prefixes: déduplication et suppression du préfixe générique '*' s'il existe un préfixe plus spécifique
        if prefixes:
            normalized_prefixes = []
            for p in prefixes:
                if p not in normalized_prefixes:
                    normalized_prefixes.append(p)
            if '*' in normalized_prefixes and any(p != '*' for p in normalized_prefixes):
                normalized_prefixes = [p for p in normalized_prefixes if p != '*']
                logger.debug("Prefix '*' ignoré car d'autres prefixes spécifiques présents: %s" % normalized_prefixes)
            prefixes = normalized_prefixes

        # Politique de date: descendre dans le dernier sous-dossier YYYYMMDD
        date_policy_value = ''
        if DATE_POLICY in reference_df.columns:
            raw_policy = row[DATE_POLICY]
            if raw_policy is not None:
                date_policy_value = str(raw_policy).strip().upper()

        if date_policy_value == 'LATEST_YYYYMMDD':
            logger.debug('Politique date latest_yyyymmdd active [%s]' % source_path)
            latest_arch_dir = find_latest_yyyymmdd_subdir(source_path)
            if latest_arch_dir is None:
                logger.debug('Ligne ignoree: aucun sous-dossier date sous [%s]' % source_path)
                continue
            source_path = latest_arch_dir

        # Construction des patterns depuis le referentiel CSV
        filename_masks = []
        if prefixes and extensions:
            for prefix_value in prefixes:
                for extension_value in extensions:
                    filename_masks.append(prefix_value + extension_value)
        elif not prefixes and extensions:
            filename_masks = list(extensions)
        elif prefixes and not extensions:
            filename_masks = list(prefixes)

        # Normalisation & déduplication des patterns (suppression doublons et multi-*)
        if filename_masks:
            cleaned = []
            seen = set()
            for raw in filename_masks:
                # remplace suites de * par un seul * (ex: '**.par' -> '*.par')
                norm = re.sub(r'\*{2,}', '*', raw)
                if norm not in seen:
                    seen.add(norm)
                    cleaned.append(norm)
            if len(cleaned) != len(filename_masks):
                logger.debug(
                    'Patterns normalisés/dédupliqués de %s à %s: %s' % (len(filename_masks), len(cleaned), cleaned))
            filename_masks = cleaned

        if not filename_masks:
            logger.debug('Aucun pattern pour [%s], ligne ignoree' % source_path)
            continue

        excludes = exclude_prefixes or []
        logger.info(
            'Plan correspondance src:[%s] dest:[%s] :',
            source_path[source_path.find('/interfaces'):],
            destination_path[destination_path.find('/tech'):],
        )
        logger.info('Inclure(%s): %s Exclure(%s): %s',
                    str(len(filename_masks)), str(filename_masks),
                    str(len(excludes)), str(excludes))

        # Recherche des fichiers correspondants
        matched_fullpaths = set()
        for filename_mask in filename_masks:
            filename_paths = os.path.join(source_path, filename_mask)
            for filename_path in glob.glob(filename_paths):
                base_filename = os.path.basename(filename_path)
                if exclude_prefixes:
                    exclude_matched = False
                    for exclude_prefix in exclude_prefixes:
                        if fnmatch(base_filename, exclude_prefix):
                            exclude_matched = True
                            break
                    if exclude_matched:
                        logger.debug(
                            'Exclusion du fichier [%s] par motif [%s]' % (filename_path, str(exclude_prefixes)))
                        continue
                try:
                    if os.path.isfile(filename_path):
                        matched_fullpaths.add(filename_path)
                except Exception as error:
                    logger.warning('Erreur sur motif_fichier [%s] (%s)' % (filename_path, str(error)))

        if not matched_fullpaths:
            logger.info('Aucun fichier a copier depuis [%s]' % source_path)
            continue

        matching_files = [os.path.basename(file_path) for file_path in matched_fullpaths]
        matching_files.sort()
        logger.info('Pret a copier %s fichiers dest[%s]' % (str(len(matching_files)),
                                                             destination_path[destination_path.find('/tech'):]))

        purge_flag = False
        if PAR_PURGE in reference_df.columns:
            purge_value = (row[PAR_PURGE] or '').strip().upper()
            purge_flag = (purge_value == 'YES')

        copy_plan.append({
            "source": source_path,
            "destination": destination_path,
            "files": matching_files,
            "purge": purge_flag
        })

    if not copy_plan:
        return [], RC_NOTHING_TO_DO

    logger.debug('Plan copie genere:\n%s' % pprint.pformat(copy_plan))
    return copy_plan, RC_OK
# =============================================================================
def copy_files_to_webdav(copy_plan: list) -> tuple:
    """
    Execute la copie selon le plan fourni. Purge eventuelle, puis copie des fichiers
    Arrete au premier echec critique et renvoie un code d'erreur

    Parametres:
        copy_plan (list[dict]): liste des taches {"source","destination","files","purge"}

    Retour:
        (final_total, rc):
            final_total (int): nombre total de fichiers copies avant eventuel arret
            rc (int): RC_OK / RC_NOTHING_TO_DO / RC_RUNTIME_ERROR
    Effets de bord:
        - Creation des dossiers destination si absents
        - Purge de fichiers dans la destination si demande
    """

    final_total = 0
    total_skipped = 0
    purged_destinations = set()
    done_index_cache = {}

    # Rien a faire si pas de plan de copie
    if not copy_plan:
        return 0, RC_NOTHING_TO_DO

    logger.info('Debut deroulement copie de tous les plans de correspondance')
    for index, task in enumerate(copy_plan):
        source_dir = task["source"]
        destination_dir = task["destination"]
        files_to_copy = task["files"]

        # Creation du dossier destination si besoin
        try:
            if not os.path.exists(destination_dir):
                os.makedirs(destination_dir, exist_ok=True)
                logger.info('Creation repertoire [%s]' % destination_dir)
        except Exception as error:
            logger.error('Erreur creation repertoire [%s] (%s)' % (destination_dir, str(error)))
            return final_total, RC_RUNTIME_ERROR

        domain, kind, wait_dir, done_dir = match_domain_destination(destination_dir)

        if done_dir and done_dir not in done_index_cache and os.path.exists(done_dir):
            done_index_cache[done_dir] = build_done_index(done_dir)

        logger.info('Debut copie des fichiers depuis src[%s] dest[%s]' % (source_dir, destination_dir))
        for file_name in files_to_copy:
            source_path = os.path.join(source_dir, file_name)

            # Ajout .txt si besoin
            if file_name.lower().endswith(".txt"):
                dest_filename_extension_txt = file_name
            else:
                dest_filename_extension_txt = file_name + ".txt"

            destination_filename = dest_filename_extension_txt

            # Renommage optionnel (desactive par defaut)
            if ENABLE_RENAME:
                destination_filename = resolve_duplicate_name(
                    destination_dir=destination_dir,
                    dest_filename=destination_filename,
                    purged_destinations=purged_destinations,
                    relance_tag="-Doublon",
                    allow_sequence=True
                )

            destination_path = os.path.join(destination_dir, destination_filename)

            # Variables pour le log
            short_source = os.path.basename(source_path)
            short_dest = destination_dir.split('/batchs', 1)[-1] if '/batchs' in destination_dir else destination_dir

            # WAIT policy: if key exists in DONE, compare (if WAIT exists), log info if different, remove WAIT, skip copy
            if kind == "WAIT" and done_dir and os.path.exists(done_dir):
                done_index = done_index_cache.get(done_dir, {})
                key = compute_logical_key(os.path.basename(destination_path))
                done_equiv_path = done_index.get(key)

                if done_equiv_path:
                    if os.path.exists(destination_path):
                        is_diff = files_are_different_streaming(destination_path, done_equiv_path)
                        if is_diff:
                            logger.info(
                                'Doublon avec contenu different WAIT[%s] DONE[%s]',
                                os.path.basename(destination_path), os.path.basename(done_equiv_path)
                            )
                        try:
                            os.remove(destination_path)
                            logger.info('Netoyage doublon WAIT [%s]',
                                        os.path.basename(destination_path))
                        except Exception as error:
                            logger.error('Erreur suppression fichier WAIT [%s] (%s)', destination_path, str(error))
                            return final_total, RC_RUNTIME_ERROR

                    logger.debug('[SKIP] Fichier deja present en DONE/WAIT [%s]', short_source)
                    total_skipped += 1
                    continue

            # Copie incrementale: skip si existe
            try:
                if os.path.exists(destination_path):
                    total_skipped += 1
                    logger.debug('[SKIP] Source [%s] deja present destination [%s]', short_source, short_dest)
                    continue

                shutil.copy2(source_path, destination_path)
                final_total += 1
                logger.info('Source: [%s] [%s]' % (final_total, short_source))

                if kind == "DONE" and done_dir and done_dir in done_index_cache:
                    key = compute_logical_key(os.path.basename(destination_path))
                    if key not in done_index_cache[done_dir]:
                        done_index_cache[done_dir][key] = destination_path

            except Exception as error:
                logger.error('Echec copie [%s] (%s)' % (source_path, str(error)))
                return final_total, RC_RUNTIME_ERROR

        logger.info('Fin copie fichier depuis [%s] vers [%s]' % (source_dir, destination_dir))

    if final_total == 0:
        logger.info('Total fichiers skip deja present [%s]', str(total_skipped))
        return 0, RC_NOTHING_TO_DO

    return final_total, RC_OK
# =============================================================================
def main() -> int:
    """
    Point d'entree principal
    - Initialisation du logger une fois les args parses
    - Construction du plan puis execution de la copie
    - Retourne un code de sortie unique, utilise par __main__ pour sys.exit()
    """
    # Parse des arguments (capture d'une eventuelle erreur argparse)
    try:
        _ = parseArgs()
    except SystemExit:
        # argparse a deja affiche l'usage; on traduit en code retour standard
        return RC_BAD_ARGS

    # Definir les variables selon domaine CLEVA ou DSN
    set_reference_batch_vars(MODE_COPY_PAR)

    # Initialiser le logger sur la base des parametres
    logger_path_generation()
    startLogger()

    logger.info(PREFIX_MSG + ' Demarrage')

    # Construire le plan
    copy_plan, rc_plan = prepare_copy_plan_from_reference()
    if rc_plan != RC_OK:
        if rc_plan == RC_NOTHING_TO_DO:
            logger.warning('Aucun fichier a copier RC[%s]' % rc_plan)
        logger.info(PREFIX_MSG + ' Fin')
        return rc_plan

    copy_plan.sort(key=copy_task_priority)

    logger.info('Plan copie genere avec [%s] taches' % str(len(copy_plan)))

    # Copie si plan non vide
    if copy_plan:
        final_total, rc_copy = copy_files_to_webdav(copy_plan)
        if rc_copy == RC_OK:
            logger.info('Copie terminee vers WebDAV Total[%s] fichiers' % str(final_total))
        elif rc_copy == RC_NOTHING_TO_DO:
            logger.info('WebDAV a jour RC[%s]' % rc_copy)
        else:
            # RC_RUNTIME_ERROR
            logger.error('Traitement interrompu suite a une erreur critique RC[%s]' % rc_copy)
            logger.info(PREFIX_MSG + ' Fin')
            return rc_copy
    logger.info(PREFIX_MSG + ' Fin')
    return rc_copy
# =============================================================================
if __name__ == '__main__':
    # Traces initiales avant logger
    log_before_logger('Init: %s' % ThisProgramVersion)
    log_before_logger('Init: Parsing des parametres d\'entree ...')

    # Execution et sortie unique par code retour
    rc = main()
    sys.exit(rc)
# =============================================================================
