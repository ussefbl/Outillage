# -*- coding: utf-8 -*-
"""
======================================================================================================================
PROJET  : COLLECTIVE - 2024
MISSION: Personnalisation des .par ajout argument/ update argument

Fonctionnalites majeures :
- Ajout/Mise a jour des arguments dans les fichiers .par
- Gestion specifique pour l'argument 'moisprincipaldeclar':
    - si date de traitement >= 15/MM alors l'argument sera 01/M-1 Sinon l'argument sera 01/M-2
======================================================================================================================
Parametre :
./programme.py -v info ....
-v info|debug|...       Niveau de verbosite : fatal error info warn debug
--log_shell_path        Chemin de stockage du log genere par ce "present" programme
-d DATETRAITEMENT       Date YYYYMMDD permettant de calculer la date pour l'argument 'moisprincipaldeclar'
--archive_original      Aide au developpeur: Bypass l'activation du archive_original_files du ../param/*.properties
--forcefeature          Aide au developpeur
======================================================================================================================
Chemin du log /data/package/clevacol/envir/log/shell/
======================================================================================================================
# Cinematique
# 16h Generateur .PAR (en local serveur)
# Copie ./batch/yyyymmdd/*.par vers ./batch/pars/*
# CustomizePar (en local serveur)
# Eclateur (facture unique) (en local serveur)
# Copie Webdav
* la regle des msg est de ne pas mettre des ''  : Impossible de definir une valeur a 'moisPrincipalDeclare' pour
======================================================================================================================
Archivage desactive
        # Renommer directement le fichier original en _updated (sans suppression)


======================================================================================================================
TODO:


======================================================================================================================
"""

import csv
import argparse
import getpass
import glob
import re
import shutil
import sys
import logging
import os
import time

import pandas as pd
import datetime

#############################################################################################################################
LOG_ERROR = 'ERROR'
LOG_WARN = 'WARN'
LOG_INFO = 'INFO'
LOGLEVEL_LIST = [LOG_ERROR,LOG_WARN,LOG_INFO]


RC_SUCCESS = 0
RC_NO_PAR_FILE = 1
RC_NO_VALID_RULES = 10
RC_FAILED_APPLY_RULES = 20


VERSION = '1.0.1'
RUNTIME_DATE = '%s' % datetime.datetime.now().strftime('%Y.%m.%d')
RUNTIME_TIME = '%s' % datetime.datetime.now().strftime('%H:%M:%S')
PROGRAM_NAME = "customizer_pars.py"
ThisProgramVersion = '%s version %s - Current Runtime: %s - %s' %\
                     (PROGRAM_NAME, VERSION, RUNTIME_DATE, RUNTIME_TIME)
LOCALTIME = datetime.datetime.now().strftime('%H%M')
SELF_LOG_DATETIME = datetime.datetime.now().strftime('%Y-%m-%d--%H-%M-%S')
PARAM_ENABLE_FILENAME = "customizer_pars" + ".properties"

# Liste des parametres inclus a la ligne de commande
param_logshell_path = ''
param_log_verbose = ''
param_dateTraitement = ''
param_archive_original = False
param_force_feature = False
this_program_log_path = ''

# REFERENCE_BATCH_TECHNIC_LOG_PATH = '/data/package/clevacol/*/log/shell/'
REFERENCE_BATCH_TECHNIC_LOG_PATH = '/data/share/interfaces/log/'
BATCH_PREFIX_LOG_FILENAME = 'rapport-clevacol-batch'
RULES_FILE_DIR = os.path.dirname(sys.argv[0]) + "/../../ressources/"

RULES_FILE_NAME = "rules_customizer_pars.csv"
RULES_FILE_PATH = RULES_FILE_DIR + RULES_FILE_NAME

PAR_FILE_DIR = '/data/share/interfaces/appcleva/batch/pars/'
PAR_FILE_MASK  = "*.par"
DATE_MASK_01MMYYYY = "01/%m/%Y"
DATE_MASK_YYYYMMDD   = "%Y%m%d"

logger = ''
init_log_msg = ''
date_plan_valid_or_invalid = False

drop_empty_line = 'all'

# -----------------------------------------------
# DF _ Colonne
RULE_NUM = 'RULES_NUM'
RULE_ACTIVE = 'RULE_ACTIVE'
BATCH_CODE = 'BATCH_CODE'
MODE_TRT = 'MODE'
ARGUMENT = 'KEY'
VALEUR = 'VALUE'
RULE__DATE_MOIS_PRECEDENT = "DATE_MOIS_PRECEDENT"

############################################################################################################################
### Public library - # https://gist.github.com/techtonik/5694830
def findfiles(filemask, search_path='.'):
    '''Returns list of filenames from `where` path matched by 'which' shell pattern. Matching is case-insensitive.'''
    # # TODO: recursive param with walk() filtering
    # rule = re.compile(fnmatch.translate(which), re.IGNORECASE)
    # return [name for name in os.listdir(where) if rule.match(name)]
    searchfiles = search_path + filemask
    foundfiles = glob.glob(searchfiles)
    return foundfiles
#############################################################################################################################
#
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    return False
#############################################################################################################################
#
def is_utf8_str(s):
    try:
        s.decode('utf-8')
        return True
    except UnicodeError:
        return False

#############################################################################################################################
#
def decode_utf8_str(s):
    decode_utf8_str = s
    if isinstance(s, (bytes, bytearray)):
        try:
            decode_utf8_str = s.decode('utf-8')
        except UnicodeError:
            pass
    return decode_utf8_str

#############################################################################################################################
#############################################################################################################################
def parseArgs():
    global  \
        param_logshell_path, param_log_verbose, param_dateTraitement, param_force_feature, param_archive_original, par_file_path

    parser = argparse.ArgumentParser(prog=ThisProgramVersion.split('-')[0],
                                    formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''(*) Param_DatePlan obligatoire''',
        description="""
         >> Mission :
         >> Script de Personnalisation des .par : ajout/mise a jour des parametres dans les .par
         >> Calculer 'moisPrincipalDeclare' en fonction de la date plan.
         >> Utilisation : python customizer_pars.py --date_plan <DD/MM/YYYY>
         """)
    parser.add_argument('-V', '--version', action='version', version='%(prog)s')

    parser.add_argument('-d', type=str, metavar='dateTraitement', required=True,
                                help='Date du plan au format: YYYYMMDD')

    ## Chemin pour le log de ce propre shell python
    parser.add_argument('--logshell_path',  type=str, help='Chemin explicite du log python')

    ## Niveau de verbosite
    parser.add_argument('-v', '--verbose', type=str, metavar='Log_Level', nargs='?', const='info',
                        choices=['debug', 'info', 'warn', 'error', 'critical'], default='info',
                        help='Definition du niveau de logger,\n debug | info | warning | error | critical')

    ## Bypass l'activation du feature par contenu du ../param/*.properties (alimente par TFS) (for DEVELOPER)
    parser.add_argument('--forcefeature',   action='store_true', help='Ignore l\'activation par properties')

    ## Bypass l'activation du archive_original_files du ../param/*.properties (alimente par TFS) (for DEVELOPER)
    parser.add_argument('--archive_original', action='store_true', help='Activation archivage file')

    ## ----------------------------------

    input_args = parser.parse_args()
    log_before_logger('Init: %s' % str(input_args))
    log_before_logger('Init: Chemin d\'execution [%s]' % os.getcwd())
    log_before_logger('Init: Contexte utilisateur [%s]' % getpass.getuser())

    if input_args.d :
        param_dateTraitement = str(input_args.d).strip()
        log_before_logger('Init: Date Traitement [%s]' % param_dateTraitement)
        par_file_path = add_path_trailing_slash(PAR_FILE_DIR)

    if input_args.verbose:
        param_log_verbose = input_args.verbose.upper()
        log_before_logger('Init: Mode [%s] actif [%s]' % ('log_Verbose',param_log_verbose))

    ##This program log
    if input_args.logshell_path :
        if input_args.logshell_path and not (input_args.logshell_path.replace("\\", "/").endswith("/")):
            input_args.logshell_path = input_args.logshell_path  + '/'
        param_logshell_path = input_args.logshell_path.replace('\\', '/')
        log_before_logger('Init: Mode [%s] actif [%s]' % ('Logshell_path',param_logshell_path))

    if input_args.forcefeature:
       param_force_feature = True
       log_before_logger('Init: Mode [%s] actif [%s]' % ('ForceFeature', param_force_feature))

    if input_args.archive_original:
        param_archive_original = True
        log_before_logger('Init: Mode [%s] actif [%s]' % ('ForceArchiveOriginal', param_archive_original))
    if not check_feature_enabled() and not param_force_feature:
        # capte toutes properties, dont PROP_LINE_PALIER_ENABLE
        log_before_logger('Feature Disabled. Quitting...')
        return None

    return True

#############################################################################################################################
def startLogger() -> str:
    """Initialise un mecanisme de log et renvoi la reference au chemin utilise"""
    global logger
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
    # Masque genere : ./programme-2021-02-05--16-28-12.log
    thisProgramFilename =  os.path.splitext(os.path.basename(__file__))[0]
    # fiabiliser le path qui peut ne pas contenir un / de fin
    logger_filename = '%s-%s-%s.log' % (BATCH_PREFIX_LOG_FILENAME, thisProgramFilename,  SELF_LOG_DATETIME)
    logger_fullpath = this_program_log_path + logger_filename

    log_before_logger('Init: Chemin du log : [%s]' % logger_fullpath)

    fileHandler = logging.FileHandler(logger_fullpath, mode='a') #,encoding='utf-8')
    fileHandler.setFormatter(formatter)

    # Il faut tester args.v avant son usage :
    log_before_logger('Init: Niveau de log [%s]' % param_log_verbose)
    fileHandler.setLevel(getattr(logging, param_log_verbose ))

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)

    log = logging.getLogger(thisProgramFilename)

    # getattrib transforme un "DEBUG", en valeur compatible pour setlevel()
    log.setLevel(getattr(logging, param_log_verbose ))
    log.addHandler(fileHandler)
    log.addHandler(consoleHandler)

    logger = log
    # Insert INIT already existing information
    for lines in init_log_msg.split('\n'):
        if len(lines)>0:
            log.info(lines)

    return logger_fullpath
#############################################################################################################################
# Les messages necessaire avant LOGGER sont ecrit sur STDOUT et stocke pour stockage en LOGGER (webdav) ensuite
def log_before_logger(msg):
    global init_log_msg
    print(msg)
    init_log_msg = init_log_msg +'\n' + msg

#############################################################################################################################
# Defini le chemin d' ecriture du log (./ ou /standardUnix ou specific ....)
def logger_path_generation():
    global this_program_log_path, param_logshell_path

    log_before_logger('Init: Identification du chemin de log du self python')

    param_logshell_path = add_path_trailing_slash(param_logshell_path)

    # Check des repertoires standards
    log_before_logger('Init: Check existence [%s]' % REFERENCE_BATCH_TECHNIC_LOG_PATH)
    standard_unix_logpath_found = False
    standard_unix_logpath = ''
    try:
        file_list = glob.glob(REFERENCE_BATCH_TECHNIC_LOG_PATH + '/*.log')
        if len(file_list) > 0:
            standard_unix_logpath_found = True
            standard_unix_logpath = add_path_trailing_slash(os.path.dirname(file_list[0]))
            log_before_logger('Init: Check existence [%s]: OK' % standard_unix_logpath)
        else:
            log_before_logger('Init: Check existence [%s]: KO' % REFERENCE_BATCH_TECHNIC_LOG_PATH)
    except IOError:
        log_before_logger('Init: Check existence [%s]: KO' % REFERENCE_BATCH_TECHNIC_LOG_PATH)

    # Verification du chemin de log python s'il est indique
    ## Chemin de log en parametre de ligne de commande
    if len(param_logshell_path) > 0 and os.path.exists(param_logshell_path):
        this_program_log_path = param_logshell_path
    ## Chemin stantard des serveurs unix
    elif standard_unix_logpath_found:
        this_program_log_path = standard_unix_logpath
    else:
        log_before_logger('Init: Logshell_path usage du standard [./]')
        this_program_log_path = './'  ### current working directory
    return this_program_log_path

#############################################################################################################################
# Norme: tous nos repertoires se finissent par un /
def add_path_trailing_slash(directory=''):
    if len(directory) > 0 and not (directory.endswith('/')):
        directory = directory + os.path.sep
    return directory

#############################################################################################################################
def check_feature_enabled():
    global archive_original_files, param_archive_original

    paramDir = os.path.join(os.path.dirname(sys.argv[0])+"/../../param/")
    paramFullPath = os.path.join(paramDir + PARAM_ENABLE_FILENAME)

    # Check param file exist
    log_before_logger("Init: Checking FeatureEnableParam [%s]" % paramFullPath)
    if not os.path.exists(paramDir):
        log_before_logger("Init: FeatureEnableParam directory is missing [%s]" % str(paramDir))
        return False
    if not os.path.isfile(paramFullPath):
        log_before_logger("Init: FeatureEnableParam file is missing [%s]" % str(paramFullPath))
        return False

    # Check: function_enable=yes
    dict_props = parse_properties_file(paramFullPath)

    archive_key = 'function_enable_archive_original_files'
    key='function_enable'
    val='yes'
    check_enable = False
    if key in dict_props and dict_props[key].lower() == val:
        check_enable = True
        log_before_logger("Init: Lecture properties : Feature = [%s]" % check_enable)

    if archive_key in dict_props and dict_props[archive_key].lower() == val:
        archive_original_files = True
        log_before_logger('Init: Lecture properties : archive_original = [%s]' % archive_original_files)
    else:
        archive_original_files = False
        log_before_logger('Init: Lecture properties : archive_original = [%s]' % archive_original_files)
    if param_archive_original:
        archive_original_files =True

    return check_enable

#############################################################################################################################
def parse_properties_file(paramFullPath: str) -> dict:
    dict_properties = {}
    with open(paramFullPath, 'r') as prop_file:
        for prop_line in prop_file:
            prop_line = prop_line.strip()
            if not prop_line or prop_line.startswith('#'):
                continue
            if '=' in prop_line:
                prop_key, prop_value = prop_line.split('=', 1)
                prop_key, prop_value = prop_key.strip(), prop_value.strip()
                dict_properties[prop_key] = prop_value
    return dict_properties

#############################################################################################################################
def _______Zone_Fonction__Specific_A_CE_TRAITEMENT():
    pass #Simple delimiteur pour voir facilement dans Pycharm : Structure View Left Panel

#############################################################################################################################
def check_rules_file(rule_file_path: str, rule_file_name: str, date_traitement_YYYYMMDD: str) \
        -> (bool, pd.DataFrame):
    # Verifier la validite des rules dans le fichier de reference_rules
    # identifier et ignorer les valeurs en double pour le meme BATCH_CODE

    ## LOAD REFERENTIEL .CVS
    logger.info('Rules loading [%s/%s]' % (rule_file_path, rule_file_name))
    try:
        if not os.path.exists(rule_file_path):
            logger.error('Rules file missing [%s]' % rule_file_path)
            return (False, pd.DataFrame())
        df_data_rules = pd.read_csv(rule_file_path, delimiter=';')
        # Supprime les lignes complétement vide
        df_data_rules = df_data_rules.dropna(how= drop_empty_line)
        # Nettoyage des cellules pour enlever les espaces inutiles
        df_data_rules = df_data_rules.applymap(lambda cell: cell.strip() if isinstance(cell, str) else cell)

        if df_data_rules.empty:
            logger.debug('Rules file empty [%s]' % rule_file_name)
        else:
            df_data_rules[RULE_ACTIVE] = df_data_rules[RULE_ACTIVE].astype(str)

    except Exception as e:
        logger.error("Error while reading [%s] [%s]" % (rule_file_name, e))
        return (False, pd.DataFrame)

    ## CHECK each rule in REFEReNTIEL
    def check_valid_line_rules(row):
        try:
            # Check valide RULES_NUM
            # "^R\d{3}" = lettre majiscule + trois chiffres exemple R001
            check_rules_num = row.get(RULE_NUM, '')
            if not check_rules_num or not re.match(r'^R\d{3}$', check_rules_num):
                logger.debug('Invalid rule RULES_NUM [%s]' % check_rules_num)
                return False

            # Check BATCH_CODE existe
            check_batch_code = row.get(BATCH_CODE, '')
            if not check_batch_code:
                logger.debug('Invalid rule BATCH_CODE [%s]' % check_batch_code)
                return False

            # Check MODE seule 'new' 'update' insensible a la casse
            check_mode = row.get(MODE_TRT, '').lower()
            if check_mode not in ['new', 'update']:
                logger.debug('Invalid rule MODE [%s]' % check_mode)
                return False

            # Check KEY existe et ne contient pas d'espace
            check_key = row.get(ARGUMENT, '')
            if not check_key or ' ' in check_key:
                logger.debug('Invalid rule KEY [%s]' % check_key)
                return False

            # Check VALUE existe
            check_value = row.get(VALEUR, '')
            if not check_value or ' ' in check_value:
                logger.debug('Invalid rule VALUE [%s]' % check_value)
                return False

            # Check si la ligne de rule est active
            check_rules_active = row.get(RULE_ACTIVE, '').upper()
            if check_rules_active != 'TRUE':
                logger.debug('Invalid rule RULE_ACTIVE [%s]' %  check_rules_active)
                return False
        except:
            logger.error("Rules file empty [%s]" % rule_file_name)
            return False
        return True
    # application des validations ligne par ligne
    logger.info('Check rules [%s]' % rule_file_name)
    df_data_rules['VALIDE'] = df_data_rules.apply(check_valid_line_rules, axis=1)

    # Separation des lignes valides et invalides
    df_valid_rules = df_data_rules[df_data_rules['VALIDE']].copy()
    df_invalid_rules = df_data_rules[~df_data_rules['VALIDE']].copy()
    df_rule_unable_to_generate = pd.DataFrame(columns=df_valid_rules.columns)

    ## REFERENTIEL (VALUE) = [ FixValue  |  RULE_NAME ]
    ## RULE_xxx = [ RULE__DATE_MOIS_PRECEDENT, ..... ]

    # Recuperer les rules ayant la valeur de la REGLE_01 = DATE_MOIS_PRECEDENT
    df_rule_date_mois_precedent = df_valid_rules[df_valid_rules[VALEUR] == RULE__DATE_MOIS_PRECEDENT]

    # Verifie que la generation de la REGLE_01 se passe bien
    # Sinon exclure les lignes qui contient cette regle du df_valid_rules
    # df_rule_unable_to_generate contient les lignes de la REGLE_01
    if not df_rule_date_mois_precedent.empty:
        success, _ = genere_rule_mois_principal_declare(date_traitement_YYYYMMDD)
        if not success:
            df_rule_unable_to_generate = df_rule_date_mois_precedent.copy()
            # Exclure les rules de df_valid_rules
            df_valid_rules = df_valid_rules.drop(index=df_rule_date_mois_precedent.index)

    # Loguer les invalides
    if not df_invalid_rules.empty:
        for idx, row in df_invalid_rules.iterrows():
            logger.warning(
                "Invalid rule RULES_NUM [%s] BATCH_CODE [%s] MODE [%s] KEY [%s] VALUE [%s] RULE_ACTIVE [%s]" %
            (row[RULE_NUM], row[BATCH_CODE], row[MODE_TRT], row[ARGUMENT], row[VALEUR], row[RULE_ACTIVE]))

    # loguer les numero de rules unable to generate
    if not df_rule_unable_to_generate.empty:
        rule_unable_to_generate_list = df_rule_unable_to_generate[RULE_NUM].tolist()
        logger.warning("Unable to generate value for RULES_NUM [%s] DATE PLAN [%s]" % (rule_unable_to_generate_list, date_traitement_YYYYMMDD))

    if df_valid_rules.empty:
        logger.debug("No valid rule '[%s]'" % rule_file_path)
        return (False, df_invalid_rules)

    for idx, row in df_valid_rules.iterrows():
        logger.info(
            "Valid rule RULES_NUM [%s] BATCH_CODE [%s] MODE [%s] KEY [%s] VALUE [%s] RULE_ACTIVE [%s]" %
            (row[RULE_NUM], row[BATCH_CODE], row[MODE_TRT], row[ARGUMENT], row[VALEUR], row[RULE_ACTIVE]))

    # verification des doublons
    # Condition: meme BATCH_CODE et KEY avec differentes VALUE
    # les doublons seront ignores
    duplicates = df_valid_rules.duplicated(subset=[BATCH_CODE, ARGUMENT], keep=False)
    df_duplicated_rules = df_valid_rules[duplicates]
    rule_num_list = df_duplicated_rules[RULE_NUM].tolist()

    if df_duplicated_rules.empty:
        logger.info('Check rules:  [OK]')
    else:
        logger.info("[%s] Count duplicated rules detected [%s]" % (df_duplicated_rules.shape[0],rule_num_list))
        grouped_duplicated_rules = df_duplicated_rules.groupby([BATCH_CODE, ARGUMENT])
        # exclure les lignes en doublon
        for (batch_code, key), batch_code_key_group in grouped_duplicated_rules:
            dublicated_values = batch_code_key_group[VALEUR].unique()
            logger.debug('Batch Code [%s] [%s] [%s]' % (batch_code,key,dublicated_values))
            if len(dublicated_values) > 0:
                rules_num = batch_code_key_group[RULE_NUM].tolist()
                logger.warning('RULE ignored RULES_NUM [%s] BATCH_CODE [%s] KEY [%s] VALUES [%s]' % (rules_num,batch_code,key,dublicated_values))
                df_valid_rules = df_valid_rules.drop(batch_code_key_group.index)

    return (True,df_valid_rules)

############################################################################################################################
def genere_rule_mois_principal_declare(date_traitement_YYYYMMDD: str) -> (bool, str):
    # Fonction pour calculer la valeur pour arguement 'moisprincipaldeclar' au format 'DD/MM/YYYY'
    # Si Date du jour de generation du .par >= 15/MM alors l argument sera 01/M-1 avec MM le mois courant.
    # Sinon largument sera 01/M-2.

    result_date =''
    try:
        date_obj = datetime.datetime.strptime(date_traitement_YYYYMMDD, DATE_MASK_YYYYMMDD)
        if date_obj.day >= 15:
            # `date_obj.replace(day=1)` : Definit la date au premier jour du mois courant
            # `- datetime.timedelta(days=1)` : Soustrait un jour pour obtenir le dernier jour du mois precedent
            # `replace(day=1)` : Remplace la date par le premier jour du mois precedent
            target_date = (date_obj.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)
        else:
            #  Sinon, calculer le premier jour de deux mois avant (M-2)
            # Meme logique que ci-dessus mais on soustrait un mois supplementaire
            target_date = (date_obj.replace(day=1) - datetime.timedelta(days=1)).replace(day=1) - datetime.timedelta(days=1)
        # Formater la date calculee sous forme de chaine au format '01/MM/YYYY'
        result_date = target_date.replace(day=1)
        logger.debug("Definition moisPrincipalDeclare DatePlan [%s] TargetDate [%s]" %
                        (date_traitement_YYYYMMDD, result_date.strftime(DATE_MASK_01MMYYYY)))

    except ValueError as e:
        logger.error("Date plan invalid [%s] Erreur [%s]" % (date_traitement_YYYYMMDD, e))
        return (False, result_date)

    return (True, result_date.strftime(DATE_MASK_01MMYYYY))

############################################################################################################################
def apply_rules_on_par_files(df_rules: pd.DataFrame, par_file_path: str, date_traitement_YYYYMMDD: str) \
        -> (bool, int):
    # Recherche les .PAR depuis "par_file_path" et appliquer l'ensemble des règles

    if len(df_rules) == 0:
        logger.info("No valid rule, no file processing")
        return (False, pd.DataFrame())

    # Isoler les BATCH_CODE (unique) car plusieurs regle peuvent solliciter le meme BATCH_CODE
    df_grouped_rules = df_rules.groupby(BATCH_CODE)
    valid_par_batch_code = set(df_rules[BATCH_CODE])

    par_file_list = findfiles(filemask=PAR_FILE_MASK, search_path=par_file_path)
    if len(par_file_list) == 0:
        logger.info('No par file found [%s] [%s]'% (par_file_path, PAR_FILE_MASK))
        return RC_NO_PAR_FILE

    pars_files_treated = 0
    # Parcourir les fichiers .par
    for parfilepath in par_file_list:
        with open(parfilepath, 'r', encoding='utf-8') as par_file_read:
            par_file_lines = par_file_read.readlines()

        # Verifier si une ligne contient exactement le batch_code
        batch_code_value = ''

        # Verifier si la ligne commence par 'BATCH_CODE'
        for par_file_line in par_file_lines:
            if par_file_line.strip().upper().startswith("BATCH_CODE\t"):
                    key, value = par_file_line.split("\t", 1)
                    batch_code_value = value.strip()
                    logger.info("Batch code found in PAR [%s] BATCH_CODE [%s]" % (parfilepath, batch_code_value))
                    par_filename = os.path.basename(parfilepath)
                    break

        if not batch_code_value or (batch_code_value not in valid_par_batch_code):
            continue
        logger.debug('PAR to update [%s]' % par_filename)

        if batch_code_value in df_grouped_rules.groups:
            rules_to_apply = df_grouped_rules.get_group(batch_code_value)
            logger.info("Rules to apply Nb[%s] PAR [%s] BATCH_CODE [%s] [%s]" % (len(rules_to_apply), par_filename, batch_code_value, rules_to_apply[[ARGUMENT, VALEUR]].to_dict(orient='records')))
            # Mise a jour des .pars
            (apply_success, updated_lines) = apply_rules_on_single_par_file(
                par_file_lines, rules_to_apply, date_traitement_YYYYMMDD, batch_code_value, par_filename)
            # sauvegarde des .pars
            if apply_success:
                save_updated_file(parfilepath, updated_lines)
                pars_files_treated += 1
        else:
            logger.info("No rule applies BATCH_CODE[%s] PAR [%s]" % (batch_code_value,par_filename))
    logger.info("Total PAR updated: [%s]" % pars_files_treated)
    return RC_SUCCESS

############################################################################################################################
def apply_rules_on_single_par_file(par_file_lines: list,
                                   rules_to_apply: pd.DataFrame, date_traitement_YYYYMMDD: str,
                                   batch_code: str, par_filename: str) \
        -> (bool, list):
    """
    Applique les regles pour chaque fichier .par
    Mode UPDATE modifie le .par si KEY existe dans .par
    Mode NEW    modifie le .par avec la nouvelle KEY/VALUE

    Ex Rule :
      BATCH_CODE;           MODE;   KEY;                    VALUE;
      DSN-ANALYSECHANGEMENT;new;    moisPrincipalDeclare;   DATE_MOIS_PRECEDENT;
      MonBatchCode;         new;    maNouvelleClé;          GRAA
    La valeur indiquée est soit une valeur fixe (ici GRAA)
    soit le nom d'une "METHODE" générant la valeur à calculer : ici "DATE_MOIS_PRECEDENT"
    """
    par_basename = os.path.basename(par_filename)
    #par_file_lines = None

    # First, Get BATCH_CODE from PARS_FILE_LINES
    # .......

    ## Parcourir les rules
    update_par_sucess = False
    for idx, rule in rules_to_apply.iterrows():
        KEY_SEARCH = rule[ARGUMENT]
        RULENAME_OR_FIXVALUE = rule[VALEUR]
        MODE_UPDATE_OR_NEW = rule[MODE_TRT].lower()

        # Limite apply rule on batch_code related
        # if batch_code ==  BATCH_CODE_TO_APPLY:


        # REGLE_01 Gestion specifique pour 'moisPrincipalDeclare'
        if RULENAME_OR_FIXVALUE == RULE__DATE_MOIS_PRECEDENT:
            (success, value) = genere_rule_mois_principal_declare(date_traitement_YYYYMMDD)
            if success:
                RULENAME_OR_FIXVALUE = value
            else:
                # On en doit jamais arriver en FAILED car le CHECK initial a écarter la règle INVALIDE
                # logger.warning("Rule PAR [%s] KEY [%s] VALUE [%s] Unable to generate" %
                               #(par_basename, KEY_SEARCH, RULE__DATE_MOIS_PRECEDENT))
                continue

        # REGLE_02 .....

        # Mise a jour rule si elle existe dans .par avec la nouvelle valeur
        key_found = False
        for lineno, line in enumerate(par_file_lines):  ##peut on modifier une liste qui est en cours de lecture ?
            if line.startswith(f"{KEY_SEARCH}\t"):
                key_found = True
                par_file_lines[lineno] = f"{KEY_SEARCH}\t{RULENAME_OR_FIXVALUE}\n"
                logger.info("Rule [Update] PAR [%s] KEY [%s] VALUE [%s]" % (par_basename, KEY_SEARCH, RULENAME_OR_FIXVALUE))
                update_par_sucess = True
                break ## Hypothese: une seule regle par ligne d'un fichier .PAR

        if not key_found and (MODE_UPDATE_OR_NEW == 'update'):
                logger.warning("Rule [Update] PAR [%s] KEY [%s] unfound" % (par_basename, KEY_SEARCH))
                continue

        # Ajouter rule si le mode 'new' et la ligne n'existe pas dans le par
        if not key_found and (MODE_UPDATE_OR_NEW == 'new'):
            fin_index = None
            for index, line in enumerate(par_file_lines):
                if line.strip() == 'FIN':
                    fin_index = index
                    break

            if fin_index is not None:
                par_file_lines.insert(fin_index, f"{KEY_SEARCH}\t{RULENAME_OR_FIXVALUE}\n")
                logger.info("Rule [New] PAR [%s] KEY [%s] VALUE[%s] " % (
                        par_filename, KEY_SEARCH, RULENAME_OR_FIXVALUE)) # rules_to_apply[[KEY_SEARCH, RULENAME_OR_FIXVALUE]].to_dict(orient='records'
                update_par_sucess = True
            else:
                logger.warning("Rule [New] PAR [%s] Line [FIN] unfound" % par_basename)

    return (update_par_sucess, par_file_lines)

#############################################################################################################################
def save_updated_file(parfilepath: str, updated_lines: list) -> bool:
    # Sauvegarde des fichiers pars modifier
    # Creation repertoire pour archiver les par original (/ORIGINAL_pars)
    dir_path = os.path.dirname(parfilepath)
    base_name = os.path.basename(parfilepath)
    name_without_ext, ext = os.path.splitext(base_name)
    if "_updated" not in name_without_ext.lower():
        updated_filename = f"{name_without_ext}_updated{ext}"
    else:
        updated_filename = base_name
    updated_filepath = os.path.join(dir_path, updated_filename)

    if archive_original_files:
        # Archivage active
        original_dir = os.path.join(dir_path, "ORIGINAL_pars")
        if not os.path.exists(original_dir):
            try:
                os.makedirs(original_dir)
                logger.info("Directory created [%s]" % original_dir)
            except Exception as e:
                logger.error("Unable to create directory [%s] [%s]" % (original_dir,e))

        original_pars_new_name = f"{base_name}.original"
        original_pars_new_path = os.path.join(original_dir, original_pars_new_name)

        # Deplacer le fichier original vers ORIGINAL_pars
        try:
            shutil.move(parfilepath, original_pars_new_path)
            logger.info("Move original file PAR [%s] [%s]" % (parfilepath, original_pars_new_path))

            # ecrire le fichier mis a jour
            with open(updated_filepath, 'w', encoding='utf-8') as file:
                file.writelines(updated_lines)
        except Exception as e:
            logger.error("Unable to move file PAR path [%s] New path [%s] [%s]" % (parfilepath,original_pars_new_path, e))
    else:
        # Archivage desactive
        # Renommer directement le fichier original en _updated
        try:
            shutil.move(parfilepath, updated_filepath)
            logger.info("Rename original PAR [%s] to [%s]" % (base_name,updated_filename))

            # Reecrire le contenu modifie dans un fichier _updated
            with open(updated_filepath, 'w', encoding='utf-8') as file:
                file.writelines(updated_lines)
        except Exception as e:
            logger.error("Unable to save PAR [%s] [%s]" % (updated_filepath, e))
    return True

#############################################################################################################################
#############################################################################################################################
def main():
    args = parseArgs()
    if args != None :
        logger_path_generation()
        startLogger()
        logger.info("%s Starting ..." % (ThisProgramVersion))
        (success, df_valid_rules) = check_rules_file(RULES_FILE_PATH, RULES_FILE_NAME, param_dateTraitement)
        if not success:
            logger.error("No rules valid in [%s]" % RULES_FILE_NAME)
            return RC_NO_VALID_RULES

        if len(df_valid_rules) > 0:
            apply_rules_on_par_files(df_valid_rules, par_file_path, param_dateTraitement)
            return RC_SUCCESS
        else:
            logger.error("Error processing files pars")
            return RC_FAILED_APPLY_RULES

    return RC_SUCCESS

#############################################################################################################################
if __name__ == "__main__":
    log_before_logger('Init: %s' % (ThisProgramVersion))
    log_before_logger('Init: Parsing des parametres d\'entree ...')

    return_code = main()
    if return_code == RC_SUCCESS:
        if isinstance(logger, str):
            log_before_logger('End %s' % PROGRAM_NAME)
        else:
            logger.info("Exit-code: COMPLETED [%s]" % return_code)
    else:
        logger.error("Exit-code: FAILED [%s]" % return_code)

    time.sleep(1)
    #logger.info("End of the script")
    exit(return_code)

#############################################################################################################################
