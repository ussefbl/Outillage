#!/bin/bash
#!/bin/ksh
#################################################################
# Script        : cp_webdav_fichierspars.sh
# Sujet         : Copie des fichiers de parametrages des traitements
#                 batchs Cleva vers Webdav
#################################################################
# fonctionnalite :
# * Creation du repertoire et copie fichiers pars sur WEBDAV
# * Eclateur FactureUnique
# * update des fichiers pars si existe deja sur WEBDAV
#################################################################
# Usage : cp_webdav_fichierspars.sh
# Parametres du Scripts
#
#################################################################
# SOUS PROGRAMMES INTERNES
#################################################################
#
# cmdPrefix : elimination du chemin
cmdPrefix() {
  typeset cmd=$(basename $0)
  # remove '-*' :
  cmd=$(expr "$cmd" : '-*\(.*\)')
  # remove filename extension :
  echo ${cmd%.*}
}

# eraseLog : vide le fichier log
eraseLog() {
  echo "" > $SCRIPTLOG
}

# traceLog : ecriture dans le fichier log du msg passe en parametre
traceLog() {
  MESSAGE=`date`" - ${1}"
  echo "${MESSAGE}" >> $SCRIPTLOG
}

# printLog : impression dans le fichier log avec datage
printLog() {
  echo "$1"
  traceLog "$1"
}

# modeHelp : affichage de l'aide en ligne
#modeHelp() {
#  printLog "Fonction modeHelp non surchargee. Aucune aide disponible."
#}

# erreurMsg : gestion d'une erreur et sortie en erreur
erreurMsg() {
  printLog "ERROR : ${1}"
  printLog "ABNORMAL SCRIPT END"
  printLog "SCRIPT LOG : ${SCRIPTLOG}"
#  exit 1
}

# erreurMsg : gestion d'une erreur et sortie en erreur
erreurMsgExit() {
  printLog "ERROR : ${1}"
  printLog "ABNORMAL SCRIPT END"
  printLog "SCRIPT LOG : ${SCRIPTLOG}"
  exit 1
}

# Fonction de test d'une variable d'environnement
# Entrée : $1, nom de la variable à tester

checkEnv() {
        ENV_VAR_NAME="\${${1}}"
        ENV_VAR_VALUE=$(eval echo ${ENV_VAR_NAME})
        if [[ "${ENV_VAR_VALUE}" = "" ]]; then
                echo "ERREUR ENVIRONNEMENT : La variable ${ENV_VAR_NAME} n'est pas définie."
                return 1
        else
                echo "Variable ${ENV_VAR_NAME}=${ENV_VAR_VALUE} définie correctement."
                return 0
        fi
}

# Fonction de test de validite de parametre de postion (xxxx-xxxx)
# Entree : $1, nom de la variable a tester

# erreurSyntaxe : gestion d'une erreur syntaxique sur la commande d'appel du script
erreurSyntaxe() {
  printLog "SYNTAX ERROR"
  modeHelp
  printLog "ABNORMAL SCRIPT END"
  printLog "SCRIPT LOG : ${SCRIPTLOG}"
  exit 1
}

# erreurSyntaxe : gestion d'une erreur syntaxique sur la commande d'appel du script
erreurSyntaxeExit() {
  printLog "SYNTAX ERROR"
  modeHelp
  printLog "ABNORMAL SCRIPT END"
  printLog "SCRIPT LOG : ${SCRIPTLOG}"
  exit 1
}

#################################################################
# SURCHARGE DU HELP
#################################################################
# modeHelp : affichage de l'aide en ligne
modeHelp() {
        printLog "Usage :"
        printLog "      Mode HELP : sh $SCRIPTNAME "
        printLog "      Mode applicatif : sh $SCRIPTNAME DATE(AAAAMMJJ)"
}

#################################################################
# INITIALISATION DE VARIABLES GLOBALES
#################################################################

DIRNAME="$(cd "$(dirname "$BASH_SOURCE")" ; pwd -P )"
UENV="$(echo "$DIRNAME" | awk -F"/" '{ print $5 }')"
PACKAGE="$(echo "$DIRNAME" | awk -F"/" '{ print $3 }')"

DATA_INTERFACES_HOME=/data/share/interfaces

APP_HOME=/app/${PACKAGE}/clevacol/${UENV}
DATA_HOME=${DATA_INTERFACES_HOME}
DATA_MIGRATION_HOME=${DATA_INTERFACES_HOME}/migration

DATA_WEBDAV_HOME=/data/share/batchs
DATA_WEBDAV_TECH_HOME=/data/share/batchs/tech

DATA_INTERFACES_TRV=${DATA_HOME}/trv
DATA_INTERFACES_OUT=${DATA_HOME}/out
DATA_INTERFACES_IN=${DATA_HOME}/in
DATA_INTERFACES_PARS=${DATA_INTERFACES_HOME}/appcleva/batch/pars

APP_SHELL_DIR=${APP_HOME}/scripts/shell
DATA_LOG_SHELL_DIR=/data/${PACKAGE}/clevacol/${UENV}/log/shell

SUFFIXE_FICHIER_PAR=par
SUFFIXE_FICHIER_PAR_ORIGINAL=original

DATE_DEB_TRAIT=`date +%d/%m/%Y:%H:%M:%S`
DATE_LOG_TRAIT=`date +%Y%m%d%H%M%S`
SCRIPTNAME=`cmdPrefix`

SCRIPTLOG=${DATA_LOG_SHELL_DIR}/${SCRIPTNAME}_ERR_${DATE_LOG_TRAIT}.log

if [[ $# -ne 1 ]]; then
    erreurSyntaxe
fi

DATE_PLAN_BATCH=$1
SCRIPTLOG=${DATA_LOG_SHELL_DIR}/${SCRIPTNAME}_${DATE_LOG_TRAIT}.log

#################################################################
# DEBUT SCRIPT
#################################################################
printLog "==========================================================="
printLog "DEBUT DU SCRIPT ${SCRIPTNAME}"
printLog "Log consultable : $SCRIPTLOG"
printLog ""
printLog " Copie des fichiers de parametrages des traitements batchs Cleva vers Webdav ...      "
printLog "============================================================"

export APP_HOME=${APP_HOME}
export DATA_HOME=${DATA_HOME}

#########################################################################################################
# Test existence DATA_HOME et APP_HOME
printLog " ===>  Verification de la validite des parametres  de traitements ..."

if [[  ! -d  ${APP_HOME} ]]  ||   [[ !  -d  ${DATA_HOME} ]]; then
	printLog " ======>  Pb de configuration de l'env:  APP_HOME= ${APP_HOME}  et  DATA_HOME=${DATA_HOME}  "
	printLog " ======>  Echec de Copie des fichiers de parametrages des traitements batchs Cleva vers Webdav "
	exit 1
fi

if [[  ! -d  ${APP_SHELL_DIR} ]]  ||   [[ !  -d  ${DATA_LOG_SHELL_DIR} ]]; then
	printLog " ======>  Pb de configuration de l'env:  APP_SHELL_DIR= ${APP_SHELL_DIR}  et  DATA_LOG_SHELL_DIR=${DATA_LOG_SHELL_DIR}  "
	printLog " ======>  Echec de Copie des fichiers de parametrages des traitements batchs Cleva vers Webdav  "
	exit 1
fi

if [[  ! -d  ${DATA_WEBDAV_TECH_HOME} ]]  ||   [[ !  -d  ${DATA_INTERFACES_PARS} ]]; then
	printLog " ======>  Pb de configuration de l'env:  DATA_WEBDAV_TECH_HOME=${DATA_WEBDAV_TECH_HOME}  et  DATA_INTERFACES_PARS=${DATA_INTERFACES_PARS}  "
	printLog " ======>  Echec Copie des fichiers de parametrages des traitements batchs Cleva vers Webdav  "
	exit 1
fi

printLog " ===>  Verification de la validite des parametres  de traitements : OK "

#########################################################################################################
##   Eclatement FactureUnique
printLog "============================================================="
printLog " ===>  Eclatement FactureUnique.par ..."
printLog "============================================================="

# old python ${APP_SHELL_DIR}/eclateur_par.py ${DATA_INTERFACES_PARS}/   >> $SCRIPTLOG
CMD_ECLATE_FACTURE_UNIQUE="${APP_HOME}/clevacol-outils-techniques/eclateur_facture_unique/scripts/python/eclateur_par.py"
printLog "Commande: ${CMD_ECLATE_FACTURE_UNIQUE}"
printLog "Path.par: ${DATA_INTERFACES_PARS}/"
if [[ -e ${CMD_ECLATE_FACTURE_UNIQUE} ]]; then
	python3 ${CMD_ECLATE_FACTURE_UNIQUE} ${DATA_INTERFACES_PARS}/
  RET=$?
  if [[ ${RET} -ne 0 ]]; then
     printLog "ERROR Erreur Code ${RET} : Eclatement FactureUnique  "
     exit   2
  fi
else
  printLog "Fonctionnalite desactivee "
fi
printLog "Eclatement FactureUnique.par  : OK"
printLog "============================================================="

#########################################################################################################
# Creation de repertoire & Purge .PAR persistant
# execution 16h et 19h

REP_WEBDAV_PARS_JOUR=${DATA_WEBDAV_TECH_HOME}/${DATE_PLAN_BATCH}/pars
LISTE_PARS_EXIST=`find ${REP_WEBDAV_PARS_JOUR} -maxdepth 1 -type f -name "*.par.txt" -o -name ".original.txt"`
NBR_PARS_EXIST=`echo "$LISTE_PARS_EXIST" | wc -l`
compteur_pars_exist=0;
if [[ -d  ${REP_WEBDAV_PARS_JOUR} ]];then
	printLog " ===>  Repertoire pars existe dans ${REP_WEBDAV_PARS_JOUR} "
	printLog " ===>  Supression des .pars ... "

	for pars_exist in $LISTE_PARS_EXIST
	do
		rm -f "$pars_exist" 2>> $SCRIPTLOG
		if [[ $? -ne 0 ]]; then
			printLog " ======>  Erreur lors de la suppression de .pars : ${pars_exist} "
			exit	2
		else
			compteur_pars_exist=$((${compteur_pars_exist} + 1))
		fi
	done
	printLog " ===>  Supression des .pars : OK "
	printLog " ===>  PURGE Nombre fichier PAR trouve/traite : [$NBR_PARS_EXIST]/[$compteur_pars_exist]"
else
	printLog " ===>  Creation repertoire ${DATA_WEBDAV_TECH_HOME}/${DATE_PLAN_BATCH}/pars ... "
	mkdir -pm 777 ${REP_WEBDAV_PARS_JOUR}
	if [[ $? -ne 0 ]]; then
	printLog " ======>  Erreur lors de la Creation du repertoire : ${REP_WEBDAV_PARS_JOUR}  "
	printLog " ======>  Echec Copie des fichiers de parametrages des traitements batchs Cleva vers Webdav  "
	exit   2
	fi
	printLog " ===>  Creation repertoire ${DATA_WEBDAV_TECH_HOME}/${DATE_PLAN_BATCH}/pars : OK "
fi

#########################################################################################################
## Transfert des fichiers PAR sous webdav
## .par and .original files (to support eclateur_factureunique .original)
printLog " ===>  Transfert des fichiers pars sur  webdav ..."
printLog " Recherche ${DATA_INTERFACES_PARS} - {*.${SUFFIXE_FICHIER_PAR},*.${SUFFIXE_FICHIER_PAR_ORIGINAL}}"

LISTE_PARS_COPY=`cd ${DATA_INTERFACES_PARS};ls {*.${SUFFIXE_FICHIER_PAR},*.${SUFFIXE_FICHIER_PAR_ORIGINAL}} 2>/dev/null`
NBR_PARS_COPY=`echo $LISTE_PARS_COPY | wc -w`
compteur_pars_copy=0;
for pars_copy in $LISTE_PARS_COPY
do
	printLog "    ----> Transfert du fichier  ${DATA_INTERFACES_PARS}/${pars_copy} ... "
	# transformation des fichiers au format Windows ( copie vers Webdav)
	#sed  -i  's/\n/\r\n/g'   ${DATA_INTERFACES_MIGR_TRV}/${REP_LOG_TRT}/${LOG_FILE_NAME}
	cp  -f   ${DATA_INTERFACES_PARS}/${pars_copy}   ${REP_WEBDAV_PARS_JOUR}/${pars_copy}.txt
	if [[ $? -ne 0 ]]; then
		printLog " ======>  Erreur lors de la copie du Fichier : ${DATA_INTERFACES_PARS}/${pars_copy}  "
		printLog " ======>  Echec Copie des fichiers de parametrages des traitements batchs Cleva vers Webdav  "
		exit   2
	else
		compteur_pars_copy=$((${compteur_pars_copy} + 1))
	fi
done
chmod -fR 777  ${REP_WEBDAV_PARS_JOUR}  ||  :
printLog " ===>  Transfert des fichiers pars sur  webdav : OK "
printLog " ===>  Copy Nombre PARS trouve/traite : [$NBR_PARS_COPY]/[$compteur_pars_copy]"

#################################################################
# FIN
#################################################################
printLog "============================================================="
printLog " Copie des fichiers de parametrages des traitements batchs Cleva vers Webdav : OK "
printLog "============================================================="
printLog "FIN DU SCRIPT $SCRIPTNAME"

exit 0
#################################################################
# FIN SCRIPT
#################################################################