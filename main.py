import logging
from datetime import date
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Literal
import argparse

import config
from src.date_utils import DateUtils
from src.db.requetes_sql import SQL_MDPS_PROECO, SQL_CONTRATS_EN_COURS_PROECO, SQL_MDPS_SIGALE
import logging
from src.db.proeco_connector import ProecoConnector
import pandas as pd
import sys

from src.db.sigale_connector import SigaleConnector
from src.migrations import migrate_emails, migrate_personnes, migrate_phones, migrate_adresses


def test():

    logger = create_logger(loglevel='debug', write_to_file=False, write_to_stdout=True)
    # On initie le connecteur Proeco
    proeco_connector = ProecoConnector('PROF.FDB')
    # test de la connexion
    proeco_connector.test_connection(logger=logger)

    ## Même chose pour Sigale
    sigale_connector = SigaleConnector()
    sigale_connector.test_connection(logger=logger)


def create_logger(loglevel = 'INFO', write_to_file: bool = True, write_to_stdout: bool = True) -> logging.Logger:

    logger = logging.getLogger(name='logger_migration_mdps_sigale')
    logger.setLevel(loglevel.upper())
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

    stdout_handler = logging.StreamHandler(sys.stdout)
    # stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)

    log_path = Path(config.LOGS_FILE)
    # On créé le dossier existant
    log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = RotatingFileHandler(log_path, mode='a', maxBytes=5*1024*1024)
    # file_handler.setLevel(loglevel)
    file_handler.setFormatter(formatter)

    if write_to_file:
        logger.addHandler(file_handler)
    if write_to_stdout:
        logger.addHandler(stdout_handler)

    return logger


def main():
    # On parse les options:
    parser = argparse.ArgumentParser(description='Options du script')
    parser.add_argument('--historique', action='store_true', dest='historique', default=False,
                        help='Importe également les mdps historiques (contrats terminés)')
    parser.add_argument('--drop-duplicates', action='store_true', dest='drop_duplicates', default=False,
                        help='Supprime les registres nationaux en doublons (stop la synchro sinon)')
    parser.add_argument('--export', action='store_true', dest='export', default=False,
                        help='Exporte les données en csv')
    parser.add_argument('--dry-run', action='store_true', dest='dry_run', default=False,
                        help="Tester la synchro, n'importe pas en db")
    parser.add_argument('--no-update', action='store_false', dest='update', default=True,
                        help="N'ajoute que les nouveaux enseignants, pas de mise à jour")
    parser.add_argument('--no-stdout', action='store_false', dest='stdout', default=True, help="Pas d'impression des logs dans stdout")
    parser.add_argument('--no-logfile', action='store_false', dest='logfile', default=True,
                        help="Pas d'impression des logs dans le fichier")
    parser.add_argument('-log',
                        '--loglevel',
                        default='info',
                        help='Provide logging level. Example --loglevel debug, default=info')

    args = parser.parse_args()

    logger = create_logger(loglevel=args.loglevel, write_to_file=args.logfile, write_to_stdout=args.stdout)

    # On initie le connecteur Proeco
    proeco_connector = ProecoConnector('PROF.FDB')
    # test de la connexion
    proeco_connector.test_connection(logger=logger)
    # Création du connecteur
    proeco_engine = proeco_connector.create_engine()


    ## Même chose pour Sigale
    sigale_connector = SigaleConnector()
    sigale_connector.test_connection(logger=logger)
    sigale_engine = sigale_connector.create_engine()



    contrat_en_cours_uniquement = not args.historique
    action_when_duplicates: Literal['drop','stop'] = 'drop' if args.drop_duplicates else 'stop'
    export = args.export
    dry_run = args.dry_run
    update = args.update



    # On récupère le résulat de la SQL dans un dataframe
    enseignants_proeco = pd.read_sql_query(SQL_MDPS_PROECO, proeco_engine)

    # Si option pour ne prendre que les contrats en cours
    if contrat_en_cours_uniquement:
        # On calcule la date proeco d'aujourd'hui
        today_proeco = DateUtils.convert_date_to_dateproeco(date.today())
        # On récupère les contrats en cours
        contrats_en_cours = pd.read_sql_query(SQL_CONTRATS_EN_COURS_PROECO, proeco_engine, params={'date_proeco': today_proeco})
        contrats_en_cours.drop_duplicates(subset='matric', inplace=True)
        # Inner join pour ne garder que les enseignants avec contrat en cours
        enseignants_proeco = enseignants_proeco.merge(contrats_en_cours, on='matric', how='inner', validate='1:1')

    # On vérifie si il y a des doublons sur le numéro national
    duplicated = enseignants_proeco[enseignants_proeco.duplicated(subset=['registre_national_numero'])]
    if len(duplicated) > 0:
        logger.error(f"{len(duplicated)} enseignants ont des numéros de registre nationaux dupliqués : {duplicated}")
        # Si action définie à stop, on arrête la migration
        if action_when_duplicates == 'stop':
            return None
        # Sinon, on supprime les doublons avec les matricules les plus anciens
        if action_when_duplicates == 'drop':
            enseignants_proeco.drop_duplicates(subset=['registre_national_numero'], inplace=True, keep='last')

    # On transforme les dates de naissances proeco en dates normales
    enseignants_proeco['date_naissance'] = enseignants_proeco['date_naissance'].apply(lambda x: DateUtils.convert_dateproeco_to_date(x))

    # On migre les personnes, gestion de l'ajout/mise à jour dans personnes.personnes
    attributs_personnes = ['matric','nom','prenom','sexe','nation','paynaiss','lieunaiss','etatcivil','registre_national_numero','date_naissance']
    migrate_personnes(enseignants_proeco[attributs_personnes], sigale_engine, logger, export, dry_run, update)

    ## AJOUT DES ID PERSONNES
    # On récupère les personnes existantes dans Sigale
    personnes = pd.read_sql_query(SQL_MDPS_SIGALE, sigale_engine)
    # On ajoute les ids dans les emails
    enseignants_proeco = enseignants_proeco.merge(personnes, on='registre_national_numero', how='inner', validate='m:1')

    # On migre les emails, gestion de l'ajout/mise à jour dans personnes.personne_emails
    attributs_emails = ['personne_id', 'email', 'email2']
    migrate_emails(enseignants_proeco[attributs_emails], sigale_engine, logger, export, dry_run, update)

    # On migre les téléphones, gestion de l'ajout/mise à jour dans personnes.personne_telephones
    attributs_phones = ['personne_id', 'teldomi', 'telresi', 'gsm', 'telbureau']
    migrate_phones(enseignants_proeco[attributs_phones], sigale_engine, logger, export, dry_run, update)

    attributs_adresses = ['personne_id','ruedomi','paysdomi','cpostdomi','commdomi','locadomi','zonedomi','rueresi','paysresi','cpostresi','commresi','locaresi','zoneresi']
    migrate_adresses(enseignants_proeco[attributs_adresses], sigale_engine, logger, export, dry_run, update)
    return None


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        test()
    else:
        main()
