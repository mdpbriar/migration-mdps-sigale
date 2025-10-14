from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Literal
import argparse

from migration_mdps_proeco_sigale import config
import logging
from migration_mdps_proeco_sigale.db.proeco_connector import ProecoConnector
import sys

from migration_mdps_proeco_sigale.db.sigale_connector import SigaleConnector
from migration_mdps_proeco_sigale.run import run_migrations


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

    contrat_en_cours_uniquement = not args.historique
    action_when_duplicates: Literal['drop', 'stop'] = 'drop' if args.drop_duplicates else 'stop'
    export = args.export
    dry_run = args.dry_run
    update = args.update

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

    # On lance les migrations avec les options passées dans main
    run_migrations(
        proeco_engine=proeco_engine,
        sigale_engine=sigale_engine,
        logger=logger,
        contrat_en_cours_uniquement=contrat_en_cours_uniquement,
        action_when_duplicates=action_when_duplicates,
        dry_run=dry_run,
        update=update,
        export=export,

    )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        test()
    else:
        main()
