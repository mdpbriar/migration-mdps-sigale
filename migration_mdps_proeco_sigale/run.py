import logging
from datetime import date
from typing import Literal

import pandas as pd
from sqlalchemy import Engine
from migration_mdps_proeco_sigale import config as default_config
from migration_mdps_proeco_sigale.date_utils import DateUtils
from migration_mdps_proeco_sigale.db.requetes_sql import SQL_MDPS_PROECO, SQL_CONTRATS_EN_COURS_PROECO, SQL_MDPS_SIGALE
from migration_mdps_proeco_sigale.migrations import migrate_personnes, migrate_emails, migrate_phones, migrate_adresses, \
    migrate_users
from migration_mdps_proeco_sigale.tools import clean_numero_registre_national


def run_migrations(
        proeco_engine: Engine,
        sigale_engine: Engine,
        logger: logging.Logger,
        action_when_duplicates: Literal['drop', 'stop'] = 'stop',
        export: bool = False,
        contrat_en_cours_uniquement:bool = True,
        create_users: bool = False,
        update: bool = False,
        dry_run: bool = False,
        config = default_config
):
    """

    :param proeco_engine: connexion à Proeco
    :param sigale_engine: connexion à Sigale
    :param logger: logger utilisé pour les logs
    :param action_when_duplicates: action en cas de doublons, par défaut arrête la migration, 'drop' permet de les supprimer
    :param export: permet d'exporter les données en csv
    :param contrat_en_cours_uniquement: Si on traite uniquement les contrats en cours
    :param create_users: Créé les utilisateurs manquants après création des personnes
    :param update: si on update les mdps existants
    :param dry_run: Permet de tester, on insère pas les données en DB
    :param config: permet d'importer un autre fichier de configuration
    :return:
    """


    # On récupère le résulat de la SQL dans un dataframe
    enseignants_proeco = pd.read_sql_query(SQL_MDPS_PROECO, proeco_engine, dtype={'registre_national_numero': str})

    # On nettoie le numéro de registre national des éventuels espaces inutiles
    enseignants_proeco['registre_national_numero'] = enseignants_proeco['registre_national_numero'].apply(clean_numero_registre_national)

    # On ne garde que les lignes où le numéro de registre national est 11
    enseignants_proeco = enseignants_proeco[enseignants_proeco['registre_national_numero'].str.len() == 11]

    # Si option pour ne prendre que les contrats en cours
    if contrat_en_cours_uniquement:
        # On calcule la date proeco d'aujourd'hui
        today_proeco = DateUtils.convert_date_to_dateproeco(date.today())
        # On récupère les contrats en cours
        contrats_en_cours = pd.read_sql_query(SQL_CONTRATS_EN_COURS_PROECO, proeco_engine,
                                              params={'date_proeco': today_proeco})
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
    enseignants_proeco['date_naissance'] = enseignants_proeco['date_naissance'].apply(
        lambda x: DateUtils.convert_dateproeco_to_date(x))

    # On migre les personnes, gestion de l'ajout/mise à jour dans personnes.personnes
    attributs_personnes = ['matric', 'nom', 'prenom', 'sexe', 'nation', 'paynaiss', 'lieunaiss', 'etatcivil',
                           'registre_national_numero', 'date_naissance', 'matriche', 'reserved']
    migrate_personnes(enseignants_proeco[attributs_personnes], sigale_engine, logger, export, dry_run, update, config=config)


    ## AJOUT DES ID PERSONNES
    # On récupère les personnes existantes dans Sigale
    personnes = pd.read_sql_query(SQL_MDPS_SIGALE, sigale_engine)
    # On ajoute les ids dans les emails
    enseignants_proeco = enseignants_proeco.merge(personnes, on='registre_national_numero', how='inner', validate='m:1')

    # Si création des utilisateurs
    if create_users:
        migrate_users(enseignants_proeco, sigale_engine, logger, export, dry_run, update, config=config)

    # On migre les emails, gestion de l'ajout/mise à jour dans personnes.personne_emails
    attributs_emails = ['personne_id', 'email', 'email2']
    migrate_emails(enseignants_proeco[attributs_emails], sigale_engine, logger, export, dry_run, update, config=config)

    # On migre les téléphones, gestion de l'ajout/mise à jour dans personnes.personne_telephones
    attributs_phones = ['personne_id', 'teldomi', 'telresi', 'gsm', 'telbureau']
    migrate_phones(enseignants_proeco[attributs_phones], sigale_engine, logger, export, dry_run, update, config=config)

    attributs_adresses = ['personne_id', 'ruedomi', 'paysdomi', 'cpostdomi', 'commdomi', 'locadomi', 'zonedomi',
                          'rueresi', 'paysresi', 'cpostresi', 'commresi', 'locaresi', 'zoneresi']
    migrate_adresses(enseignants_proeco[attributs_adresses], sigale_engine, logger, export, dry_run, update, config=config)
    return None
