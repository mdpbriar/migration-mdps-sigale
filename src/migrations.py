import os.path
from logging import Logger

import pandas as pd
from unidecode import unidecode

import config
from src.db.requetes_sql import SQL_PARAMETER_SIGALE, SQL_MDPS_SIGALE, SQL_EMAILS_SIGALE, SQL_PHONES_SIGALE
from src.db.sql_write_methods import WriteMethods


def migrate_personnes(enseignants_proeco: pd.DataFrame, sigale_engine, logger: Logger, export:bool = False, dry_run:bool = False, update:bool = True):

    #### AJOUT DES SEXE_ID ####
    # On récupère les sexes de Sigale dans une table ['sexe_id', 'sexe']
    sexes_sigale = pd.read_sql_query(SQL_PARAMETER_SIGALE, sigale_engine,
                                     params={'type_parameter': 'sexes_sigale'}).rename(
        columns={'id': 'sexe_id', 'code': 'sexe'})
    # On récupère la première lettre du code sexe ( 'feminin', 'masculin' ) qu'on met en majuscule pour recoupement avec Proeco
    sexes_sigale['sexe'] = sexes_sigale['sexe'].apply(lambda x: x[0].upper())
    # On merge
    enseignants_proeco = enseignants_proeco.merge(sexes_sigale, on='sexe', how='inner', validate='m:1').drop(
        columns='sexe')

    ### AJOUT DES PAYS_ID_NATIONALITE
    # On reformate les codes pays dans notre liste d'enseignants, majuscules, suppression d'accents
    enseignants_proeco['nation'] = enseignants_proeco['nation'].apply(str.upper).apply(unidecode).apply(str.strip)
    # On récupère les nationalités de Sigale dans une table ['pays_id_nationalite', 'code']
    pays_sigale = pd.read_sql_query("select id as pays_id_nationalite, code as code_pays from core.countries",
                                    sigale_engine)
    # On merge
    enseignants_proeco = enseignants_proeco.merge(pays_sigale, left_on='nation', right_on='code_pays', how='left',
                                                  validate='m:1').drop(columns=['nation']).drop(columns='code_pays')

    ### AJOUT DES PAYS_ID_NAISSANCE_PAYS
    # On reformate les codes pays dans notre liste d'enseignants, majuscules, suppression d'accents
    enseignants_proeco['paynaiss'] = enseignants_proeco['paynaiss'].apply(str.upper).apply(unidecode).apply(str.strip)
    # On renome l'id pour correspondre à la clé étrangère dans personnes.personnes
    pays_sigale.rename(columns={'pays_id_nationalite': 'pays_id_naissance_pays', 'code_pays': 'paynaiss'}, inplace=True)
    enseignants_proeco = enseignants_proeco.merge(pays_sigale, on='paynaiss', how='left', validate='m:1')

    ### AJOUT DES CITY_ID_NAISSANCE
    # On récupère les villes de Sigale en ajoutant le code pays BE
    villes_sigale = pd.read_sql_query(
        "select id as city_id_naissance, name as city_name from core.cities order by postal_code asc", sigale_engine)
    villes_sigale['code_pays'] = 'BE'
    # On reformate les noms de villes pour s'assurer du match
    villes_sigale['city_name'] = villes_sigale['city_name'].apply(str.upper).apply(unidecode).apply(str.strip)
    enseignants_proeco['lieunaiss'] = enseignants_proeco['lieunaiss'].apply(str.upper).apply(unidecode).apply(str.strip)

    # On supprime les doublons, (plusieurs fois la même ville avec codes postaux différents)
    # keep first, pour garder le code postal le plus petit (on conserve 4000 au lieu de 4020)
    villes_sigale.drop_duplicates(subset=['city_name', 'code_pays'], inplace=True, keep='first')
    enseignants_proeco = (
        enseignants_proeco.merge(villes_sigale, left_on=['lieunaiss', 'paynaiss'], right_on=['city_name', 'code_pays'],
                                 how='left', validate='m:1')
        .drop(columns=['city_name', 'code_pays']))

    # On ajoute la ville dans le champ lieu_naissance_hors_belgique quand le pays de naissance n'est pas BE
    enseignants_proeco['lieu_naissance_hors_belgique'] = enseignants_proeco.apply(
        lambda row: row['lieunaiss'] if row['paynaiss'] != 'BE' else None, axis=1)

    ### AJOUT DES ETAT_CIVIL_ID
    # On récupère les états civils de Sigale
    etats_civils_sigale = pd.read_sql_query(SQL_PARAMETER_SIGALE, sigale_engine,
                                            params={'type_parameter': 'etats_civils'})
    # On renomme pour correspondre aux champs existants dans Sigale et Proeco
    etats_civils_sigale.rename(columns={'id': 'etat_civil_id', 'code': 'etatcivil'}, inplace=True)
    # On remplace les états civils de Proeco avec ceux de Sigale pour préparer le merge
    enseignants_proeco['etatcivil'] = enseignants_proeco['etatcivil'].apply(
        lambda code: config.MAPPING_ETATS_CIVILS.get(code))
    # On merge pour ajouter les id d'états civils à nos mdps
    enseignants_proeco = enseignants_proeco.merge(etats_civils_sigale, on='etatcivil', how='left', validate='m:1').drop(
        columns='etatcivil')

    ### NETTOYAGE
    # # On supprime les colonnes devenues inutiles
    enseignants_proeco.drop(columns=['paynaiss', 'lieunaiss'], inplace=True)
    # On renomme les champs selon le mapping pour correspondre à Sigale
    enseignants_proeco.rename(columns=config.MAPPING_PROECO_SIGALE, inplace=True)

    ### RECOUPEMENT AVEC LES DONNEES DE SIGALE
    # on récupère les personnes de Sigale
    personnes_sigales = pd.read_sql_query(SQL_MDPS_SIGALE, sigale_engine)
    # On merge pour voir quels enseignants sont déjà dans Sigale
    enseignants_proeco = enseignants_proeco.merge(personnes_sigales, on='registre_national_numero', how='left',
                                                  indicator=True, validate='1:1')
    # Les nouveaux enseignants sont ceux n'existant que dans Proeco
    nouveaux_enseignants = enseignants_proeco[enseignants_proeco['_merge'] == 'left_only'].drop(columns=['_merge', 'personne_id'])
    # Les enseignants à mettre à jour sont ceux existant déjà dans Sigale
    enseignants_existants = enseignants_proeco[enseignants_proeco['_merge'] == 'both'].drop(columns=['_merge'])

    # On exporte si option
    if export:
        nouveaux_enseignants.to_csv(os.path.join(config.EXPORT_PATH,'mdps_nouveaux.csv'), index=False)
        enseignants_existants.to_csv(os.path.join(config.EXPORT_PATH,'mdps_existants.csv'), index=False)

    # On ajoute les métadonnées
    for key, value in config.SIGALE_METADATA_FIELDS.items():
        nouveaux_enseignants[key] = value
        enseignants_existants[key] = value

    # On ajoute les champs par défaut de Proeco
    for key, value in config.SIGALE_PERSONNES_DEFAULT_FIELDS.items():
        nouveaux_enseignants[key] = value

    # Si dry_run, on s'arrête avant les modifications en DB
    if dry_run:
        logger.log(
            f"Dry run, pas de modification en DB, {len(nouveaux_enseignants)} mdps à insérer, {len(enseignants_existants)} mdps à mettre à jour")
        return None

    # On insère les nouveaux enseignants dans Sigale
    nouveaux_enseignants.to_sql('personnes', con=sigale_engine, schema='personnes', index=False, if_exists='append')
    logger.log(f"{len(nouveaux_enseignants)} nouveaux mdps introduits dans Sigale")

    # Si no-update, on s'arrête
    if not update:
        return None


    # On mets à jour les champs des enseignants existants basé sur la config
    enseignants_existants.rename(columns={'personne_id': 'id'}).to_sql('personnes', con=sigale_engine, schema='personnes', index=False, if_exists='append',
                                 method=WriteMethods(index_columns=['id'], update_columns=config.SIGALE_UPDATE_FIELDS).update_on_conflict)
    logger.log(f"{len(enseignants_existants)} mdps mis à jour dans Sigale")

    return None


def migrate_emails(personne_emails: pd.DataFrame, sigale_engine, logger: Logger, export:bool = False, dry_run:bool = False, update:bool = True):

    # Avec melt, on répartit nos colonnes email et email2 dans des nouvelles lignes
    # On passe d'une structure registre_national, email, email2
    # à registre_national, champ_proeco(email ou email2), valeur
    personne_emails = personne_emails.melt(id_vars=['personne_id'], value_vars=['email', 'email2'], var_name='champ_proeco', value_name='valeur')
    # en partant de la config, on ajoute le type d'email, None si le champ n'est pas dans la config
    personne_emails['code_domaine'] = personne_emails.apply(lambda row: config.EMAILS_FIELDS.get(row['champ_proeco']).get('code_domaine') if row['champ_proeco'] in config.EMAILS_FIELDS else None, axis=1)
    # On supprime là où le champ n'est pas déclaré dans la config
    personne_emails.dropna(subset=['code_domaine'], inplace=True)
    personne_emails['est_individuel'] = personne_emails.apply(lambda row: config.EMAILS_FIELDS.get(row['champ_proeco']).get('est_individuel'), axis=1)
    personne_emails['est_principal'] = personne_emails.apply(lambda row: config.EMAILS_FIELDS.get(row['champ_proeco']).get('est_principal'), axis=1)

    ## AJOUT DES ID DOMAINES
    # On récupère les domaines emails de Sigale
    emails_domaines = pd.read_sql_query(SQL_PARAMETER_SIGALE, sigale_engine, params={'type_parameter': 'email_domaines'})
    emails_domaines.rename(columns={'code': 'code_domaine', 'id': 'email_domaine_id'}, inplace=True)
    # On merge avec les nouveaux emails
    personne_emails = personne_emails.merge(emails_domaines, on='code_domaine', how='inner', validate='m:1')

    ## NETTOYAGE
    personne_emails.drop(columns=['code_domaine', 'champ_proeco'], inplace=True)

    ### RECOUPEMENT AVEC DONNEES SIGALE
    emails_sigale = pd.read_sql_query(SQL_EMAILS_SIGALE, sigale_engine)
    # Si config pour ne remplacer que les created_by migration
    if config.UPDATE_ONLY_CREATED_BY_MIGRATION:
        emails_sigale.drop(
            emails_sigale[emails_sigale['created_by'] != config.SIGALE_METADATA_FIELDS.get('created_by', 1)].index,
            inplace=True)

    # Suppression de la colonne created by, utilisée uniquement pour filtrer
    emails_sigale.drop(columns=['created_by'], inplace=True)
    # On merge pour voir quels enseignants sont déjà dans Sigale
    personne_emails = personne_emails.merge(emails_sigale, on=['personne_id', 'email_domaine_id'], how='left',
                                                  indicator=True, validate='1:1', suffixes=['_new', '_old'])
    # Les nouveaux emails sont ceux n'existant que dans Proeco
    nouveaux_emails = personne_emails[personne_emails['_merge'] == 'left_only'].drop(columns=['_merge', 'email_id'])
    # Les emails à mettre à jour sont ceux existant déjà dans Sigale
    emails_existants = personne_emails[personne_emails['_merge'] == 'both'].drop(columns=['_merge'])

    nouveaux_emails.drop(columns=['valeur_old'], inplace=True)
    nouveaux_emails.rename(columns={'valeur_new': 'valeur'}, inplace=True)

    # On exporte si option
    if export:
        nouveaux_emails.to_csv(os.path.join(config.EXPORT_PATH, 'emails_nouveaux.csv'), index=False)
        emails_existants.to_csv(os.path.join(config.EXPORT_PATH, 'emails_existants.csv'), index=False)

    # On ajoute les métadonnées
    for key, value in config.SIGALE_METADATA_FIELDS.items():
        nouveaux_emails[key] = value
        emails_existants[key] = value

    # Si dry_run, on s'arrête avant les modifications en DB
    if dry_run:
        logger.log(
            f"Dry run, pas de modification en DB, {len(nouveaux_emails)} emails à insérer, {len(emails_existants)} emails à mettre à jour")
        return None

    # On insère les nouveaux enseignants dans Sigale
    nouveaux_emails.to_sql('personne_emails', con=sigale_engine, schema='personnes', index=False, if_exists='append')
    logger.log(f"{len(nouveaux_emails)} nouveaux emails introduits dans Sigale")

    # Si no-update, on s'arrête
    if not update:
        return None

    emails_existants.drop(columns=['valeur_old'], inplace=True)
    emails_existants.rename(columns={'valeur_new': 'valeur'}, inplace=True)

    # On mets à jour les champs des enseignants existants basé sur la config
    emails_existants.rename(columns={'email_id': 'id'}).to_sql('personne_emails', con=sigale_engine,
                                                                       schema='personnes', index=False,
                                                                       if_exists='append',
                                                                       method=WriteMethods(index_columns=['id'],
                                                                                           update_columns=config.SIGALE_EMAIL_UPDATE_FIELDS).update_on_conflict)
    logger.log(f"{len(emails_existants)} emails mis à jour dans Sigale")

    return None


def migrate_phones(phones: pd.DataFrame, sigale_engine, logger: Logger, export:bool = False, dry_run:bool = False, update:bool = True):

    # On ne conserve que les champs définis dans la config
    champs_utilises = [field for field in config.PHONE_FIELDS]
    champs_utilises.append('personne_id')
    phones = phones[champs_utilises]
    # Avec melt, on répartit nos colonnes telephones dans des nouvelles lignes
    # On passe d'une structure personne_id, teldomi, telresi, gsm, telbureau
    # à personne_id, champ_proeco(teldomi, telresi, gsm ou telbureau), numero
    phones = phones.melt(id_vars=['personne_id'], value_vars=champs_utilises,
                                           var_name='champ_proeco', value_name='numero')

    # On ajoute les champs tels que définis dans la config
    phones['code_domaine'] = phones.apply(
        lambda row: config.PHONE_FIELDS.get(row['champ_proeco']).get('code_domaine'), axis=1)
    phones['code_type'] = phones.apply(
        lambda row: config.PHONE_FIELDS.get(row['champ_proeco']).get('code_type'), axis=1)
    phones['est_individuel'] = phones.apply(
        lambda row: config.PHONE_FIELDS.get(row['champ_proeco']).get('est_individuel'), axis=1)
    phones['est_principal'] = phones.apply(
        lambda row: config.PHONE_FIELDS.get(row['champ_proeco']).get('est_principal'), axis=1)

    ## AJOUT DES ID DOMAINES
    # On récupère les domaines phones de Sigale
    phones_domaines = pd.read_sql_query(SQL_PARAMETER_SIGALE, sigale_engine,
                                        params={'type_parameter': 'telephone_domaines'})
    phones_domaines.rename(columns={'code': 'code_domaine', 'id': 'telephone_domaine_id'}, inplace=True)
    # On merge avec les nouveaux emails
    phones = phones.merge(phones_domaines, on='code_domaine', how='inner', validate='m:1')

    ## AJOUT DES ID TYPES
    # On récupère les domaines phones de Sigale
    phones_types = pd.read_sql_query(SQL_PARAMETER_SIGALE, sigale_engine,
                                        params={'type_parameter': 'telephone_types'})
    phones_types.rename(columns={'code': 'code_type', 'id': 'telephone_type_id'}, inplace=True)
    # On merge avec les nouveaux emails
    phones = phones.merge(phones_types, on='code_type', how='inner', validate='m:1')

    ## NETTOYAGE
    phones.drop(columns=['code_domaine', 'code_type', 'champ_proeco'], inplace=True)
    phones['numero'] = phones['numero'].apply(lambda x: x.replace(' ', '').replace('.', '').replace('/', ''))
    # On supprime là où le numéro de téléphone est vide
    phones.drop(phones[phones['numero'] == ''].index, inplace=True)
    phones.dropna(subset=['numero'], inplace=True)

    ### RECOUPEMENT AVEC DONNEES SIGALE

    phones_sigale = pd.read_sql_query(SQL_PHONES_SIGALE, sigale_engine)

    # Si config pour ne remplacer que les created_by migration
    if config.UPDATE_ONLY_CREATED_BY_MIGRATION:
        # On écarte les lignes non créées par la migration
        phones_sigale.drop(phones_sigale[phones_sigale['created_by'] != config.SIGALE_METADATA_FIELDS.get('created_by', 1)].index, inplace=True)

    # Suppression de la colonne created by, utilisée uniquement pour filtrer
    phones_sigale.drop(columns=['created_by'], inplace=True)

    # On merge pour voir quels numéros sont déjà dans Sigale
    phones = phones.merge(phones_sigale, on=['personne_id', 'telephone_domaine_id', 'telephone_type_id'], how='left',
                                            indicator=True, validate='1:1', suffixes=['_new', '_old'])
    # Les nouveaux téléphones sont ceux n'existant que dans Proeco
    nouveaux_phones = phones[phones['_merge'] == 'left_only'].drop(columns=['_merge', 'telephone_id'])
    # Les emails à mettre à jour sont ceux existant déjà dans Sigale
    phones_existants = phones[phones['_merge'] == 'both'].drop(columns=['_merge'])

    nouveaux_phones.drop(columns=['numero_old'], inplace=True)
    nouveaux_phones.rename(columns={'numero_new': 'numero'}, inplace=True)

    # On exporte si option
    if export:
        nouveaux_phones.to_csv(os.path.join(config.EXPORT_PATH, 'telephones_nouveaux.csv'), index=False)
        phones_existants.to_csv(os.path.join(config.EXPORT_PATH, 'telephones_existants.csv'), index=False)

    # On ajoute les métadonnées
    for key, value in config.SIGALE_METADATA_FIELDS.items():
        nouveaux_phones[key] = value
        phones_existants[key] = value

    # Si dry_run, on s'arrête avant les modifications en DB
    if dry_run:
        logger.log(
            f"Dry run, pas de modification en DB, {len(nouveaux_phones)} téléphones à insérer, {len(phones_existants)} emails à mettre à jour")
        return None

    # On insère les nouveaux téléphones dans Sigale
    nouveaux_phones.to_sql('personne_telephones', con=sigale_engine, schema='personnes', index=False,
                           if_exists='append')
    logger.log(f"{len(nouveaux_phones)} nouveaux téléphones introduits dans Sigale")

    # Si no-update, on s'arrête
    if not update:
        return None

    phones_existants.drop(columns=['numero_old'], inplace=True)
    phones_existants.rename(columns={'numero_new': 'numero'}, inplace=True)

    # On mets à jour les champs des enseignants existants basé sur la config
    phones_existants.rename(columns={'telephone_id': 'id'}).to_sql('personne_telephones', con=sigale_engine,
                                                               schema='personnes', index=False,
                                                               if_exists='append',
                                                               method=WriteMethods(
                                                                   index_columns=['id'],
                                                                   update_columns=config.SIGALE_PHONE_UPDATE_FIELDS).update_on_conflict)
    logger.log(f"{len(phones_existants)} téléphones mis à jour dans Sigale")

    return None



