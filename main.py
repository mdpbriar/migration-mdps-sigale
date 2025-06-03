from datetime import date
from typing import Literal
import argparse
from unidecode import unidecode

from src.date_utils import DateUtils
from src.db.requetes_sql import SQL_MDPS_PROECO, SQL_CONTRATS_EN_COURS_PROECO, SQL_PARAMETER_SIGALE, SQL_MDPS_SIGALE
from src.db.sql_write_methods import WriteMethods
from src.logger import Logger
from src.db.proeco_connector import ProecoConnector
import config
from sqlalchemy import text
import pandas as pd
import sys

from src.db.sigale_connector import SigaleConnector


def test():
    # On initie le connecteur Proeco
    proeco_connector = ProecoConnector('PROF.FDB')
    # test de la connexion
    proeco_connector.test_connection()

    ## Même chose pour Sigale
    sigale_connector = SigaleConnector()
    sigale_connector.test_connection()


def main():

    logger = Logger()

    # On initie le connecteur Proeco
    proeco_connector = ProecoConnector('PROF.FDB')
    # test de la connexion
    proeco_connector.test_connection()
    # Création du connecteur
    proeco_engine = proeco_connector.create_engine()


    ## Même chose pour Sigale
    sigale_connector = SigaleConnector()
    sigale_connector.test_connection()
    sigale_engine = sigale_connector.create_engine()

    # On parse les options:
    parser = argparse.ArgumentParser(description='Options du script')
    parser.add_argument('--historique', action='store_true', dest='historique', default=False, help='Importe également les mdps historiques (contrats terminés)')
    parser.add_argument('--drop-duplicates', action='store_true', dest='drop_duplicates', default=False, help='Supprime les registres nationaux en doublons (stop la synchro sinon)')
    parser.add_argument('--export', action='store_true', dest='export', default=False, help='Exporte les données en csv')
    parser.add_argument('--dry-run', action='store_true', dest='dry_run', default=False, help="Tester la synchro, n'importe pas en db")
    parser.add_argument('--no-update', action='store_false', dest='update', default=True, help="N'ajoute que les nouveaux enseignants, pas de mise à jour")
    args = parser.parse_args()

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
        logger.log(f"{len(duplicated)} enseignants ont des numéros de registre nationaux dupliqués : {duplicated}")
        # Si action définie à stop, on arrête la migration
        if action_when_duplicates == 'stop':
            return None
        # Sinon, on supprime les doublons avec les matricules les plus anciens
        if action_when_duplicates == 'drop':
            enseignants_proeco.drop_duplicates(subset=['registre_national_numero'], inplace=True, keep='last')

    # On transforme les dates de naissances proeco en dates normales
    enseignants_proeco['date_naissance'] = enseignants_proeco['date_naissance'].apply(lambda x: DateUtils.convert_dateproeco_to_date(x))

    #### AJOUT DES SEXE_ID ####
    # On récupère les sexes de Sigale dans une table ['sexe_id', 'sexe']
    sexes_sigale = pd.read_sql_query(SQL_PARAMETER_SIGALE, sigale_engine, params={'type_parameter': 'sexes_sigale'}).rename(columns={'id': 'sexe_id', 'code': 'sexe'})
    # On récupère la première lettre du code sexe ( 'feminin', 'masculin' ) qu'on met en majuscule pour recoupement avec Proeco
    sexes_sigale['sexe'] = sexes_sigale['sexe'].apply(lambda x: x[0].upper())
    # On merge
    enseignants_proeco = enseignants_proeco.merge(sexes_sigale, on='sexe', how='inner', validate='m:1').drop(columns='sexe')

    ### AJOUT DES PAYS_ID_NATIONALITE
    # On reformate les codes pays dans notre liste d'enseignants, majuscules, suppression d'accents
    enseignants_proeco['nation'] = enseignants_proeco['nation'].apply(str.upper).apply(unidecode).apply(str.strip)
    # On récupère les nationalités de Sigale dans une table ['pays_id_nationalite', 'code']
    pays_sigale = pd.read_sql_query("select id as pays_id_nationalite, code as code_pays from core.countries", sigale_engine)
    # On merge
    enseignants_proeco = enseignants_proeco.merge(pays_sigale, left_on='nation', right_on='code_pays', how='left', validate='m:1').drop(columns=['nation']).drop(columns='code_pays')

    ### AJOUT DES PAYS_ID_NAISSANCE_PAYS
    # On reformate les codes pays dans notre liste d'enseignants, majuscules, suppression d'accents
    enseignants_proeco['paynaiss'] = enseignants_proeco['paynaiss'].apply(str.upper).apply(unidecode).apply(str.strip)
    # On renome l'id pour correspondre à la clé étrangère dans personnes.personnes
    pays_sigale.rename(columns={'pays_id_nationalite': 'pays_id_naissance_pays', 'code_pays': 'paynaiss'}, inplace=True)
    enseignants_proeco = enseignants_proeco.merge(pays_sigale, on='paynaiss', how='left', validate='m:1')


    ### AJOUT DES CITY_ID_NAISSANCE
    # On récupère les villes de Sigale en ajoutant le code pays BE
    villes_sigale = pd.read_sql_query("select id as city_id_naissance, name as city_name from core.cities order by postal_code asc", sigale_engine)
    villes_sigale['code_pays'] = 'BE'
    # On reformate les noms de villes pour s'assurer du match
    villes_sigale['city_name'] = villes_sigale['city_name'].apply(str.upper).apply(unidecode).apply(str.strip)
    enseignants_proeco['lieunaiss'] = enseignants_proeco['lieunaiss'].apply(str.upper).apply(unidecode).apply(str.strip)

    # On supprime les doublons, (plusieurs fois la même ville avec codes postaux différents)
    # keep first, pour garder le code postal le plus petit (on conserve 4000 au lieu de 4020)
    villes_sigale.drop_duplicates(subset=['city_name', 'code_pays'], inplace=True, keep='first')
    enseignants_proeco = (enseignants_proeco.merge(villes_sigale, left_on=['lieunaiss', 'paynaiss'], right_on=['city_name', 'code_pays'], how='left', validate='m:1')
                          .drop(columns=['city_name', 'code_pays']))

    # On ajoute la ville dans le champ lieu_naissance_hors_belgique quand le pays de naissance n'est pas BE
    enseignants_proeco['lieu_naissance_hors_belgique'] = enseignants_proeco.apply(lambda row: row['lieunaiss'] if row['paynaiss'] != 'BE' else None, axis=1)

    ### AJOUT DES ETAT_CIVIL_ID
    # On récupère les états civils de Sigale
    etats_civils_sigale = pd.read_sql_query(SQL_PARAMETER_SIGALE, sigale_engine, params={'type_parameter': 'etats_civils'})
    # On renomme pour correspondre aux champs existants dans Sigale et Proeco
    etats_civils_sigale.rename(columns={'id': 'etat_civil_id', 'code': 'etatcivil'}, inplace=True)
    # On remplace les états civils de Proeco avec ceux de Sigale pour préparer le merge
    enseignants_proeco['etatcivil'] = enseignants_proeco['etatcivil'].apply(lambda code: config.MAPPING_ETATS_CIVILS.get(code))
    # On merge pour ajouter les id d'états civils à nos mdps
    enseignants_proeco = enseignants_proeco.merge(etats_civils_sigale, on='etatcivil', how='left', validate='m:1').drop(columns='etatcivil')

    ### NETTOYAGE
    # On supprime les colonnes devenues inutiles
    enseignants_proeco.drop(columns=['paynaiss', 'lieunaiss'], inplace=True)
    # On renomme les colonnes pour correspondre avec Sigale
    # enseignants_proeco.rename(columns={
    #     'matric': 'matric_mdp'
    # }, inplace=True)
    # On renomme les champs selon le mapping pour correspondre à Sigale
    enseignants_proeco.rename(columns=config.MAPPING_PROECO_SIGALE, inplace=True)

    ### RECOUPEMENT AVEC LES DONNEES DE SIGALE
    # on récupère les personnes de Sigale
    personnes_sigales = pd.read_sql_query(SQL_MDPS_SIGALE, sigale_engine)
    # On merge pour voir quels enseignants sont déjà dans Sigale
    enseignants_proeco = enseignants_proeco.merge(personnes_sigales, on='registre_national_numero', how='left', indicator=True, validate='1:1')
    # Les nouveaux enseignants sont ceux n'existant que dans Proeco
    nouveaux_enseignants = enseignants_proeco[enseignants_proeco['_merge'] == 'left_only'].drop(columns=['_merge'])
    # Les enseignants à mettre à jour sont ceux existant déjà dans Sigale
    enseignants_existants = enseignants_proeco[enseignants_proeco['_merge'] == 'both'].drop(columns=['_merge'])


    # On exporte si option
    if export:
        nouveaux_enseignants.to_csv('mdps_nouveaux.csv', index=False)
        enseignants_existants.to_csv('mdps_existants.csv', index=False)

    # On ajoute les métadonnées
    for key, value in config.SIGALE_METADATA_FIELDS.items():
        nouveaux_enseignants[key] = value
        enseignants_existants[key] = value

    # On ajoute les champs par défaut de Proeco
    for key, value in config.SIGALE_PERSONNES_DEFAULT_FIELDS.items():
        nouveaux_enseignants[key] = value

    # Si dry_run, on s'arrête avant les modifications en DB
    if dry_run:
        logger.log(f"Dry run, pas de modification en DB, {len(nouveaux_enseignants)} mdps à insérer, {len(enseignants_existants)} mdps à mettre à jour")
        return None

    # On insère les nouveaux enseignants dans Sigale
    nouveaux_enseignants.to_sql('personnes', con=sigale_engine, schema='personnes', index=False, if_exists='append')
    logger.log(f"{len(nouveaux_enseignants)} nouveaux mdps introduits dans Sigale")

    # Si no-update, on s'arrête
    if not update:
        return None

    if config.UNIQUE_INDEX_ON_REG_NATIONAL:
        # Si un index unique existe sur registre_national_numero, on utilise cette méthode, plus rapide
        method = WriteMethods(index_columns=['registre_national_numero'], update_columns=config.SIGALE_UPDATE_FIELDS).update_on_conflict
    else:
        # Si pas d'index unique, on utilise la méthode lente
        method = WriteMethods(index_columns=['registre_national_numero'], update_columns=config.SIGALE_UPDATE_FIELDS).update_existing

    # On mets à jour les champs des enseignants existants basé sur la config
    enseignants_existants.to_sql('personnes', con=sigale_engine, schema='personnes', index=False, if_exists='append', method=method)
    logger.log(f"{len(enseignants_existants)} mdps mis à jour dans Sigale")

    return None


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        test()
    else:
        main()
