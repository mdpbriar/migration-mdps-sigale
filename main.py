from datetime import date
from typing import Literal

from unidecode import unidecode

from src.date_utils import DateUtils
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


    # with sigale_engine.connect() as conn:
    #     # On créé une colonne
    #     conn.begin()
    #     conn.execute(text("ALTER TABLE personnes.personnes ADD COLUMN IF NOT EXISTS matric_prof INTEGER UNIQUE;"))
    #     conn.commit()
    #     logger.log("Colonne matric_prof créée si non existante")

    contrat_en_cours_uniquement = True
    action_when_duplicates: Literal['drop','stop'] = 'stop'

    sql_mdps_proeco = text(f"""
    select
    matric,
    nom,
    prenom,
    initprenom1,
    initprenom2,
    initprenom3,
    initprenom4,
    UPPER(sexe) as sexe,
    nation,
    paynaiss,
    lieunaiss,
    etatcivil,
    matriche,
    reservef,
    regnat1 as registre_national_numero, 
    datnaiss as date_naissance
    from PERSONNE
    where regnat1 is not null
    and regnat1 != ''
    and CHAR_LENGTH(regnat1) = 11
    order by matric
    """)

    sql_contrats_en_cours_proeco = text("""
    select matric 
    from FONCTION
    where DATEDEB <= :date_proeco
        and ( DATEFIN >= :date_proeco or DATEFIN is NULL)
    """)

    sql_mdps_sigale = text("""
    select registre_national_numero
    from personnes.personnes
    where est_membre_personnel = true
    and registre_national_numero != ''
    and registre_national_numero is not null
    """)

    sql_parameter_sigale = text("""
    select pv.id, pv.code
    from core.parameter_values pv
    inner join core.parameter_types pt on pv.parameter_type_id = pt.id
    where pt.code = :type_parameter
    """)

    # On récupère le résulat de la SQL dans un dataframe
    enseignants_proeco = pd.read_sql_query(sql_mdps_proeco, proeco_engine)

    # Si option pour ne prendre que les contrats en cours
    if contrat_en_cours_uniquement:
        # On calcule la date proeco d'aujourd'hui
        today_proeco = DateUtils.convert_date_to_dateproeco(date.today())
        # On récupère les contrats en cours
        contrats_en_cours = pd.read_sql_query(sql_contrats_en_cours_proeco, proeco_engine, params={'date_proeco': today_proeco})
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
    # On renomme les champs selon le mapping pour correspondre à Sigale
    enseignants_proeco.rename(config.MAPPING_PROECO_SIGALE, inplace=True)


    #### AJOUT DES SEXE_ID ####
    # On récupère les sexes de Sigale dans une table ['sexe_id', 'sexe']
    sexes_sigale = pd.read_sql_query(sql_parameter_sigale, sigale_engine, params={'type_parameter': 'sexes_sigale'}).rename(columns={'id': 'sexe_id', 'code': 'sexe'})
    # On récupère la première lettre du code sexe ( 'feminin', 'masculin' ) qu'on met en majuscule pour recoupement avec Proeco
    sexes_sigale['sexe'] = sexes_sigale['sexe'].apply(lambda x: x[0].upper())
    # On merge
    enseignants_proeco = enseignants_proeco.merge(sexes_sigale, on='sexe', how='inner', validate='m:1').drop(columns='sexe')

    ### AJOUT DES PAYS_ID_NATIONALITE
    # On reformate les codes pays dans notre liste d'enseignants, majuscules, suppression d'accents
    enseignants_proeco['nation'] = enseignants_proeco['nation'].apply(str.upper).apply(unidecode)
    # On récupère les nationalités de Sigale dans une table ['pays_id_nationalite', 'code']
    pays_sigale = pd.read_sql_query("select id as pays_id_nationalite, code as code_pays from core.countries", sigale_engine)
    # On merge
    enseignants_proeco = enseignants_proeco.merge(pays_sigale, left_on='nation', right_on='code_pays', how='left', validate='m:1').drop(columns=['nation'])

    ### AJOUT DES PAYS_ID_NAISSANCE_PAYS
    # On reformate les codes pays dans notre liste d'enseignants, majuscules, suppression d'accents
    enseignants_proeco['paynaiss'] = enseignants_proeco['paynaiss'].apply(str.upper).apply(unidecode)
    # On renome l'id pour correspondre à la clé étrangère dans personnes.personnes
    pays_sigale.rename(columns={'pays_id_nationalite': 'pays_id_naissance_pays', 'code_pays': 'paynaiss'}, inplace=True)
    enseignants_proeco = enseignants_proeco.merge(pays_sigale, on='paynaiss', how='left',
                                                  validate='m:1').drop(columns=['paynaiss'])


    ### AJOUT DES CITY_ID_NAISSANCE
    # On récupère les villes de Sigale en ajoutant le code pays BE
    villes_sigale = pd.read_sql_query("select id as city_id_naissance, name as city_name from core.cities order by postal_code asc", sigale_engine)
    villes_sigale['code_pays'] = 'BE'
    # On reformate les noms de villes pour s'assurer du match
    villes_sigale['city_name'] = villes_sigale['city_name'].apply(str.upper).apply(unidecode)
    enseignants_proeco['lieunaiss'] = enseignants_proeco['lieunaiss'].apply(str.upper).apply(unidecode)

    # On supprime les doublons, (plusieurs fois la même ville avec codes postaux différents)
    # keep first, pour garder le code postal le plus petit (on conserve 4000 au lieu de 4020)
    villes_sigale.drop_duplicates(subset=['city_name', 'code_pays'], inplace=True, keep='first')
    enseignants_proeco = (enseignants_proeco.merge(villes_sigale, left_on=['lieunaiss', 'code_pays'], right_on=['city_name', 'code_pays'], how='left', validate='m:1')
                          .drop(columns=['city_name', 'code_pays', 'lieunaiss']))

    ### AJOUT DES ETAT_CIVIL_ID
    # On récupère les états civils de Sigale
    etats_civils_sigale = pd.read_sql_query(sql_parameter_sigale, sigale_engine, params={'type_parameter': 'etats_civils'})
    # On renomme pour correspondre aux champs existants dans Sigale et Proeco
    etats_civils_sigale.rename(columns={'id': 'etat_civil_id', 'code': 'etatcivil'}, inplace=True)
    # On remplace les états civils de Proeco avec ceux de Sigale pour préparer le merge
    enseignants_proeco['etatcivil'] = enseignants_proeco['etatcivil'].apply(lambda code: config.MAPPING_ETATS_CIVILS.get(code))
    # On merge pour ajouter les id d'états civils à nos mdps
    enseignants_proeco = enseignants_proeco.merge(etats_civils_sigale, on='etatcivil', how='left', validate='m:1').drop(columns='etatcivil')

    print(enseignants_proeco)
    exit()



    ### RECOUPEMENT AVEC LES DONNEES DE SIGALE
    # on récupère les personnes de Sigale
    personnes_sigales = pd.read_sql_query(sql_mdps_sigale, sigale_engine)

    # On merge pour voir quels enseignants sont déjà dans Sigale
    enseignants_proeco = enseignants_proeco.merge(personnes_sigales, on='registre_national_numero', how='left', indicator=True, validate='1:1')

    # Les nouveaux enseignants sont ceux n'existant que dans Proeco
    nouveaux_enseignants = enseignants_proeco[enseignants_proeco['_merge'] == 'left_only'].drop(columns=['_merge'])
    # Les enseignants à mettre à jour sont ceux existant déjà dans Sigale
    enseignants_existants = enseignants_proeco[enseignants_proeco['_merge'] == 'both'].drop(columns=['_merge'])

    print(nouveaux_enseignants)
    print(enseignants_existants)

    # On ajoute les métadonnées
    for key, value in config.SIGALE_METADATA_FIELDS.items():
        nouveaux_enseignants[key] = value

    # On ajoute les champs par défaut de Proeco
    for key, value in config.SIGALE_PERSONNES_DEFAULT_FIELDS.items():
        nouveaux_enseignants[key] = value





if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        test()
    else:
        main()
