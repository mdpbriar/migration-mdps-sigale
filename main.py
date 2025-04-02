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


    with sigale_engine.connect() as conn:
        # On créé une colonne
        conn.begin()
        conn.execute(text("ALTER TABLE personnes.personnes ADD COLUMN IF NOT EXISTS matric_prof INTEGER UNIQUE;"))
        conn.commit()
        logger.log("Colonne matric_prof créée si non existante")

    sql_mdps = text(f"""
    select 
    nom,
    prenom,
    UPPER(sexe) as sexe,
    regnat1 as registre_national_numero, 
    datnaiss as date_naissance
    from PERSONNE
    where regnat1 is not null
    and regnat1 != ''
    and CHAR_LENGTH(regnat1) = 11
    """)

    sql_parameter_sigale = text("""
    select pv.id, pv.code
    from core.parameter_values pv
    inner join core.parameter_types pt on pv.parameter_type_id = pt.id
    where pt.code = :type_parameter
    """)

    # On récupère le résulat de la SQL dans un dataframe
    enseignants_proeco = pd.read_sql_query(sql_mdps, proeco_engine)

    # On vérifie si il y a des doublons sur le numéro national
    duplicated = enseignants_proeco[enseignants_proeco.duplicated(subset=['registre_national_numero'])]
    logger.log(f"{len(duplicated)} enseignants ont des numéros de registre nationaux dupliqués : {duplicated}")

    # On supprime les doublons des données de Proeco
    enseignants_proeco.drop_duplicates(subset=['registre_national_numero'], inplace=True, keep='last')
    # On transforme les dates de naissances proeco en dates normales
    enseignants_proeco['date_naissance'] = enseignants_proeco['date_naissance'].apply(lambda x: DateUtils.convert_dateproeco_to_date(x))


    #### AJOUT DES SEXE_ID

    # On récupère les sexes de Sigale dans une table ['sexe_id', 'sexe']
    sexes_sigale = pd.read_sql_query(sql_parameter_sigale, sigale_engine, params={'type_parameter': 'sexes_sigale'}).rename(columns={'id': 'sexe_id', 'code': 'sexe'})

    # On récupère la première lettre du code sexe ( 'feminin', 'masculin' ) qu'on met en majuscule pour recoupement avec Proeco
    sexes_sigale['sexe'] = sexes_sigale['sexe'].apply(lambda x: x[0].upper())

    # On merge
    enseignants_proeco = enseignants_proeco.merge(sexes_sigale, on='sexe', how='inner', validate='m:1').drop(columns='sexe')


    # on récupère les personnes de Sigale
    personnes_sigales = pd.read_sql_query("select registre_national_numero from personnes.personnes where matric_etud is null", sigale_engine)

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
