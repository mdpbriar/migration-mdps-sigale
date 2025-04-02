from src.date_utils import DateUtils
from src.logger import Logger
from src.db.proeco_connector import ProecoConnector
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
    regnat1 as registre_national_numero, 
    datnaiss as date_naissance
    from PERSONNE
    where regnat1 is not null
    and regnat1 != ''
    and CHAR_LENGTH(regnat1) = 11
    """)

    # On récupère le résulat de la SQL dans un dataframe
    enseignants_proeco = pd.read_sql_query(sql_mdps, proeco_engine)

    # On vérifie si il y a des doublons sur le numéro national
    duplicated = enseignants_proeco[enseignants_proeco.duplicated(subset=['registre_national_numero'])]
    logger.log(f"{len(duplicated)} enseignants ont des numéros de registre nationaux dupliqués : {duplicated}")

    enseignants_proeco.drop_duplicates(subset=['registre_national_numero'], inplace=True, keep='last')

    enseignants_proeco['date_naissance'] = enseignants_proeco['date_naissance'].apply(lambda x: DateUtils.convert_dateproeco_to_date(x))

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



if __name__ == "__main__":
    if sys.argv[1] == 'test':
        test()
    else:
        main()
