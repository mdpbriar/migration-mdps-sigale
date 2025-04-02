# arguments de connexion à la DB Firebird
from datetime import datetime

FIREBIRD_CONNECT_ARGS:dict = {"charset" : "ISO8859_1"}

# Champs et valeurs par défaut dans la table personnes de Sigale
SIGALE_METADATA_FIELDS:dict = {
    'created_by': 1,
    'created_by_display': 'TeamMigration',
    'created_on': datetime.now(),
    'updated_by': 1,
    'updated_by_display': 'TeamMigration',
    'updated_on': datetime.now(),
    'optimistic_lock_version': 1
}

SIGALE_PERSONNES_DEFAULT_FIELDS:dict = {
    'est_collaborateur_rh': False,
    'est_confidentielle': False,
    'est_membre_personnel': True,
}

MAPPING_PROECO_SIGALE:dict = {
    'matric': 'matric_etud',

}