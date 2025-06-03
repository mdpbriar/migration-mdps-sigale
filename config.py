# arguments de connexion à la DB Firebird
from datetime import datetime
from typing import Dict

# Options de connection à Firebird
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

# Valeurs par défaut des champs à la création
SIGALE_PERSONNES_DEFAULT_FIELDS:dict = {
    'est_collaborateur_rh': False,
    'est_confidentielle': False,
    'est_membre_personnel': True,
}

# correspondance des champs Proeco -> Sigale
MAPPING_PROECO_SIGALE:dict = {
    'matric': 'matric_mdp',
}

# Mapping des états civils entre Proeco et Sigale
MAPPING_ETATS_CIVILS:dict = {
    'L': 'cohabitant_legal',
    'C': 'celibataire',
    'D': 'divorce',
    'M': 'marie',
    'V': 'veuf',
    'O': 'cohabitant',
    'S': 'separe',
    'Y': 'separe_corp',
    'G': 'religieux',
    'Z': 'decede',
    'R': 'remarie',
}

# Liste des champs à mettre à jour dans Sigale pour les mdps existant
SIGALE_UPDATE_FIELDS:list = [
    'nom',
    'prenom',
    'lieu_naissance_hors_belgique',
    'sexe_id',
    'city_id_naissance',
    'pays_id_nationalite',
    'pays_id_naissance_pays',
    'etat_civil_id',
    'matric_mdp',
    'updated_by',
    'updated_on',
    'updated_by_display',
]

SIGALE_EMAIL_UPDATE_FIELDS:list = [
    'est_individuel',
    'est_principal',
    'valeur',
    'updated_by',
    'updated_on',
    'updated_by_display',
]


EMAILS_FIELDS:Dict[str,Dict] = {
    'email':{
        'code_domaine': 'prive',
        'est_individuel': True,
        'est_principal': False,
    },
    'email2':{
        'code_domaine': 'institutionnel',
        'est_individuel': True,
        'est_principal': False,
    }
}

EXPORT_PATH:str = 'exports'