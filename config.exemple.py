from datetime import datetime
from typing import Dict, Literal

# Options de connection à Firebird
FIREBIRD_CONNECT_ARGS:dict = {"charset" : "ISO8859_1"}

# Code culture par défaut des nouveaux utilisateurs
USERS_DEFAULT_CULTURE_CODE = 'fr'

# Code du rôle par défaut des nouveaux utilisateurs
USERS_DEFAULT_ROLE_CODE = 'unknown'

# Champs et valeurs par défaut dans la table personnes de Sigale
# Le numéro dans created_by sera aussi utilisé pour filtrer les lignes à remplacer, le
# script de migration ne modifiera que les numéros, téléphones et adresses créées par lui-même
SIGALE_METADATA_FIELDS:dict = {
    'created_by': 1,
    'created_by_display': 'TeamMigration',
    'created_on': datetime.now(),
    'updated_by': 1,
    'updated_by_display': 'TeamMigration',
    'updated_on': datetime.now(),
    'optimistic_lock_version': 1
}

# Si à True, s'assure de ne modifier que les lignes créées par la migration (pour adresses, téléphones et emails)
# Cela permet de ne pas modifier des données manuellement saisies dans Sigale, des téléphones/adresses/emails ajoutés
UPDATE_ONLY_CREATED_BY_MIGRATION:bool = True

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

## Update fields
# la liste des champs qui sont mis à jours à chaque synchro,
# vous pouvez les commenter si vous ne souhaitez pas effectuer une mise à jour de tel ou tel champ.
# Ne pas ajouter de champ !

# Liste des champs à mettre à jour dans Sigale pour les mdps existant
SIGALE_UPDATE_FIELDS:list = [
    'nom',
    'prenom',
    'eid',
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

# Liste des champs à mettre à jour dans Sigale pour les emails
SIGALE_EMAIL_UPDATE_FIELDS:list = [
    'est_individuel',
    'est_principal',
    'valeur',
    'updated_by',
    'updated_on',
    'updated_by_display',
]

# Liste des champs à mettre à jour dans Sigale pour les téléphones
SIGALE_PHONE_UPDATE_FIELDS:list = [
    'est_individuel',
    'est_principal',
    'numero',
    'updated_by',
    'updated_on',
    'updated_by_display',
]

# Liste des champs à mettre à jour dans Sigale pour les adresses
SIGALE_ADRESSES_UPDATE_FIELDS:list = [
    'est_principale',
    'street',
    'city_name',
    'postal_code',
    'country_id',
    'city_id',
    'updated_by',
    'updated_on',
    'updated_by_display',
]

# Ces options permettent de définir quels champs de Proeco utiliser, et comment les importer
# Vous pouvez simplement supprimer ou commenter des options si vous ne souhaitez pas importer un champ

# Options pour les champs Email
EMAILS_FIELDS:Dict[Literal['email', 'email2'],Dict] = {
    'email':{
        'code_domaine': 'prive', # voir code = email_domaines dans core.parameter_types
        'est_individuel': True,
        'est_principal': False,
    },
    'email2':{
        'code_domaine': 'institutionnel',
        'est_individuel': True,
        'est_principal': False,
    }
}

# Options pour les champs téléphones
PHONE_FIELDS: Dict[Literal['teldomi', 'gsm', 'telresi', 'telbureau'], Dict] = {
    'teldomi': {
        'code_domaine': 'prive', # institutionnel, etablissement, prive, professionnel, voir code = telephone_domaines dans core.parameter_types
        'code_type': 'fixe', # fixe ou mobile, voir code = telephone_types dans core.parameter_types
        'est_individuel': True,
        'est_principal': False,
    },
    'gsm': {
        'code_domaine': 'prive',
        'code_type': 'mobile',
        'est_individuel': True,
        'est_principal': False,
    },
    # 'telresi': {
    #     'code_domaine': 'professionnel',
    #     'code_type': 'fixe',
    #     'est_individuel': True,
    #     'est_principal': False,
    # },
    # 'telbureau': {
    #     'code_domaine': 'etablissement',
    #     'code_type': 'fixe',
    #     'est_individuel': True,
    #     'est_principal': False,
    # },
}

# Options pour les champs adresses
ADRESSES_FIELDS: Dict[Literal['domi', 'resi'], Dict] = {
    # 2 clés possibles, domi ou resi, pour adresses Proeco domicile ou residence
    'domi': {
        'code_type': 'domicile', # professionnelle, correpsondance, domicile, residence, etc... voir code = adresse_types dans core.parameter_types
        'est_principale': True,
    },
    'resi': {
        'code_type': 'residence',
        'est_principale': False,
    }
}

# Chemin de fichier pour les exports
EXPORT_PATH:str = 'exports'

# fichier de logs
LOGS_FILE: str = 'logs/logs_migration.log'

# Fonction qui calcule le EID en fonction des champs de Proeco
# Vous pouvez utiliser n'importe quelle colonne présente dans la liste
# attributs_personne dans run.py:75
def get_eid(row: Dict) -> str:
    # pour l'exemple, ici l'EID est concaténé depuis les champs matric et reserved
    return str(row['matric']) + str(row['reserved'])
