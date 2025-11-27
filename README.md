## Installation

### Avec pip

Créez un nouvel environnement virtuel avec
`python -m venv .venv`

Ouvrez l'environnement virtuel
`source .venv/bin/activate`

Lancez l'installation avec `pip install .`

### Avec uv

Commencez par installer uv : https://docs.astral.sh/uv/getting-started/installation/

Sélectionnez la version de python souhaitée avec `uv python pin 3.11` pour 3.11 par exemple

Créer l'environnement virtuel et installer les dépendences avec `uv sync` depuis la racine du projet.

Avec uv, vous pouvez ensuite lancer les commandes depuis l'environnement virtuel ( `source .venv/bin/activate`), ou lancer les commandes précédées de `uv run`.
Ex:
`uv run main.py`

> **Important**
> 
> Pour la suite, nous noterons `python main.py` pour l'exécution du script, 
> selon que vous ayez activé l'environnement virtuel ou non, vous pourriez 
> devoir remplacer le début de commande par `uv run main.py`

### Configuration


> **Important**
> 
> copier `.env.exemple` en `.env` et saisir les identifiants de connexion
> 
> copier `config.exemple.py` en `config.py` dans le dossier racine

**En cas d'oubli de la copie du fichier de config, la config par défaut est utilisée**

Tester les connections aux bases de données avec `python main.py test` ( ou `uv run main.py test`) 

### Utilisation

Lancez simplement `python main.py --help` pour afficher les options du script.

Le meilleur moyen dans un premier temps pour vérifier son bon fonctionnement est de lancer `python main.py --dry-run`,
cela lancera le script et traitera les données sans rien insérer en db.
Vous pouvez y ajouter `--export` pour obtenir un export en csv des lignes que le script insérerait en DB

### Fonctionnement

> **Important**
> 
> Consultez le fichier config.py pour mieux comprendre les options et diverses configuration du script

Voici sommairement les étapes effectuées par le script de migration :

1. Il récupère la liste des mdps depuis la table PERSONNE de Proeco, avec leurs adresses, numéros de téléphone et emails.
2. Il filtre pour ne garder que les mdps avec un contrat en cours ou à venir ( sauf si utilisation `--historique`)
3. Il recoupe avec les données Sigale pour ajouter les clés étrangères nécessaires (city_id_naissance, sexe_id, etat_civil_id, etc.)
4. Il recoupe avec la liste des mdps déjà présents dans Sigale, pour séparer les mdps existants, et les nouveaux.
5. Il exporte les données dans un csv si `--export` utilisé
6. S'arrête si `--dry-run` utilisé
7. Ajoute les nouvelles données
8. Met à jour les données existantes ( sauf si `--no-update` utilisé)
9. Répète les étapes 3 à 9 pour les emails, téléphones et adresses

Par défaut, le script ne modifie que les emails, téléphones et adresses créés par lui-même.
( ou par un script de migration, il se réfère au created_by), ce comportement peut être changé