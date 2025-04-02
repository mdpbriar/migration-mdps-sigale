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

### Configuration

copier .env.exemple en .env et saisir les identifiants de connexion

Tester les connections aux bases de données avec `python main.py test` ( ou `uv run main.py test`) 
