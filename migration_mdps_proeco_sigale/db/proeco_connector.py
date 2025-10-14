import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

from migration_mdps_proeco_sigale.config import FIREBIRD_CONNECT_ARGS

DB_DRIVER: str = "firebird+fdb"

class ProecoConnector:
    """
    Classe permettant de se connecter à Proeco, prends le nom du fichier env en paramètre
    """

    #: Url de la DB
    db_url: str
    #: Port de la DB
    db_port: int | str
    #: Base de données utilisée
    database_path: str
    #: Utilisateur
    db_user: str
    db_password: str

    def __init__(self, db: str = 'PROF.FDB'):
        load_dotenv()
        self.db_url = os.getenv('FIREBIRD_URL')
        self.db_port = os.getenv('FIREBIRD_PORT')
        self.db_user = os.getenv('FIREBIRD_USER')
        self.db_password = os.getenv('FIREBIRD_PASSWORD')
        db_base_path = os.getenv('FIREBIRD_DB_BASE_PATH')

        self.database_path = os.path.join(db_base_path, db)

    def create_engine(self):
        """
        Renvoi l'instance de connection pour accèder à la DB avec pandas ou sqlalchemy
        :return:
        """
        return create_engine(
            f"{DB_DRIVER}://{self.db_user}:{self.db_password}@{self.db_url}:{self.db_port}/{self.database_path}",
            connect_args=FIREBIRD_CONNECT_ARGS,
        )

    def test_connection(self, logger = None):
        """
        Pour tester la connexion à Proeco
        """
        try:
            conn = self.create_engine().connect()
            conn.close()
            if logger:
                logger.debug(f"La connection à la base de donnée {self.database_path} sur {self.db_url} est OK")
        except Exception as e:
            if logger:
                logger.error(e)
