import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

DB_DRIVER: str = "postgresql"
CONNECT_ARGS: dict = {"connect_timeout" : 10}



class SigaleConnector:
    """
    Classe permettant de se connecter à Proeco, prends le nom du fichier env en paramètre
    """

    #: Url de la DB
    db_url: str
    #: Port de la DB
    db_port: int | str
    #: Utilisateur
    db_user: str
    db_password: str
    #: Base de données utilisée
    database: str

    def __init__(self):
        load_dotenv()
        self.db_url = os.getenv('SIGALE_URL')
        self.db_port = os.getenv('SIGALE_PORT')
        self.db_user = os.getenv('SIGALE_USER')
        self.db_password = os.getenv('SIGALE_PASSWORD')
        self.database = os.getenv('SIGALE_DATABASE', 'usig')

    def create_engine(self):
        """
        Renvoi l'instance de connection pour accèder à la DB avec pandas ou sqlalchemy
        :return:
        """
        return create_engine(
            f"{DB_DRIVER}://{self.db_user}:{self.db_password}@{self.db_url}:{self.db_port}/{self.database}",
            connect_args=CONNECT_ARGS,
        )

    def test_connection(self):
        """
        Pour tester la connexion à Proeco
        """
        try:
            conn = self.create_engine().connect()
            conn.close()
            print("Connecté avec succès !")
            print(f"La connection à la base de donnée {self.database} sur {self.db_url} est OK")
        except Exception as e:
            print(e)
