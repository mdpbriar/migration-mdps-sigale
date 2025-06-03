from typing import Literal

from sqlalchemy import create_engine, update, bindparam, delete
from sqlalchemy.dialects.postgresql import insert as insert_postgresql
from sqlalchemy.dialects.mysql import insert as insert_mysql

class WriteMethods:
    """
    class pour les méthodes pandas.to_sql,
    permet de passer des paramètres supplémentaires aux méthodes invoquées par pandas
    :param: index_columns : Colonnes servant d'index
    :param: update_columns : Colonnes à mettre à jour
    :param: sql_dialect : dialect sql, mysql ou postgresql, postgresql par défaut
    """

    def __init__(self, index_columns: list[str], update_columns: list[str],
                 sql_dialect: Literal['postgresql', 'mysql'] = 'postgresql'):
        self.index_columns = index_columns
        self.update_columns = update_columns
        self.sql_dialect = sql_dialect


    def update_existing(self, table, conn, keys, data_iter):
        """
        Mise à jour des lignes existantes, ne tente pas d'insérer des données non existantes
        Les paramètres sont automatiquement passés par pandas

        :return:
        """
        # data = [dict(zip(keys, row)) for row in data_iter]
        data = []
        for row in data_iter:
            # On change le nom des colonnes d'index pour éviter CompileError avec bindparam de sqlalchemy
            single_row = dict(zip(keys, row))
            for column in self.index_columns:
                single_row[f"{column}_index"] = single_row.pop(column)

            data.append(single_row)

        update_cols = {}
        for column in self.update_columns:
            update_cols[column] = bindparam(column)

        stmt = update(table.table).values(update_cols)
        for column in self.index_columns:
            stmt = stmt.where(table.table.c[column] == bindparam(f"{column}_index"))

        result = conn.execute(stmt, data)
        return result.rowcount

    def update_on_conflict(self, table, conn, keys, data_iter):
        """
        Tente d'insérer les rows, si existe ( sur base de index_columns ), alors met à jour les colonnes présentes dans
        update_columns
        Les paramètres sont automatiquement passés par pandas
        :return:
        """
        # on récupère les données de la dataframe dans un dictionnaire
        data = [dict(zip(keys, row)) for row in data_iter]
        updates = {}
        # pour une db postgresql
        if self.sql_dialect == 'postgresql':
            stmt = (
                insert_postgresql(table.table)
                .values(data)
            )
            for column in self.update_columns:
                updates[column] = stmt.excluded[column]
            stmt = stmt.on_conflict_do_update(
                index_elements=self.index_columns,
                set_=updates,
            )
        else:
            stmt = (
                insert_mysql(table.table)
                .values(data)
            )
            columns = [*self.index_columns, *self.update_columns]
            for column in columns:
                updates[column] = stmt.inserted[column]
            stmt = stmt.on_duplicate_key_update(
                updates
            )
        # index_columns = ['id_people']
        # update_columns = ['ancservpo', 'ancservcf', 'ancadm']
        # print(table, conn, keys, data_iter, index_columns, update_columns)
        result = conn.execute(stmt)
        return result.rowcount

