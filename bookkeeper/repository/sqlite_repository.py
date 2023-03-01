import sqlite3

from inspect import get_annotations
from bookkeeper.repository.abstract_repository import AbstractRepository, T
from typing import Any


class SQLiteRepository(AbstractRepository[T]):
    def __init__(self, db_file: str, cls: type) -> None:
        self.db_file = db_file
        self.table_name = cls.__name__.lower()
        self.fields = get_annotations(cls, eval_str=True)
        self.fields.pop('pk')
        self.cls = cls

        definition_strings = [
            f'{f_name} {self.__class__._resolve_type(f_type)}'
            for f_name, f_type in self.fields.items()
        ]

        create_sql = f'CREATE TABLE IF NOT EXISTS {self.table_name} (' \
            + f'{", ".join(definition_strings + ["pk INTEGER PRIMARY KEY"])}' \
            + ')'

        with self.connect() as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(create_sql)
        con.close()

    def add(self, obj: T) -> int:
        if getattr(obj, 'pk', None) != 0:
            raise ValueError(f'trying to add object {obj} with filled `pk` attribute')
        names = ', '.join(self.fields.keys())
        place_holder = ', '.join("?" * len(self.fields))
        values = [getattr(obj, x) for x in self.fields]
        with self.connect() as con:
            cur = con.cursor()
            if len(self.fields) != 0:
                cur.execute(
                    f'INSERT INTO {self.table_name} ({names}) VALUES ({place_holder})',
                    values
                )
            else:  # specific case for table with only pk column
                cur.execute(f'INSERT INTO {self.table_name} DEFAULT VALUES')
            pk = cur.lastrowid
            obj.pk = pk if pk is not None else 0
        con.close()
        return obj.pk

    def __generate_object(self, db_row: tuple) -> T:
        obj = self.cls(self.fields)
        for field, value in zip(self.fields, db_row[1:]):
            setattr(obj, field, value)
        obj.pk = db_row[0]
        return obj

    def get(self, pk: int) -> T | None:
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                f'SELECT * FROM {self.table_name} WHERE pk = ?',
                [pk]
            )
            res = cur.fetchall()
        con.close()
        return self.cls(*res[0]) if len(res) != 0 else None

    def get_all(self, where: dict[str, Any] | None = None) -> list[T]:
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                f'SELECT * FROM {self.table_name}'
            )
            res = cur.fetchall()
        con.close()
        res: list[T] = [self.cls(*obj) for obj in res]
        if where is not None:
            res = [obj for obj in res
                   if all(getattr(obj, attr) == value for attr, value in where.items())]
        return res

    def update(self, obj: T) -> None:
        """ Обновить данные об объекте. Объект должен содержать поле pk. """
        pass

    def delete(self, pk: int) -> None:
        """ Удалить запись """
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(f'DELETE FROM {self.table_name} where pk = {pk}')
        con.close()
