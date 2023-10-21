import re
from asyncio import TaskGroup
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, Literal, Type

import sqlalchemy as sa
from cwtch import asdict, dataclass, from_attributes
from sqlalchemy import delete, insert, literal, select, union_all, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker

__all__ = [
    "AlreadyExistsError",
    "NepenteError",
    "NotFoundError",
    "OrderBy",
    "bind_engine",
    "transaction",
    "CRUD",
]


_conn: dict[str | None, ContextVar] = {}
_engine = {}


@dataclass(validate=False)
class OrderBy:
    by: Any
    order: Literal["asc", "desc"] = "asc"


class NepenteError(Exception):
    pass


class NotFoundError(NepenteError):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __str__(self):
        return f"item with {self.key} '{self.value}' not found"


class AlreadyExistsError(NepenteError):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __str__(self):
        return f"key ({self.key})=({self.value}) already exists"


def bind_engine(engine, name: str | None = None):
    _engine[name] = engine
    _conn[name] = ContextVar("conn", default=None)


@asynccontextmanager
async def transaction(engine_name: str | None = None):
    if (conn := _conn[engine_name].get()) is None:
        async with _engine[engine_name].connect() as conn:
            async with conn.begin():
                _conn[engine_name].set(conn)
                try:
                    yield conn
                finally:
                    _conn[engine_name].set(None)
    else:
        yield conn


@asynccontextmanager
async def _get_conn(engine_name: str | None = None):
    if (conn := _conn[engine_name].get()) is None:
        async with _engine[engine_name].connect() as conn:
            async with conn.begin():
                yield conn
    else:
        yield conn


@asynccontextmanager
async def _get_sess(engine_name: str | None = None):
    async with _get_conn(engine_name=engine_name) as conn:
        async_session = async_sessionmaker(conn, expire_on_commit=False)
        async with async_session() as sess:
            async with sess.begin():
                yield sess


def raise_exc(e: Exception):
    if isinstance(e, sa.exc.IntegrityError):
        detail_match = re.match(r".*\nDETAIL:\s*(?P<text>.*)$", e.orig.args[0])
        if detail_match:
            text = detail_match.groupdict()["text"].strip()
            m = re.match(r"Key \((?P<key>.*)\)=\((?P<key_value>.*)\) already exists.", text)
            if m:
                key = m.groupdict()["key"]
                key_value = m.groupdict()["key_value"].strip('\\"')
                raise AlreadyExistsError(key, key_value)
    raise e


class Meta(type):
    def __new__(cls, name, bases, ns):
        if len(bases) > 0:
            if (model_db := ns.get("model_db")) is None:
                raise ValueError("model_db is required")
            if (model := ns.get("model")) is None:
                raise ValueError("model is required")
            if not any((getattr(model, "__cwtch_model__", None), getattr(model, "__cwtch_view__", None))):
                raise ValueError("model is not cwtch model or view")
            if (model_create := ns.get("model_create")) is None:
                raise ValueError("model_create is required or use model_create = False")
            if not any((getattr(model_create, "__cwtch_model__", None), getattr(model_create, "__cwtch_view__", None))):
                raise ValueError("model_create is not cwtch model or view")
            if (model_save := ns.get("model_save")) is None:
                raise ValueError("model_save is required or use model_save = False")
            if not any((getattr(model_save, "__cwtch_model__", None), getattr(model_save, "__cwtch_view__", None))):
                raise ValueError("model_save is not cwtch model or view")
            if (model_update := ns.get("model_update")) is None:
                raise ValueError("model_update is required or use model_update = False")
            if not any((getattr(model_update, "__cwtch_model__", None), getattr(model_update, "__cwtch_view__", None))):
                raise ValueError("model_update is not cwtch model or view")

            if not isinstance(model_create, bool):
                ns["_fields_create"] = set(model_db.__table__.columns.keys()) & set(model_create.__dataclass_fields__)
                assert ns["_fields_create"]
            if not isinstance(model_save, bool):
                ns["_fields_save"] = set(model_db.__table__.columns.keys()) & set(model_save.__dataclass_fields__)
                assert ns["_fields_save"]
            if not isinstance(model_update, bool):
                ns["_fields_update"] = set(model_db.__table__.columns.keys()) & set(model_update.__dataclass_fields__)
                assert ns["_fields_update"]

        return super().__new__(cls, name, bases, ns)

    def __getattribute__(cls, name):
        if name in {"create", "create_many"} and super().__getattribute__("model_create") is False:
            raise Exception("model_create is not defined")
        if name in {"save", "save_many"} and super().__getattribute__("model_save") is False:
            raise Exception("model_save is not defined")
        if name in {"update", "update_many"} and super().__getattribute__("model_update") is False:
            raise Exception("model_update is not defined")
        return super().__getattribute__(name)


class CRUD(metaclass=Meta):
    engine_name: str | None = None

    model_db = None

    model: Type = None
    model_create: Type | bool = None
    model_save: Type | bool = None
    model_update: Type | bool = None

    key = None
    index_elements: list | None = None
    order_by: list[OrderBy] | OrderBy | str | None = None
    joinedload: dict = {}

    _fields_create: set[str] = set()
    _fields_save: set[str] = set()
    _fields_update: set[str] = set()

    @classmethod
    async def _execute(cls, stmt):
        async with _get_conn(engine_name=cls.engine_name) as conn:
            try:
                return await conn.execute(stmt)
            except Exception as e:
                raise_exc(e)

    @classmethod
    def _get_key(cls, key=None):
        if key is None:
            key = cls.key
        if key is None:
            raise ValueError("key is None")
        if isinstance(key, str):
            key = getattr(cls.model_db, key)
        return key

    @classmethod
    def _get_order_by(cls, order_by: list[OrderBy] | OrderBy | str | None = None) -> list | None:
        if order_by is None:
            order_by = cls.order_by or cls._get_key()
        if order_by is not None:
            if not isinstance(order_by, list):
                order_by = [order_by]
            else:
                order_by = list(order_by)
            for i, x in enumerate(order_by):
                if not isinstance(x, OrderBy):
                    x = OrderBy(by=x, order="asc")
                if isinstance(x.by, str):
                    x.by = getattr(cls.model_db, x.by)
                order_by[i] = getattr(sa, x.order)(x.by)
        return order_by

    @classmethod
    def _make_from_db_data(cls, item_db, joinedload: dict | None = None) -> dict | None:
        return {}

    @classmethod
    def _from_db(
        cls,
        data_db,
        data: dict | None = None,
        exclude: list | None = None,
        suffix: str | None = None,
        model_out=None,
    ):
        return from_attributes(
            model_out or cls.model,
            data_db,
            data=data,
            exclude=exclude,
            suffix=suffix,
            reset_circular_refs=True,
        )

    @classmethod
    def _get_create_values(cls, model) -> dict:
        return asdict(model, include=cls._fields_create)

    @classmethod
    def _make_create_stmt(cls, model, returning: bool | None = None):
        model_db = cls.model_db
        if returning:
            return insert(model_db).values(cls._get_create_values(model)).returning(model_db)
        return insert(cls.model_db).values(cls._get_create_values(model))

    @classmethod
    async def create(cls, model, model_out=None, returning: bool = True):
        item_db = (await cls._execute(cls._make_create_stmt(model, returning=returning))).one()
        return cls._from_db(item_db, data=cls._make_from_db_data(item_db), model_out=model_out)

    @classmethod
    def _make_create_many_stmt(cls, models: list, returning: bool | None = None):
        model_db = cls.model_db
        if returning:
            return insert(model_db).values([cls._get_create_values(model) for model in models]).returning(model_db)
        return insert(model_db).values([cls._get_create_values(model) for model in models])

    @classmethod
    async def create_many(cls, models: list, model_out=None, returning: bool = True) -> list:
        if not models:
            return []
        if len({model.__class__ for model in models}) > 1:
            raise ValueError("mix of different models not supported")
        return [
            cls._from_db(item_db, data=cls._make_from_db_data(item_db), model_out=model_out)
            for item_db in (await cls._execute(cls._make_create_many_stmt(models, returning=returning))).all()
        ]

    @classmethod
    def _get_save_values(cls, model) -> dict:
        if isinstance(model, cls.model_create):
            return asdict(model, include=cls._fields_create)
        return asdict(model, include=cls._fields_save)

    @classmethod
    def _get_save_set(cls, excluded, model) -> dict:
        if isinstance(model, cls.model_create):
            return {k: getattr(excluded, k) for k in cls._create_fields}
        return {k: getattr(excluded, k) for k in cls._fields_save}

    @classmethod
    def _make_save_stmt(cls, model, index_elements: list | None = None, returning: bool | None = None):
        model_db = cls.model_db
        stmt = insert(model_db).values(cls._get_save_values(model))
        index_elements = index_elements or cls.index_elements
        if index_elements is None:
            raise ValueError("index_elements is None")
        index_elements = list(map(lambda e: isinstance(e, str) and getattr(model_db, e) or e, index_elements))
        if returning:
            return stmt.on_conflict_do_update(
                index_elements=index_elements,
                set_=cls._get_save_set(stmt.excluded, model),
            ).returning(model_db)
        return stmt.on_conflict_do_update(
            index_elements=index_elements,
            set_=cls._get_save_set(stmt.excluded, model),
        )

    @classmethod
    async def save(cls, model, index_elements: list | None = None, model_out=None, returning: bool = True):
        item_db = (
            await cls._execute(
                cls._make_save_stmt(
                    model,
                    index_elements=index_elements,
                    returning=returning,
                )
            )
        ).one()
        return cls._from_db(item_db, data=cls._make_from_db_data(item_db), model_out=model_out)

    @classmethod
    def _make_save_many_stmt(cls, models: list, index_elements: list | None = None, returning: bool | None = None):
        model_db = cls.model_db
        stmt = insert(model_db).values([cls._get_save_values(model) for model in models])
        index_elements = index_elements or cls.index_elements
        if index_elements is None:
            raise ValueError("index_elements is None")
        index_elements = list(map(lambda e: isinstance(e, str) and getattr(model_db, e) or e, index_elements))
        if returning:
            return stmt.on_conflict_do_update(
                index_elements=index_elements,
                set_=cls._get_save_set(stmt.excluded, models[0]),
            ).returning(model_db)
        return stmt.on_conflict_do_update(
            index_elements=index_elements,
            set_=cls._get_save_set(stmt.excluded, models[0]),
        )

    @classmethod
    async def save_many(
        cls,
        models: list,
        index_elements: list | None = None,
        model_out=None,
        returning: bool = True,
    ) -> list:
        if not models:
            return []
        if len({model.__class__ for model in models}) > 1:
            raise ValueError("mix of different models not supported")
        return [
            cls._from_db(item_db, data=cls._make_from_db_data(item_db), model_out=model_out)
            for item_db in (
                await cls._execute(
                    cls._make_save_many_stmt(
                        models,
                        index_elements=index_elements,
                        returning=returning,
                    )
                )
            ).all()
        ]

    @classmethod
    def _get_update_values(cls, model) -> dict:
        return asdict(model, include=cls._fields_update, exclude_unset=True)

    @classmethod
    def _make_update_stmt(cls, model, key: str, returning: bool | None = None):
        model_db = cls.model_db
        if returning:
            return (
                update(model_db)
                .values(cls._get_update_values(model))
                .where(getattr(model_db, key) == getattr(model, key))
                .returning(model_db)
            )
        return (
            update(model_db)
            .values(cls._get_update_values(model))
            .where(getattr(model_db, key) == getattr(model, key))
            .returning(getattr(model_db, key))
        )

    @classmethod
    async def update(cls, model, key: str, model_out=None, returning: bool = True):
        if item_db := (await cls._execute(cls._make_update_stmt(model, key, returning=returning))).one_or_none():
            if returning:
                return cls._from_db(item_db, data=cls._make_from_db_data(item_db), model_out=model_out)
        else:
            raise NotFoundError(key=key, value=getattr(model, key))

    @classmethod
    async def update_many(cls, models: list, key: str, model_out=None, returning: bool = True) -> list:
        async with TaskGroup() as tg:
            tasks = [
                tg.create_task(cls.update(model, key, model_out=model_out, returning=returning)) for model in models
            ]
        return [await task for task in tasks]

    @classmethod
    def _make_get_stmt(cls, key, key_value):
        return select(cls.model_db).where(key == key_value)

    @classmethod
    async def get(
        cls,
        key_value,
        key=None,
        raise_not_found: bool | None = None,
        joinedload: dict | None = None,
        model_out=None,
    ):
        key = cls._get_key(key=key)
        stmt = cls._make_get_stmt(key, key_value)

        if joinedload is None or not any(joinedload.values()):
            if item_db := (await cls._execute(stmt)).one_or_none():
                return cls._from_db(item_db, data=cls._make_from_db_data(item_db), model_out=model_out)
            if raise_not_found:
                raise NotFoundError(key=key, value=key_value)
            return

        exclude = []
        for k, v in cls.joinedload.items():
            if joinedload.get(k) is True:
                stmt = stmt.options(v)
            else:
                exclude.append(k)

        async with _get_sess(engine_name=cls.engine_name) as sess:
            if item_db := (await sess.execute(stmt)).unique().scalars().one_or_none():
                return cls._from_db(
                    item_db,
                    exclude=exclude,
                    data=cls._make_from_db_data(item_db, joinedload=joinedload),
                    model_out=model_out,
                )
            if raise_not_found:
                raise NotFoundError(key=key, value=key_value)

    @classmethod
    def _make_get_many_stmt(cls, *args, where=None, **kwds):
        stmt = select(sa.func.count("*").over().label("rows_total"), cls.model_db)
        if where is not None:
            stmt = stmt.where(where)
        return stmt

    @classmethod
    async def get_many(
        cls,
        where=None,
        page: int | None = None,
        page_size: int | None = None,
        order_by=None,
        joinedload: dict | None = None,
        model_out=None,
        **kwds,
    ) -> tuple[int, list]:
        model_db = cls.model_db

        stmt = cls._make_get_many_stmt(where=where, **kwds)

        order_by = cls._get_order_by(order_by)
        if order_by:
            stmt = stmt.order_by(*order_by)

        cte = stmt.cte("cte")

        stmt = select(literal(1).label("i"), cte)

        if page_size:
            page = page or 1
            stmt = stmt.limit(page_size).offset((page - 1) * page_size)

        stmt = union_all(select(sa.literal(0).label("i"), cte).limit(1), stmt)

        from_db = cls._from_db
        make_from_orm_data = cls._make_from_db_data

        if joinedload is None or not any(joinedload.values()):
            rows = (await cls._execute(stmt)).all()
            return rows[0].rows_total if rows else 0, [
                from_db(row, data=make_from_orm_data(row), model_out=model_out) for row in rows[1:]
            ]

        main_cte = stmt.cte("main_cte")
        c = main_cte.c

        stmt = select(c.i, c.rows_total, model_db)

        exclude = []
        for k, v in cls.joinedload.items():
            if joinedload.get(k) is True:
                stmt = stmt.options(v)
            else:
                exclude.append(k)

        stmt = stmt.join(
            main_cte,
            sa.and_(
                *[
                    getattr(model_db, column.name) == getattr(c, column.name)
                    for column in model_db.__table__.primary_key.columns
                ]
            ),
        ).order_by(sa.asc(c.i))
        if order_by:
            stmt = stmt.order_by(*order_by)

        async with _get_sess(engine_name=cls.engine_name) as sess:
            rows = (await sess.execute(stmt)).unique().all()
            return rows[0].rows_total if rows else 0, [
                from_db(
                    row[2],
                    exclude=exclude,
                    data=make_from_orm_data(row[2], joinedload=joinedload),
                    model_out=model_out,
                )
                for row in rows[1:]
            ]

    @classmethod
    def _make_get_or_create_stmt(cls, model, update_element: str, index_elements: list | None = None):
        model_db = cls.model_db
        stmt = insert(model_db).values(cls._get_create_values(model))
        index_elements = index_elements or cls.index_elements
        if index_elements is None:
            raise ValueError("index_elements is None")
        index_elements = list(map(lambda e: isinstance(e, str) and getattr(model_db, e) or e, index_elements))
        return stmt.on_conflict_do_update(
            index_elements=index_elements,
            set_={update_element: getattr(stmt.excluded, update_element)},
        ).returning(model_db)

    @classmethod
    async def get_or_create(cls, model, update_element: str, index_elements: list | None = None, model_out=None):
        item_db = (
            await cls._execute(
                cls._make_get_or_create_stmt(
                    model,
                    update_element,
                    index_elements=index_elements,
                )
            )
        ).one()
        return cls._from_db(item_db, data=cls._make_from_db_data(item_db), model_out=model_out)

    @classmethod
    def _make_delete_stmt(cls, key, key_value, returning: bool | None = None):
        if returning:
            return delete(cls.model_db).where(key == key_value).returning(cls.model_db)
        return delete(cls.model_db).where(key == key_value)

    @classmethod
    async def delete(
        cls,
        key_value,
        key=None,
        raise_not_found: bool | None = None,
        returning: bool | None = None,
        model_out=None,
    ):
        key = cls._get_key(key=key)
        if returning:
            item_db = (await cls._execute(cls._make_delete_stmt(key, key_value, returning=returning))).one_or_none()
            if raise_not_found and item_db is None:
                raise NotFoundError(key=key, value=key_value)
            if item_db:
                return cls._from_db(item_db, data=cls._make_from_db_data(item_db), model_out=model_out)
        else:
            result = await cls._execute(cls._make_delete_stmt(key, key_value))
            if raise_not_found and result.rowcount != 1:
                raise NotFoundError(key=key, value=key_value)
            return result.rowcount
