import re
import typing

from asyncio import gather
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, Callable, Literal, Never, Type

import sqlalchemy as sa

from cwtch import asdict, dataclass, field, from_attributes
from sqlalchemy import Row, delete, func, literal, select, union_all, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, aliased


__all__ = [
    "AlreadyExistsError",
    "BadParamsError",
    "NepenteError",
    "NotFoundError",
    "OrderBy",
    "bind_engine",
    "transaction",
    "CRUD",
]


_conn: dict[str | None, ContextVar] = {}
_engine = {}


@dataclass
class OrderBy:
    by: Any = field(validate=False)
    order: Literal["asc", "desc"] = "asc"


class NepenteError(Exception):
    pass


class BadParamsError(NepenteError):
    def __init__(self, message: str, param: str | None = None):
        self.message = message
        self.param = param

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.__str__()


class NotFoundError(NepenteError):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __str__(self):
        return f"item with ({self.key})=({self.value}) not found"


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
async def get_conn(engine_name: str | None = None):
    if (conn := _conn[engine_name].get()) is None:
        async with _engine[engine_name].connect() as conn:
            async with conn.begin():
                yield conn
    else:
        yield conn


@asynccontextmanager
async def get_sess(engine_name: str | None = None):
    async with get_conn(engine_name=engine_name) as conn:
        async_session = async_sessionmaker(conn, expire_on_commit=False)
        async with async_session() as sess:
            async with sess.begin():
                yield sess


def _raise_exc(e: Exception) -> Never:
    if isinstance(e, sa.exc.IntegrityError):
        detail_match = re.match(r".*\nDETAIL:\s*(?P<text>.*)$", e.orig.args[0])  # type: ignore
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
                raise ValueError("model is not cwtch model")
            if (model_create := ns.get("model_create")) is None:
                raise ValueError("model_create is required or use model_create = False")
            if not any((getattr(model_create, "__cwtch_model__", None), getattr(model_create, "__cwtch_view__", None))):
                raise ValueError("model_create is not cwtch model")
            if (model_save := ns.get("model_save")) is None:
                raise ValueError("model_save is required or use model_save = False")
            if not any((getattr(model_save, "__cwtch_model__", None), getattr(model_save, "__cwtch_view__", None))):
                raise ValueError("model_save is not cwtch model")
            if (model_update := ns.get("model_update")) is None:
                raise ValueError("model_update is required or use model_update = False")
            if not any((getattr(model_update, "__cwtch_model__", None), getattr(model_update, "__cwtch_view__", None))):
                raise ValueError("model_update is not cwtch model")

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
        if name in ("create", "create_many") and super().__getattribute__("model_create") is False:
            raise Exception("model_create is not defined")
        if name in ("save", "save_many") and super().__getattribute__("model_save") is False:
            raise Exception("model_save is not defined")
        if name in ("update", "update_many") and super().__getattribute__("model_update") is False:
            raise Exception("model_update is not defined")
        return super().__getattribute__(name)


class CRUD(metaclass=Meta):
    engine_name: str | None = None

    model_db: Type[DeclarativeBase] = typing.cast(Type[DeclarativeBase], None)

    model: Type = typing.cast(Type, None)
    model_create: Type | bool = typing.cast(Type, None)
    model_save: Type | bool = typing.cast(Type, None)
    model_update: Type | bool = typing.cast(Type, None)

    key = None
    index_elements: list | None = None

    order_by: list[OrderBy] | OrderBy | str | Callable = typing.cast(list[OrderBy] | OrderBy | str | Callable, None)

    joinedload: dict = {}

    _fields_create: set[str] = set()
    _fields_save: set[str] = set()
    _fields_update: set[str] = set()

    @classmethod
    async def _execute(cls, query) -> sa.ResultProxy:
        async with get_conn(engine_name=cls.engine_name) as conn:
            try:
                return await conn.execute(query)
            except Exception as e:
                _raise_exc(e)

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
    def _get_order_by(cls, c, order_by: list[OrderBy] | OrderBy | str | None = None) -> list | None:
        _order_by = order_by
        if _order_by is None:
            if callable(cls.order_by):
                _order_by = cls.order_by(c)
            else:
                _order_by = cls.order_by or cls._get_key()  # type: ignore
        if _order_by is not None:
            if not isinstance(_order_by, list):
                _order_by = [_order_by]
            else:
                _order_by = list(_order_by)
            for i, x in enumerate(_order_by):
                if isinstance(x, OrderBy):
                    x = OrderBy(by=x.by, order=x.order)  # type: ignore
                else:
                    x = OrderBy(by=x)  # type: ignore
                if isinstance(x.by, str):
                    if getattr(c, x.by, None) is None:
                        raise BadParamsError(f"invalid order_by '{x.by}'", param=f"{x.by}")
                    x.by = getattr(c, x.by)
                _order_by[i] = getattr(sa, x.order)(x.by)
        return typing.cast(list | None, _order_by)

    @classmethod
    def _make_from_db_data(
        cls,
        db_item,
        row: tuple | None = None,
        joinedload: dict | None = None,
    ) -> dict | None:
        return {}

    @classmethod
    def _from_db(
        cls,
        item: Row,
        data: dict | None = None,
        exclude: list | None = None,
        suffix: str | None = None,
        model_out=None,
    ) -> type:
        return from_attributes(
            model_out or cls.model,
            item,
            data=data,
            exclude=exclude,
            suffix=suffix,
            reset_circular_refs=True,
        )

    @classmethod
    def _get_values_for_create_query(cls, model) -> dict:
        return asdict(model, include=typing.cast(list[str], cls._fields_create))

    @classmethod
    def _make_create_query(cls, model, returning: bool | None = None):
        model_db = cls.model_db
        query = insert(model_db).values(cls._get_values_for_create_query(model))
        if returning:
            query = query.returning(model_db)
        return query

    @classmethod
    async def create(
        cls,
        model,
        model_out: Type | None = None,
        returning: bool | None = None,
    ):
        result = await cls._execute(cls._make_create_query(model, returning=returning))
        if returning:
            item = result.one()
            model_out = model_out or cls.model
            return cls._from_db(item, data=cls._make_from_db_data(item), model_out=model_out)

    @classmethod
    def _make_create_many_query(cls, models: list, returning: bool | None = None):
        model_db = cls.model_db
        query = insert(cls.model_db).values([cls._get_values_for_create_query(model) for model in models])
        if returning:
            query = query.returning(model_db)
        return query

    @classmethod
    async def create_many(
        cls,
        models: list,
        model_out: Type | None = None,
        returning: bool | None = None,
    ) -> list | None:
        if not models:
            return []
        result = await cls._execute(cls._make_create_many_query(models, returning=returning))
        if returning:
            from_db = cls._from_db
            make_from_db_data = cls._make_from_db_data
            model_out = model_out or cls.model
            return [from_db(item, data=make_from_db_data(item), model_out=model_out) for item in result.all()]

    @classmethod
    def _get_values_for_save_query(cls, model) -> dict:
        if isinstance(model, typing.cast(Type, cls.model_create)):
            return asdict(model, include=typing.cast(list[str], cls._fields_create))
        return asdict(model, include=typing.cast(list[str], cls._fields_save))

    @classmethod
    def _get_on_conflict_do_update_set_for_save_query(cls, excluded, model) -> dict:
        if isinstance(model, typing.cast(Type, cls.model_create)):
            return {k: getattr(excluded, k) for k in cls._create_fields}
        return {k: getattr(excluded, k) for k in cls._fields_save}

    @classmethod
    def _make_save_query(
        cls,
        model,
        index_elements: list | None = None,
        returning: bool | None = None,
    ):
        model_db = cls.model_db
        query = insert(model_db).values(cls._get_values_for_save_query(model))
        index_elements = index_elements or cls.index_elements
        if index_elements is None:
            raise ValueError("index_elements is None")
        index_elements = list(map(lambda e: isinstance(e, str) and getattr(model_db, e) or e, index_elements))
        query = query.on_conflict_do_update(
            index_elements=index_elements,
            set_=cls._get_on_conflict_do_update_set_for_save_query(query.excluded, model),
        )
        if returning:
            query = query.returning(model_db)
        return query

    @classmethod
    async def save(
        cls,
        model,
        index_elements: list | None = None,
        model_out: Type | None = None,
        returning: bool | None = None,
    ):
        result = await cls._execute(
            cls._make_save_query(
                model,
                index_elements=index_elements,
                returning=returning,
            )
        )
        if returning:
            item = result.one()
            model_out = model_out or cls.model
            return cls._from_db(item, data=cls._make_from_db_data(item), model_out=model_out)

    @classmethod
    def _make_save_many_query(
        cls,
        models: list,
        index_elements: list | None = None,
        returning: bool | None = None,
    ):
        model_db = cls.model_db
        query = insert(model_db).values([cls._get_values_for_save_query(model) for model in models])
        index_elements = index_elements or cls.index_elements
        if index_elements is None:
            raise ValueError("index_elements is None")
        index_elements = list(map(lambda e: isinstance(e, str) and getattr(model_db, e) or e, index_elements))
        query = query.on_conflict_do_update(
            index_elements=index_elements,
            set_=cls._get_on_conflict_do_update_set_for_save_query(query.excluded, models[0]),
        )
        if returning:
            query = query.returning(model_db)
        return query

    @classmethod
    async def save_many(
        cls,
        models: list,
        index_elements: list | None = None,
        model_out: Type | None = None,
        returning: bool | None = None,
    ) -> list | None:
        if not models:
            return []
        result = await cls._execute(
            cls._make_save_many_query(
                models,
                index_elements=index_elements,
                returning=returning,
            )
        )
        if returning:
            from_db = cls._from_db
            make_from_db_data = cls._make_from_db_data
            model_out = model_out or cls.model
            return [from_db(item, data=make_from_db_data(item), model_out=model_out) for item in result.all()]

    @classmethod
    def _get_values_for_update_query(cls, model) -> dict:
        return asdict(model, include=typing.cast(list[str], cls._fields_update), exclude_unset=True)

    @classmethod
    def make_update_query(cls, model, key: str, returning: bool | None = None):
        model_db = cls.model_db
        query = (
            update(model_db)
            .values(cls._get_values_for_update_query(model))
            .where(getattr(model_db, key) == getattr(model, key))
            .returning(getattr(model_db, key))
        )
        if returning:
            query = query.returning(model_db)
        return query

    @classmethod
    async def update(
        cls,
        model,
        key: str,
        model_out: Type | None = None,
        returning: bool | None = None,
        raise_not_found: bool | None = None,
    ):
        result = await cls._execute(cls.make_update_query(model, key, returning=returning or raise_not_found))
        if raise_not_found and result.rowcount == 0:
            raise NotFoundError(key=key, value=getattr(model, key))
        if returning:
            item = result.one_or_none()
            if item:
                model_out = model_out or cls.model
                return cls._from_db(item, data=cls._make_from_db_data(item), model_out=model_out)

    @classmethod
    async def update_many(
        cls,
        models: list,
        key: str,
        model_out: Type | None = None,
        returning: bool | None = None,
    ) -> list | None:
        async with transaction():
            results = await gather(
                *[cls.update(model, key, model_out=model_out, returning=returning) for model in models]
            )
            if returning:
                return typing.cast(list | None, results)

    @classmethod
    def _make_get_query(cls, key, key_value, **kwds):
        return select(cls.model_db).where(key == key_value)

    @classmethod
    async def get(
        cls,
        key_value,
        key=None,
        raise_not_found: bool | None = None,
        joinedload: dict | None = None,
        model_out: Type | None = None,
        **kwds,
    ):
        key = cls._get_key(key=key)
        query = cls._make_get_query(key, key_value, **kwds)
        model_out = model_out or cls.model

        if joinedload is None or not any(joinedload.values()):
            if item := (await cls._execute(query)).one_or_none():
                return cls._from_db(item, data=cls._make_from_db_data(item), model_out=model_out)
            if raise_not_found:
                raise NotFoundError(key=key, value=key_value)
            return

        exclude = []
        for k, v in cls.joinedload.items():
            if joinedload.get(k) is True:
                query = query.options(v(cls.model_db))
            else:
                exclude.append(k)

        async with get_sess(engine_name=cls.engine_name) as sess:
            if item := (await sess.execute(query)).unique().scalars().one_or_none():
                return cls._from_db(
                    item,
                    exclude=exclude,
                    data=cls._make_from_db_data(item, joinedload=joinedload),
                    model_out=model_out,
                )
            if raise_not_found:
                raise NotFoundError(key=key, value=key_value)

    @classmethod
    def _make_get_many_query(
        cls,
        order_by: list[OrderBy] | OrderBy | str | None = None,
        **kwds,
    ):
        query = select(func.count(literal("*")).over().label("rows_total"), cls.model_db)
        query = query.order_by(*cls._get_order_by(cls.model_db, order_by))
        return query

    @classmethod
    async def get_many(
        cls,
        page: int | None = None,
        page_size: int | None = None,
        order_by: list[OrderBy] | OrderBy | str | None = None,
        joinedload: dict | None = None,
        model_out: Type | None = None,
        **kwds,
    ) -> tuple[int, list]:
        model_db = cls.model_db
        model_out = model_out or cls.model

        query = cls._make_get_many_query(order_by=order_by, **kwds)

        cte = query.cte("cte")

        query = select(literal(1).label("i"), cte)

        if page_size:
            page = page or 1
            query = query.limit(page_size).offset((page - 1) * page_size)

        query = union_all(select(literal(0).label("i"), cte).limit(1), query)

        from_db = cls._from_db
        make_from_orm_data = cls._make_from_db_data

        if joinedload is None or not any(joinedload.values()):
            rows = (await cls._execute(query)).all()
            return rows[0].rows_total if rows else 0, [
                from_db(row, data=make_from_orm_data(row), model_out=model_out) for row in rows[1:]
            ]

        main_cte = query.cte("main_cte")

        model_db_alias = aliased(model_db, main_cte)  # type: ignore
        query = select(main_cte, model_db_alias)

        exclude = []
        for k, v in cls.joinedload.items():
            if joinedload.get(k) is True:
                query = query.options(v(model_db_alias))
            else:
                exclude.append(k)

        query = query.order_by(sa.asc(main_cte.c.i), *cls._get_order_by(main_cte.c, order_by))

        def _hash(row):
            return hash((row[0], row[1], row[-1]))

        async with get_sess(engine_name=cls.engine_name) as sess:
            rows = (await sess.execute(query)).unique(_hash).all()
            return rows[0].rows_total if rows else 0, [
                from_db(
                    row[-1],
                    exclude=exclude,
                    data=make_from_orm_data(row[-1], row=typing.cast(tuple, row), joinedload=joinedload),
                    model_out=model_out,
                )
                for row in rows[1:]
            ]

    @classmethod
    def _make_get_or_create_query(
        cls,
        model,
        update_element: str,
        index_elements: list | None = None,
    ):
        model_db = cls.model_db
        query = insert(model_db).values(cls._get_values_for_create_query(model))
        index_elements = index_elements or cls.index_elements
        if index_elements is None:
            raise ValueError("index_elements is None")
        index_elements = list(map(lambda e: isinstance(e, str) and getattr(model_db, e) or e, index_elements))
        return query.on_conflict_do_update(
            index_elements=index_elements,
            set_={update_element: getattr(query.excluded, update_element)},
        ).returning(model_db)

    @classmethod
    async def get_or_create(
        cls,
        model,
        update_element: str,
        index_elements: list | None = None,
        model_out: Type | None = None,
    ):
        model_out = model_out or cls.model
        item = (
            await cls._execute(
                cls._make_get_or_create_query(
                    model,
                    update_element,
                    index_elements=index_elements,
                )
            )
        ).one()
        return cls._from_db(item, data=cls._make_from_db_data(item), model_out=model_out)

    @classmethod
    def _make_delete_query(cls, key, key_value, returning: bool = True):
        query = delete(cls.model_db).where(key == key_value)
        if returning:
            query = query.returning(cls.model_db)
        return query

    @classmethod
    async def delete(
        cls,
        key_value,
        key=None,
        raise_not_found: bool | None = None,
        returning: bool | None = None,
        model_out: Type | None = None,
    ):
        key = cls._get_key(key=key)
        result = await cls._execute(cls._make_delete_query(key, key_value))
        if raise_not_found:
            if raise_not_found and result.rowcount == 0:
                raise NotFoundError(key=key, value=key_value)
        if returning:
            item = result.one_or_none()
            if item:
                model_out = model_out or cls.model
                return cls._from_db(item, data=cls._make_from_db_data(item), model_out=model_out)
