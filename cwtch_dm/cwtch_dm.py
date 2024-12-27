import re
import typing

from asyncio import gather
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Callable, Literal, Never, Optional, Protocol, Type

import sqlalchemy as sa

from cwtch import asdict, dataclass, field, from_attributes
from sqlalchemy import Row, delete, func, literal, select, union_all, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, aliased


__all__ = [
    "DMError",
    "AlreadyExistsError",
    "BadParamsError",
    "NotFoundError",
    "OrderBy",
    "bind_engine",
    "transaction",
    "GetDM",
    "CreateDM",
    "SaveDM",
    "UpdateDM",
    "DeleteDM",
    "DM",
]


_conn: dict[str | None, ContextVar] = {}
_engine: dict[str | None, sa.ext.asyncio.AsyncEngine] = {}


@dataclass
class OrderBy:
    by: Any = field(validate=False)
    order: Literal["asc", "desc"] = "asc"


class DMError(Exception):
    pass


class BadParamsError(DMError):
    def __init__(self, message: str, param: Optional[str] = None):
        self.message = message
        self.param = param

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.__str__()


class NotFoundError(DMError):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __str__(self):
        return f"item with ({self.key})=({self.value}) not found"


class AlreadyExistsError(DMError):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __str__(self):
        return f"key ({self.key})=({self.value}) already exists"


def bind_engine(engine: sa.ext.asyncio.AsyncEngine, name: Optional[str] = None):
    _engine[name] = engine
    _conn[name] = ContextVar("conn", default=None)


@asynccontextmanager
async def transaction(engine_name: Optional[str] = None) -> AsyncIterator[sa.ext.asyncio.AsyncConnection]:
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
async def get_conn(engine_name: Optional[str] = None) -> AsyncIterator[sa.ext.asyncio.AsyncConnection]:
    if (conn := _conn[engine_name].get()) is None:
        async with _engine[engine_name].connect() as conn:
            async with conn.begin():
                yield conn
    else:
        yield conn


@asynccontextmanager
async def get_sess(engine_name: Optional[str] = None):
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


class Meta(typing._ProtocolMeta):
    def __new__(cls, name, bases, ns, skip_checks: bool = False):
        if not skip_checks:

            def get_all_bases(bases: tuple) -> set:
                result = set()
                for base in bases:
                    result.add(base)
                    result |= get_all_bases(getattr(base, "__bases__", ()))
                return result

            all_bases = get_all_bases(bases)

            if GetDM not in all_bases:
                raise TypeError(f"Any mapper class should be subclassed from {GetDM}")

            if (model_db := ns.get("model_db")) is None:
                raise ValueError("`model_db` field is required")

            if (model := ns.get("model")) is None:
                raise ValueError("`model` field is required")

            if not any((getattr(model, "__cwtch_model__", None), getattr(model, "__cwtch_view__", None))):
                raise ValueError("`model` is not cwtch model")

            if CreateDM in all_bases or SaveDM in all_bases:
                if (model_create := ns.get("model_create")) is None:
                    raise ValueError("`model_create` field is required")
                if not any(
                    (
                        getattr(model_create, "__cwtch_model__", None),
                        getattr(model_create, "__cwtch_view__", None),
                    )
                ):
                    raise ValueError("`model_create` is not cwtch model")
                ns["_fields_create"] = set(model_db.__table__.columns.keys()) & set(model_create.__dataclass_fields__)
                assert ns["_fields_create"]  # TODO

            if SaveDM in all_bases:
                if (model_save := ns.get("model_save")) is None:
                    raise ValueError("`model_save` field is required")
                if not any(
                    (
                        getattr(model_save, "__cwtch_model__", None),
                        getattr(model_save, "__cwtch_view__", None),
                    )
                ):
                    raise ValueError("`model_save` is not cwtch model")
                ns["_fields_save"] = set(model_db.__table__.columns.keys()) & set(model_save.__dataclass_fields__)
                assert ns["_fields_save"]  # TODO

            if UpdateDM in all_bases:
                if (model_update := ns.get("model_update")) is None:
                    raise ValueError("`model_update` field is required")
                if not any(
                    (
                        getattr(model_update, "__cwtch_model__", None),
                        getattr(model_update, "__cwtch_view__", None),
                    )
                ):
                    raise ValueError("`model_update` is not cwtch model")
                ns["_fields_update"] = set(model_db.__table__.columns.keys()) & set(model_update.__dataclass_fields__)
                assert ns["_fields_update"]  # TODO

        return super().__new__(cls, name, bases, ns)


class _BaseProtocol(Protocol):
    engine_name: Optional[str]

    model_db: Type[DeclarativeBase]
    model: Type

    @classmethod
    async def _execute(cls, query) -> sa.ResultProxy: ...

    @classmethod
    def _make_from_db_data(
        cls,
        db_item,
        row: Optional[tuple] = None,
        joinedload: Optional[dict] = None,
    ) -> Optional[dict]: ...

    @classmethod
    def _from_db(
        cls,
        item: Row | DeclarativeBase,
        data: Optional[dict] = None,
        exclude: Optional[list] = None,
        suffix: Optional[str] = None,
        model_out=None,
    ): ...

    @classmethod
    def _get_key(cls, key=None): ...

    @classmethod
    async def get(
        cls,
        key_value,
        key=None,
        raise_not_found: Optional[bool] = None,
        joinedload: Optional[dict] = None,
        model_out: Optional[Type] = None,
        **kwds,
    ) -> Optional[type]: ...

    @classmethod
    async def get_many(
        cls,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        order_by: Optional[list[OrderBy | str] | OrderBy | str] = None,
        joinedload: Optional[dict] = None,
        model_out: Optional[Type] = None,
        **kwds,
    ) -> tuple[int, list]: ...


class GetDM(_BaseProtocol, metaclass=Meta, skip_checks=True):
    engine_name: Optional[str] = None

    model_db: Type[DeclarativeBase] = typing.cast(Type[DeclarativeBase], None)
    model: Type = typing.cast(Type, None)

    key = None
    order_by: Optional[list[OrderBy | str | Callable] | OrderBy | str | Callable] = None

    joinedload: dict = {}

    @classmethod
    async def _execute(cls, query) -> sa.ResultProxy:
        async with get_conn(engine_name=cls.engine_name) as conn:
            try:
                return await conn.execute(query)
            except Exception as e:
                _raise_exc(e)

    @classmethod
    def _make_from_db_data(
        cls,
        db_item,
        row: Optional[tuple] = None,
        joinedload: Optional[dict] = None,
    ) -> dict | None:
        return {}

    @classmethod
    def _from_db(
        cls,
        item: Row | DeclarativeBase,
        data: Optional[dict] = None,
        exclude: Optional[list] = None,
        suffix: Optional[str] = None,
        model_out=None,
    ):
        return from_attributes(
            model_out or cls.model,
            item,
            data=data,
            exclude=exclude,
            suffix=suffix,
            reset_circular_refs=True,
        )

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
    def _get_order_by(cls, c, order_by: Optional[list[OrderBy | str] | OrderBy | str] = None) -> list:
        _order_by = order_by
        if _order_by is None:
            if callable(cls.order_by):
                _order_by = cls.order_by(c)
            else:
                _order_by = cls.order_by or cls._get_key()
        if _order_by is not None:
            if not isinstance(_order_by, list):
                _order_by = [_order_by]
            else:
                _order_by = list(_order_by)
            for i, x in enumerate(_order_by):
                if isinstance(x, OrderBy):
                    x = OrderBy(by=x.by, order=x.order)
                else:
                    x = OrderBy(by=x)
                if isinstance(x.by, str):
                    if getattr(c, x.by, None) is None:
                        raise BadParamsError(f"invalid order_by '{x.by}'", param=f"{x.by}")
                    x.by = getattr(c, x.by)
                _order_by[i] = getattr(sa, x.order)(x.by)
        return typing.cast(list, _order_by)

    @classmethod
    def _make_get_query(cls, key, key_value, **kwds) -> sa.sql.Select:
        return select(cls.model_db).where(key == key_value)

    @classmethod
    async def get(
        cls,
        key_value,
        key=None,
        raise_not_found: Optional[bool] = None,
        joinedload: Optional[dict] = None,
        model_out: Optional[Type] = None,
        **kwds,
    ) -> Optional[type]:
        key = cls._get_key(key=key)
        query = cls._make_get_query(key, key_value, **kwds)
        model_out = model_out or cls.model

        if joinedload is None or not any(joinedload.values()):
            item = (await cls._execute(query)).one_or_none()
            if item:
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
            item = (await sess.execute(query)).unique().scalars().one_or_none()
            if item:
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
        order_by: Optional[list[OrderBy | str] | OrderBy | str] = None,
        **kwds,
    ) -> sa.sql.Select:
        query = select(func.count(literal("*")).over().label("rows_total"), cls.model_db)
        query = query.order_by(*cls._get_order_by(cls.model_db, order_by))
        return query

    @classmethod
    async def get_many(
        cls,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        order_by: Optional[list[OrderBy | str] | OrderBy | str] = None,
        joinedload: Optional[dict] = None,
        model_out: Optional[Type] = None,
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

        model_db_alias = aliased(model_db, main_cte)
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


class CreateDM(_BaseProtocol, metaclass=Meta, skip_checks=True):
    model_create: Type = typing.cast(Type, None)

    index_elements: Optional[list] = None

    _fields_create: set[str] = set()

    @classmethod
    def _get_values_for_create_query(cls, model) -> dict:
        return asdict(model, include=typing.cast(list[str], cls._fields_create))

    @classmethod
    def _make_create_query(cls, model, returning: Optional[bool] = None) -> sa.sql.Insert:
        model_db = cls.model_db
        query = insert(model_db).values(cls._get_values_for_create_query(model))
        if returning:
            query = query.returning(model_db)
        return query

    @classmethod
    async def create(
        cls,
        model,
        model_out: Optional[Type] = None,
        returning: bool = True,
    ):
        result = await cls._execute(cls._make_create_query(model, returning=returning))
        if returning:
            item = result.one()
            model_out = model_out or cls.model
            return cls._from_db(item, data=cls._make_from_db_data(item), model_out=model_out)

    @classmethod
    def _make_create_many_query(cls, models: list, returning: bool | None = None) -> sa.sql.Insert:
        model_db = cls.model_db
        query = insert(cls.model_db).values([cls._get_values_for_create_query(model) for model in models])
        if returning:
            query = query.returning(model_db)
        return query

    @classmethod
    async def create_many(
        cls,
        models: list,
        model_out: Optional[Type] = None,
        returning: bool = True,
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
    def _make_get_or_create_query(
        cls,
        model,
        update_element: str,
        index_elements: Optional[list] = None,
    ) -> sa.sql.Insert:
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
        index_elements: Optional[list] = None,
        model_out: Optional[Type] = None,
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


class SaveDM(_BaseProtocol, metaclass=Meta, skip_checks=True):
    model_create: Type = typing.cast(Type, None)
    model_save: Type = typing.cast(Type, None)

    index_elements: Optional[list] = None

    _fields_create: set[str] = set()
    _fields_save: set[str] = set()

    @classmethod
    def _get_values_for_save_query(cls, model) -> dict:
        if isinstance(model, typing.cast(Type, cls.model_create)):
            return asdict(model, include=typing.cast(list[str], cls._fields_create))
        return asdict(model, include=typing.cast(list[str], cls._fields_save))

    @classmethod
    def _get_on_conflict_do_update_set_for_save_query(cls, excluded, model) -> dict:
        if isinstance(model, typing.cast(Type, cls.model_create)):
            return {k: getattr(excluded, k) for k in cls._fields_create}
        return {k: getattr(excluded, k) for k in cls._fields_save}

    @classmethod
    def _make_save_query(
        cls,
        model,
        index_elements: Optional[list] = None,
        returning: Optional[bool] = None,
    ) -> sa.sql.Insert:
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
        index_elements: Optional[list] = None,
        model_out: Optional[Type] = None,
        returning: bool = True,
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
        index_elements: Optional[list] = None,
        returning: Optional[bool] = None,
    ) -> sa.sql.Insert:
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
        index_elements: Optional[list] = None,
        model_out: Optional[Type] = None,
        returning: bool = True,
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


class UpdateDM(_BaseProtocol, metaclass=Meta, skip_checks=True):
    model_update: Type = typing.cast(Type, None)
    _fields_update: set[str] = set()

    @classmethod
    def _get_values_for_update_query(cls, model) -> dict:
        return asdict(model, include=typing.cast(list[str], cls._fields_update), exclude_unset=True)

    @classmethod
    def _make_update_query(cls, model, key: str, returning: Optional[bool] = None) -> sa.sql.Update:
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
        raise_not_found: Optional[bool] = None,
        model_out: Optional[Type] = None,
        returning: bool = True,
    ):
        result = await cls._execute(cls._make_update_query(model, key, returning=returning or raise_not_found))
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
        model_out: Optional[Type] = None,
        returning: bool = True,
    ) -> list | None:
        async with transaction():
            results = await gather(
                *[cls.update(model, key, model_out=model_out, returning=returning) for model in models]
            )
            if returning:
                return typing.cast(list | None, results)


class DeleteDM(_BaseProtocol, metaclass=Meta, skip_checks=True):
    @classmethod
    def _make_delete_query(cls, key, key_value, returning: Optional[bool] = None):
        query = delete(cls.model_db).where(key == key_value)
        if returning:
            query = query.returning(cls.model_db)
        return query

    @classmethod
    async def delete(
        cls,
        key_value,
        key=None,
        raise_not_found: Optional[bool] = None,
        model_out: Optional[Type] = None,
        returning: bool = True,
    ):
        key = cls._get_key(key=key)
        result = await cls._execute(cls._make_delete_query(key, key_value, returning=returning))
        if raise_not_found:
            if raise_not_found and result.rowcount == 0:
                raise NotFoundError(key=key, value=key_value)
        if returning:
            item = result.one_or_none()
            if item:
                model_out = model_out or cls.model
                return cls._from_db(item, data=cls._make_from_db_data(item), model_out=model_out)


class DM(GetDM, CreateDM, SaveDM, UpdateDM, DeleteDM, skip_checks=True): ...
