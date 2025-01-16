import typing

from typing import Optional, Type

import pydantic  # noqa: F401 # type: ignore
import sqlalchemy as sa

from .common import ModelDB, OrderBy, _CreateDM, _DeleteDM, _GetDM, _SaveDM, _UpdateDM


__all__ = [
    "OrderBy",
    "GetDM",
    "CreateDM",
    "SaveDM",
    "UpdateDM",
    "DeleteDM",
    "DM",
]


class _object:
    pass


class GetDM(_GetDM, skip_checks=True):
    @classmethod
    def _from_db(
        cls,
        item: sa.Row | ModelDB,
        data: Optional[dict] = None,
        exclude: Optional[list] = None,
        suffix: Optional[str] = None,
        model_out=None,
    ):
        if isinstance(item, sa.Row):
            data = {**item._asdict(), **(data or {})}
        else:
            data = {**item.__dict__, **(data or {})}

        obj = _object()

        if suffix:
            obj.__dict__ = {
                k.rstrip(suffix): v for k, v in data.items() if (not exclude or k not in exclude) and k.endswith(suffix)
            }
        else:
            obj.__dict__ = {k: v for k, v in data.items() if not exclude or k not in exclude}

        return (model_out or cls.model).model_validate(obj, from_attributes=True)


class CreateDM(_CreateDM, skip_checks=True):
    @classmethod
    def _get_values_for_create_query(cls, model) -> dict:
        return model.model_dump(include=typing.cast(list[str], cls._fields_create))


class SaveDM(_SaveDM, skip_checks=True):
    @classmethod
    def _get_values_for_save_query(cls, model) -> dict:
        if isinstance(model, typing.cast(Type, cls.model_create)):
            return model.model_dump(include=typing.cast(list[str], cls._fields_create))
        return model.model_dump(include=typing.cast(list[str], cls._fields_save))


class UpdateDM(_UpdateDM, skip_checks=True):
    @classmethod
    def _get_values_for_update_query(cls, model) -> dict:
        return model.model_dump(include=typing.cast(list[str], cls._fields_update), exclude_unset=True)


class DeleteDM(_DeleteDM, skip_checks=True): ...


class DM(GetDM, CreateDM, SaveDM, UpdateDM, DeleteDM, skip_checks=True): ...
