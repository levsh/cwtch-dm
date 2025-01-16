import typing

from typing import Optional, Type

import sqlalchemy as sa

from cwtch import asdict, from_attributes

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
        return from_attributes(
            model_out or cls.model,
            item,
            data=data,
            exclude=exclude,
            suffix=suffix,
            reset_circular_refs=True,
        )


class CreateDM(_CreateDM, skip_checks=True):
    @classmethod
    def _get_values_for_create_query(cls, model) -> dict:
        return asdict(model, include=typing.cast(list[str], cls._fields_create))


class SaveDM(_SaveDM, skip_checks=True):
    @classmethod
    def _get_values_for_save_query(cls, model) -> dict:
        if isinstance(model, typing.cast(Type, cls.model_create)):
            return asdict(model, include=typing.cast(list[str], cls._fields_create))
        return asdict(model, include=typing.cast(list[str], cls._fields_save))


class UpdateDM(_UpdateDM, skip_checks=True):
    @classmethod
    def _get_values_for_update_query(cls, model) -> dict:
        return asdict(model, include=typing.cast(list[str], cls._fields_update), exclude_unset=True)


class DeleteDM(_DeleteDM, skip_checks=True): ...


class DM(GetDM, CreateDM, SaveDM, UpdateDM, DeleteDM, skip_checks=True): ...
