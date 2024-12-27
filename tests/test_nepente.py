import re

from typing import ClassVar, Type

import pytest
import pytest_asyncio
import sqlalchemy as sa

from cwtch import dataclass, resolve_types, view
from cwtch.types import UNSET, Unset
from sqlalchemy.orm import DeclarativeBase, relationship

from nepente import CRUD, NotFoundError, bind_engine


class BaseDB(DeclarativeBase):
    pass


class ParentDB(BaseDB):
    __tablename__ = "parents"

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)
    data = sa.Column(sa.String, nullable=False)
    children = relationship("ChildDB", uselist=True, viewonly=True)


class ChildDB(BaseDB):
    __tablename__ = "children"

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)
    parent_id = sa.Column(sa.Integer, sa.ForeignKey("parents.id"), nullable=False)
    parent = relationship("ParentDB", uselist=False, viewonly=True)


@dataclass(handle_circular_refs=True)
class Parent:
    id: int
    name: str
    data: str
    children: Unset[list["Child"]] = UNSET

    Create: ClassVar[Type["ParentCreate"]]
    Save: ClassVar[Type["ParentSave"]]
    Update: ClassVar[Type["ParentUpdate"]]


@view(Parent, "Create", exclude=["id", "children"])
class ParentCreate:
    pass


@view(Parent, "Save", exclude=["children"])
class ParentSave:
    pass


@view(Parent, "Update", exclude=["children"])
class ParentUpdate:
    name: Unset[str] = UNSET
    data: Unset[str] = UNSET


@dataclass(handle_circular_refs=True)
class Child:
    id: int
    name: str
    parent_id: int
    parent: Unset[Parent] = UNSET

    Create: ClassVar[Type["ChildCreate"]]
    Save: ClassVar[Type["ChildSave"]]
    Update: ClassVar[Type["ChildUpdate"]]


@view(Child, "Create", exclude=["id", "parent"])
class ChildCreate:
    pass


@view(Child, "Save", exclude=["parent"])
class ChildSave:
    pass


@view(Child, "Update", exclude=["parent"])
class ChildUpdate:
    name: Unset[str] = UNSET
    parent_id: Unset[int] = UNSET


resolve_types(Parent, globals(), locals())


@pytest_asyncio.fixture
async def create_all(engine):
    async with engine.begin() as conn:
        await conn.run_sync(BaseDB.metadata.create_all)


@pytest.mark.asyncio
async def test_1(engine, create_all):
    bind_engine(engine)

    class ParentCRUD(CRUD):
        model_db = ParentDB
        model = Parent
        model_create = Parent.Create
        model_save = Parent.Save
        model_update = Parent.Update
        key = "id"
        index_elements = ["id"]
        joinedload = {"children": lambda m: sa.orm.joinedload(m.children)}

    total, parents = await ParentCRUD.get_many()
    assert total == 0
    assert parents == []

    for i in range(1, 5):
        parent = await ParentCRUD.create(Parent.Create(name=f"Parent_{i}", data="data"), returning=True)
        assert parent.id == i
        assert parent.name == f"Parent_{i}"
        assert parent.data == "data"
        assert parent.children == UNSET

    total, parents = await ParentCRUD.get_many()
    assert total == 4
    assert parents == [
        Parent(id=1, name="Parent_1", data="data"),
        Parent(id=2, name="Parent_2", data="data"),
        Parent(id=3, name="Parent_3", data="data"),
        Parent(id=4, name="Parent_4", data="data"),
    ]

    total, parents = await ParentCRUD.get_many(page_size=1)
    assert total == 4
    assert parents == [
        Parent(id=1, name="Parent_1", data="data"),
    ]

    total, parents = await ParentCRUD.get_many(page_size=1, page=2)
    assert total == 4
    assert parents == [
        Parent(id=2, name="Parent_2", data="data"),
    ]

    parent = await ParentCRUD.get(1)
    assert parent.id == 1
    assert parent.name == "Parent_1"
    assert parent.data == "data"
    assert parent.children == UNSET

    assert await ParentCRUD.get(999) is None
    with pytest.raises(NotFoundError, match=re.escape(r"item with (ParentDB.id)=(999) not found")):
        await ParentCRUD.get(999, raise_not_found=True)

    parent = await ParentCRUD.get(1, joinedload={"children": True})
    assert parent.id == 1
    assert parent.name == "Parent_1"
    assert parent.data == "data"
    assert parent.children == []

    parent.name = "Parent_1.1"
    parent = await ParentCRUD.save(parent.Save(), returning=True)
    assert parent.id == 1
    assert parent.name == "Parent_1.1"
    assert parent.data == "data"
    assert parent.children == UNSET

    parent = await ParentCRUD.update(Parent.Update(id=1, data="new data"), key="id", returning=True)
    assert parent.id == 1
    assert parent.name == "Parent_1.1"
    assert parent.data == "new data"
    assert parent.children == UNSET

    await ParentCRUD.delete(4)
    assert await ParentCRUD.get(4) is None
    await ParentCRUD.delete(4)
    with pytest.raises(NotFoundError, match=re.escape(r"item with (ParentDB.id)=(4) not found")):
        await ParentCRUD.delete(4, raise_not_found=True)

    parent = await ParentCRUD.delete(3, returning=True)
    assert parent.id == 3
    assert await ParentCRUD.get(3) is None

    class ChildCRUD(CRUD):
        model_db = ChildDB
        model = Child
        model_create = Child.Create
        model_save = Child.Save
        model_update = Child.Update
        key = "id"
        index_elements = ["id"]
        joinedload = {"parent": lambda m: sa.orm.joinedload(m.parent)}

    for i in range(1, 3):
        child = await ChildCRUD.create(Child.Create(name=f"Child_{i}", parent_id=i), returning=True)
        assert child.id == i
        assert child.name == f"Child_{i}"
        assert child.parent_id == i
        assert child.parent == UNSET

    child = await ChildCRUD.get(1)
    assert child.id == 1
    assert child.name == "Child_1"
    assert child.parent_id == 1
    assert child.parent == UNSET

    parent = await ParentCRUD.get(1, joinedload={"children": True})
    assert child.id == 1
    assert child.name == "Child_1"
    assert child.parent_id == 1
    assert parent.children == [Child(id=1, name="Child_1", parent_id=1, parent=UNSET)]
