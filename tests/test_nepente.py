import re

import pytest
import pytest_asyncio
import sqlalchemy as sa
from cwtch import dataclass, field, resolve_types, view
from cwtch.types import UNSET, UnsetType
from sqlalchemy.orm import DeclarativeBase, relationship

from nepente import CRUD, NotFoundError, bind_engine


class DbBase(DeclarativeBase):
    pass


class DbParent(DbBase):
    __tablename__ = "parents"

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)
    data = sa.Column(sa.String, nullable=False)
    children = relationship("DbChild", uselist=True, viewonly=True)


class DbChild(DbBase):
    __tablename__ = "children"

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)
    parent_id = sa.Column(sa.Integer, sa.ForeignKey("parents.id"), nullable=False)
    parent = relationship("DbParent", uselist=False, viewonly=True)


@dataclass(handle_circular_refs=True)
class Parent:
    id: int = field()
    name: str = field()
    data: str = field()
    children: list["Child"] | UnsetType = field(default=UNSET)

    @view(exclude={"id", "children"})
    class Create:
        pass

    @view(exclude={"children"})
    class Save:
        pass

    @view(exclude={"children"})
    class Update:
        name: str | UnsetType = field(default=UNSET)
        data: str | UnsetType = field(default=UNSET)


@dataclass(handle_circular_refs=True)
class Child:
    id: int = field()
    name: str = field()
    parent_id: int = field()
    parent: Parent | UnsetType = field(default=UNSET)

    @view(exclude={"id", "parent"})
    class Create:
        pass

    @view(exclude={"parent"})
    class Save:
        pass

    @view(exclude={"parent"})
    class Update:
        name: str | UnsetType = field(default=UNSET)
        parent_id: int | UnsetType = field(default=UNSET)


resolve_types(Parent, globals(), locals())


@pytest_asyncio.fixture
async def create_all(engine):
    async with engine.begin() as conn:
        await conn.run_sync(DbBase.metadata.create_all)


@pytest.mark.asyncio
async def test_1(engine, create_all):
    bind_engine(engine)

    class ParentCRUD(CRUD):
        model_db = DbParent
        model = Parent
        model_create = Parent.Create
        model_save = Parent.Save
        model_update = Parent.Update
        key = "id"
        index_elements = ["id"]
        joinedload = {"children": sa.orm.joinedload(DbParent.children)}

    total, parents = await ParentCRUD.get_many()
    assert total == 0
    assert parents == []

    for i in range(1, 5):
        parent = await ParentCRUD.create(Parent.Create(name=f"Parent_{i}", data="data"))
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
    with pytest.raises(NotFoundError, match=re.escape(r"item with (DbParent.id)=(999) not found")):
        await ParentCRUD.get(999, raise_not_found=True)

    parent = await ParentCRUD.get(1, joinedload={"children": True})
    assert parent.id == 1
    assert parent.name == "Parent_1"
    assert parent.data == "data"
    assert parent.children == []

    parent.name = "Parent_1.1"
    parent = await ParentCRUD.save(parent.Save())
    assert parent.id == 1
    assert parent.name == "Parent_1.1"
    assert parent.data == "data"
    assert parent.children == UNSET

    parent = await ParentCRUD.update(Parent.Update(id=1, data="new data"), key="id")
    assert parent.id == 1
    assert parent.name == "Parent_1.1"
    assert parent.data == "new data"
    assert parent.children == UNSET

    assert await ParentCRUD.delete(4) == 1
    assert await ParentCRUD.get(4) is None
    assert await ParentCRUD.delete(4) == 0
    with pytest.raises(NotFoundError, match=re.escape(r"item with (DbParent.id)=(4) not found")):
        await ParentCRUD.delete(4, raise_not_found=True)

    parent = await ParentCRUD.delete(3, returning=True)
    assert parent.id == 3
    assert await ParentCRUD.get(3) is None

    class ChildCRUD(CRUD):
        model_db = DbChild
        model = Child
        model_create = Child.Create
        model_save = Child.Save
        model_update = Child.Update
        key = "id"
        index_elements = ["id"]
        joinedload = {"parent": sa.orm.joinedload(DbChild.parent)}

    for i in range(1, 3):
        child = await ChildCRUD.create(Child.Create(name=f"Child_{i}", parent_id=i))
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
