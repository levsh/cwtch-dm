# cwtch dm [wip]


## Examples

```python
from typing import ClassVar, Type

import sqlalchemy as sa

from cwtch import dataclass, resolve_types, view
from cwtch.types import UNSET, Unset
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import create_async_engine

from cwtch_dm import DM, NotFoundError, bind_engine


class BaseDB(DeclarativeBase):
    pass


class ParentDB(BaseDB):
    __tablename__ = "parents"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    data: Mapped[str]
    children = relationship("ChildDB", uselist=True, viewonly=True)


class ChildDB(BaseDB):
    __tablename__ = "children"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    parent_id: Mapped[int] = mapped_column(sa.ForeignKey("parents.id"))
    parent = relationship("ParentDB", uselist=False, viewonly=True)


@dataclass(handle_circular_refs=True)
class Parent:
    id: int
    name: str
    data: str
    children: Unset[list["Child"]] = UNSET

    # Parent views
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

    # Child views
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


class ParentDM(DM):
    model_db = ParentDB
    model = Parent
    model_create = Parent.Create
    model_save = Parent.Save
    model_update = Parent.Update
    key = "id"
    index_elements = ["id"]
    joinedload = {"children": lambda m: sa.orm.joinedload(m.children)}


class ChildDM(DM):
    model_db = ChildDB
    model = Child
    model_create = Child.Create
    model_save = Child.Save
    model_update = Child.Update
    key = "id"
    index_elements = ["id"]
    joinedload = {"parent": lambda m: sa.orm.joinedload(m.parent)}


@pytest_asyncio.fixture
async def create_all(engine):
    async with engine.begin() as conn:
        await conn.run_sync(BaseDB.metadata.create_all)


async def example(engine):
    engine = create_async_engine(...)

    async with engine.begin() as conn:
        await conn.run_sync(BaseDB.metadata.create_all)

    bind_engine(engine)

    parent = await ParentDM.create(Parent.Create(name=f"Parent_{i}", data="data"))

    # parents: list[Parent]
    total, parents = await ParentDM.get_many()

    # parents: list[Parent]
    total, parents = await ParentDM.get_many(page_size=1)

    # parents: list[Parent]
    total, parents = await ParentDM.get_many(page_size=1, page=2)

    # parent: Parent
    parent = await ParentDM.get(1)

    # parent: Parent
    parent = await ParentDM.get(1, joinedload={"children": True})

    # parent: Parent
    parent = await ParentDM.save(parent.Save())

    # parent: Parent
    parent = await ParentDM.update(Parent.Update(id=1, data="new data"), key="id")

    await ParentDM.delete(1)

    # child : Child 
    child = await ChildDM.create(Child.Create(name=f"Child_{i}", parent_id=i))

    # parent: Parent
    parent = await ParentDM.get(1, joinedload={"children": True})
```
