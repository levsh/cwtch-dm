import re

from typing import ClassVar, Type, cast

import pytest
import pytest_asyncio
import sqlalchemy as sa

from cwtch import dataclass, resolve_types, view
from cwtch.types import UNSET, Unset
from pydantic import BaseModel
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from dbdm import NotFoundError, bind_engine


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


@pytest_asyncio.fixture
async def create_all(engine):
    async with engine.begin() as conn:
        await conn.run_sync(BaseDB.metadata.create_all)


@pytest.mark.asyncio
async def test_cwtch(engine, create_all):
    from dbdm.cwtch import DM

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

    bind_engine(engine)

    class ParentDM(DM):
        model_db: Type[DeclarativeBase] = ParentDB
        model = Parent
        model_create = Parent.Create
        model_save = Parent.Save
        model_update = Parent.Update
        key = "id"
        index_elements = ["id"]
        joinedload = {"children": lambda m: sa.orm.joinedload(m.children)}

    total, parents = await ParentDM.get_many()
    assert total == 0
    assert parents == []

    for i in range(1, 5):
        parent = cast(Parent, await ParentDM.create(Parent.Create(name=f"Parent_{i}", data="data"), returning=True))
        assert parent.id == i
        assert parent.name == f"Parent_{i}"
        assert parent.data == "data"
        assert parent.children == UNSET

    total, parents = await ParentDM.get_many()
    assert total == 4
    assert parents == [
        Parent(id=1, name="Parent_1", data="data"),
        Parent(id=2, name="Parent_2", data="data"),
        Parent(id=3, name="Parent_3", data="data"),
        Parent(id=4, name="Parent_4", data="data"),
    ]

    total, parents = await ParentDM.get_many(page_size=1)
    assert total == 4
    assert parents == [
        Parent(id=1, name="Parent_1", data="data"),
    ]

    total, parents = await ParentDM.get_many(page_size=1, page=2)
    assert total == 4
    assert parents == [
        Parent(id=2, name="Parent_2", data="data"),
    ]

    total, parents = await ParentDM.get_many(flt=ParentDM.model_db.name == "Parent_2")
    assert total == 1
    assert parents == [
        Parent(id=2, name="Parent_2", data="data"),
    ]

    parent = cast(Parent, await ParentDM.get(1))
    assert parent.id == 1
    assert parent.name == "Parent_1"
    assert parent.data == "data"
    assert parent.children == UNSET

    assert await ParentDM.get(999) is None
    with pytest.raises(NotFoundError, match=re.escape(r"item with (ParentDB.id)=(999) not found")):
        await ParentDM.get(999, raise_not_found=True)

    parent = cast(Parent, await ParentDM.get(1, joinedload={"children": True}))
    assert parent.id == 1
    assert parent.name == "Parent_1"
    assert parent.data == "data"
    assert parent.children == []

    parent.name = "Parent_1.1"
    parent = cast(Parent, await ParentDM.save(parent.Save(), returning=True))
    assert parent.id == 1
    assert parent.name == "Parent_1.1"
    assert parent.data == "data"
    assert parent.children == UNSET

    parent = cast(Parent, await ParentDM.update(Parent.Update(id=1, data="new data"), key="id", returning=True))
    assert parent.id == 1
    assert parent.name == "Parent_1.1"
    assert parent.data == "new data"
    assert parent.children == UNSET

    await ParentDM.delete(4)
    assert await ParentDM.get(4) is None
    await ParentDM.delete(4)
    with pytest.raises(NotFoundError, match=re.escape(r"item with (ParentDB.id)=(4) not found")):
        await ParentDM.delete(4, raise_not_found=True)

    parent = cast(Parent, await ParentDM.delete(3, returning=True))
    assert parent.id == 3
    assert await ParentDM.get(3) is None

    class ChildDM(DM):
        model_db: Type[DeclarativeBase] = ChildDB
        model = Child
        model_create = Child.Create
        model_save = Child.Save
        model_update = Child.Update
        key = "id"
        index_elements = ["id"]
        joinedload = {"parent": lambda m: sa.orm.joinedload(m.parent)}

    for i in range(1, 3):
        child = cast(Child, await ChildDM.create(Child.Create(name=f"Child_{i}", parent_id=i), returning=True))
        assert child.id == i
        assert child.name == f"Child_{i}"
        assert child.parent_id == i
        assert child.parent == UNSET

    child = cast(Child, await ChildDM.get(1))
    assert child.id == 1
    assert child.name == "Child_1"
    assert child.parent_id == 1
    assert child.parent == UNSET

    parent = cast(Parent, await ParentDM.get(1, joinedload={"children": True}))
    assert parent.children == [Child(id=1, name="Child_1", parent_id=1, parent=UNSET)]


@pytest.mark.asyncio
async def test_pydantic(engine, create_all):
    from dbdm.pydantic import DM

    class Parent(BaseModel):
        id: int
        name: str
        data: str
        children: list["Child"] = None

    class ParentCreate(BaseModel):
        name: str
        data: str

    class ParentSave(BaseModel):
        id: int
        name: str
        data: str

    class ParentUpdate(BaseModel):
        id: int
        name: str = None
        data: str = None

    class Child(BaseModel):
        id: int
        name: str
        parent_id: int
        parent: Parent = None

    class ChildCreate(BaseModel):
        name: str
        parent_id: int

    class ChildSave(BaseModel):
        id: int
        name: str
        parent_id: int

    class ChildUpdate(BaseModel):
        id: int
        name: str = None
        parent_id: int = None

    Parent.model_rebuild()

    bind_engine(engine)

    class ParentDM(DM):
        model_db: Type[DeclarativeBase] = ParentDB
        model = Parent
        model_create = ParentCreate
        model_save = ParentSave
        model_update = ParentUpdate
        key = "id"
        index_elements = ["id"]
        joinedload = {"children": lambda m: sa.orm.joinedload(m.children)}

        @classmethod
        def _make_from_db_data(
            cls,
            db_item,
            row: tuple | None = None,
            joinedload: dict | None = None,
        ) -> dict:
            if joinedload and "children" in joinedload and db_item.children:
                res = {
                    "children": [
                        {k: v for k, v in child.__dict__.items() if k not in ["_sa_instance_state", "parent"]}
                        for child in db_item.children
                    ]
                }
                return res
            return super()._make_from_db_data(db_item, row=row, joinedload=joinedload)

    total, parents = await ParentDM.get_many()
    assert total == 0
    assert parents == []

    for i in range(1, 5):
        parent = cast(Parent, await ParentDM.create(ParentCreate(name=f"Parent_{i}", data="data"), returning=True))
        assert parent.id == i
        assert parent.name == f"Parent_{i}"
        assert parent.data == "data"
        assert parent.children is None

    total, parents = await ParentDM.get_many()
    assert total == 4
    assert parents == [
        Parent(id=1, name="Parent_1", data="data"),
        Parent(id=2, name="Parent_2", data="data"),
        Parent(id=3, name="Parent_3", data="data"),
        Parent(id=4, name="Parent_4", data="data"),
    ]

    total, parents = await ParentDM.get_many(page_size=1)
    assert total == 4
    assert parents == [
        Parent(id=1, name="Parent_1", data="data"),
    ]

    total, parents = await ParentDM.get_many(page_size=1, page=2)
    assert total == 4
    assert parents == [
        Parent(id=2, name="Parent_2", data="data"),
    ]

    total, parents = await ParentDM.get_many(flt=ParentDM.model_db.name == "Parent_2")
    assert total == 1
    assert parents == [
        Parent(id=2, name="Parent_2", data="data"),
    ]

    parent = cast(Parent, await ParentDM.get(1))
    assert parent.id == 1
    assert parent.name == "Parent_1"
    assert parent.data == "data"
    assert parent.children is None

    assert await ParentDM.get(999) is None
    with pytest.raises(NotFoundError, match=re.escape(r"item with (ParentDB.id)=(999) not found")):
        await ParentDM.get(999, raise_not_found=True)

    parent = cast(Parent, await ParentDM.get(1, joinedload={"children": True}))
    assert parent.id == 1
    assert parent.name == "Parent_1"
    assert parent.data == "data"
    assert parent.children == []

    parent.name = "Parent_1.1"
    parent = cast(Parent, await ParentDM.save(ParentSave.model_validate(parent, from_attributes=True), returning=True))
    assert parent.id == 1
    assert parent.name == "Parent_1.1"
    assert parent.data == "data"
    assert parent.children is None

    parent = cast(Parent, await ParentDM.update(ParentUpdate(id=1, data="new data"), key="id", returning=True))
    assert parent.id == 1
    assert parent.name == "Parent_1.1"
    assert parent.data == "new data"
    assert parent.children is None

    await ParentDM.delete(4)
    assert await ParentDM.get(4) is None
    await ParentDM.delete(4)
    with pytest.raises(NotFoundError, match=re.escape(r"item with (ParentDB.id)=(4) not found")):
        await ParentDM.delete(4, raise_not_found=True)

    parent = cast(Parent, await ParentDM.delete(3, returning=True))
    assert parent.id == 3
    assert await ParentDM.get(3) is None

    class ChildDM(DM):
        model_db: Type[DeclarativeBase] = ChildDB
        model = Child
        model_create = ChildCreate
        model_save = ChildSave
        model_update = ChildUpdate
        key = "id"
        index_elements = ["id"]
        joinedload = {"parent": lambda m: sa.orm.joinedload(m.parent)}

    for i in range(1, 3):
        child = cast(Child, await ChildDM.create(ChildCreate(name=f"Child_{i}", parent_id=i), returning=True))
        assert child.id == i
        assert child.name == f"Child_{i}"
        assert child.parent_id == i
        assert child.parent is None

    child = cast(Child, await ChildDM.get(1))
    assert child.id == 1
    assert child.name == "Child_1"
    assert child.parent_id == 1
    assert child.parent is None

    parent = cast(Parent, await ParentDM.get(1, joinedload={"children": True}))
    assert parent.children == [Child(id=1, name="Child_1", parent_id=1)]
