from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column

engine = create_async_engine(
    "sqlite+aiosqlite:///tasks.db"
)

new_session = async_sessionmaker(engine, expire_on_commit=False)

class Model(DeclarativeBase):
    pass

# Модель задачи
class Task(Model):
    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    parent_id: Mapped[int | None]
    executor: Mapped[str]
    deadline: Mapped[str]
    status: Mapped[str]

# Модель сотрудника
class Employee(Model):
    __tablename__ = "employees"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    position: Mapped[str]

# Функция создания таблиц
async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Model.metadata.drop_all)

# Функция удаления таблиц
async def delete_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Model.metadata.create_all)
