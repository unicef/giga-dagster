from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base_database import BaseModel


class UserRoleAssociation(BaseModel):
    __tablename__ = "user_role_association_table"
    __table_args__ = (UniqueConstraint("user_id", "role_id", name="_user_role_uc"),)

    user_id: Mapped[str] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    role_id: Mapped[str] = mapped_column(
        ForeignKey("roles.id", ondelete="CASCADE"), nullable=False
    )


class User(BaseModel):
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(nullable=False, index=True, unique=True)
    given_name: Mapped[str] = mapped_column(nullable=True)
    surname: Mapped[str] = mapped_column(nullable=True)
    enabled: Mapped[bool] = mapped_column(default=True)
    roles: Mapped[set["Role"]] = relationship(
        secondary=UserRoleAssociation.__table__,
        back_populates="users",
    )


class Role(BaseModel):
    __tablename__ = "roles"

    name: Mapped[str] = mapped_column(nullable=False, index=True, unique=True)
    users: Mapped[set[User]] = relationship(
        secondary=UserRoleAssociation.__table__,
        back_populates="roles",
    )
