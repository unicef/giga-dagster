import country_converter as coco
from models.users import Role, User, UserRoleAssociation
from sqlalchemy import select

from src.utils.db.primary import get_db_context


class GroupsApi:
    @classmethod
    def list_role_members(cls, role: str) -> list[str]:
        with get_db_context() as db:
            users = db.scalars(
                select(User)
                .join(UserRoleAssociation)
                .join(Role)
                .where(Role.name == role)
            )

            emails = [user.email for user in users]
            return emails

    @classmethod
    def list_country_role_members(cls, country_code: str) -> list[str]:
        with get_db_context() as db:
            full_country_name = coco.convert(names=[country_code], to="name_short")
            users = db.scalars(
                select(User.email.distinct())
                .join(UserRoleAssociation)
                .join(Role)
                .where(Role.name.like(f"{full_country_name}-%"))
            )
            return list(users)
