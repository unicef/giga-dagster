from fastapi import HTTPException, Request, status

from authproxy.lib.auth import get_auth


def is_authenticated(request: Request):
    if not get_auth(request.session).get_user():
        raise HTTPException(
            detail="Authentication credentials were not provided.",
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
