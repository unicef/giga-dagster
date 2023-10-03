import json
from base64 import b64decode, b64encode

from fastapi import Request, Response
from itsdangerous import TimestampSigner

from authproxy.settings import settings


def encode_session_cookie(value: dict) -> str:
    signer = TimestampSigner(settings.SECRET_KEY)
    return signer.sign(b64encode(json.dumps(value).encode("utf-8"))).decode("utf-8")


def decode_session_cookie(value: str) -> dict:
    signer = TimestampSigner(settings.SECRET_KEY)
    return json.loads(b64decode(signer.unsign(value.encode("utf-8"))))


def get_request_session(request: Request) -> dict:
    session = request.cookies.get("session")
    if session:
        session = decode_session_cookie(session)
    else:
        session = {}
    return session


def set_response_session(response: Response, session: dict):
    response.set_cookie(
        "session",
        encode_session_cookie(session),
        **settings.SESSION_COOKIE_PARAMS,
    )
