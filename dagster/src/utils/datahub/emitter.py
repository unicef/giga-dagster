from datahub.emitter.rest_emitter import DatahubRestEmitter

from src.settings import settings


def get_rest_emitter():
    return DatahubRestEmitter(
        gms_server=settings.DATAHUB_METADATA_SERVER_URL,
        token=settings.DATAHUB_ACCESS_TOKEN,
        retry_max_times=5,
    )
