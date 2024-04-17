from datahub.emitter.mce_builder import make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DomainPropertiesClass

from src.settings import settings


def create_domains() -> list[str]:
    domains = ["Geospatial", "Infrastructure", "School", "Finance"]

    for domain in domains:
        domain_urn = make_domain_urn(domain)
        domain_properties_aspect = DomainPropertiesClass(name=domain, description="")

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=domain_urn,
            aspect=domain_properties_aspect,
        )

        datahub_emitter = DatahubRestEmitter(
            gms_server=settings.DATAHUB_METADATA_SERVER_URL,
            token=settings.DATAHUB_ACCESS_TOKEN,
        )

        datahub_emitter.emit(event)

    return domains
