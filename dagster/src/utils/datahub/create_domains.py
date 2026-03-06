from datahub.emitter.mce_builder import make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DomainPropertiesClass
from loguru import logger

from src.utils.datahub.emit_dataset_metadata import get_datahub_emitter


def create_domains() -> list[str]:
    domains = ["Geospatial", "Infrastructure", "School", "Finance"]

    datahub_emitter = get_datahub_emitter()
    if datahub_emitter is None:
        logger.warning("DataHub is not configured. Skipping domain creation.")
        return []

    for domain in domains:
        domain_urn = make_domain_urn(domain)
        domain_properties_aspect = DomainPropertiesClass(name=domain, description="")

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn=domain_urn,
            aspect=domain_properties_aspect,
        )

        datahub_emitter.emit(event)

    return domains
