def should_emit_metadata(dataset_urn: str, urn_id: str, in_production: bool) -> bool:
    """Validate if metadata should be emitted based on URN and environment."""
    prod_only_data_lake_urns = ["school_master.db", "qos.db"]

    if "deltaLake" not in dataset_urn:
        return False

    if in_production and urn_id not in prod_only_data_lake_urns:
        return False

    return True
