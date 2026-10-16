import unicodedata
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


FACILITY_ID_NAMESPACE = uuid.UUID("fb301d21-8e50-5cbd-8a47-d6e97770ecb4")


def _normalize_identity_part(value: object) -> str:
    normalized = unicodedata.normalize("NFKC", str(value))
    return " ".join(normalized.strip().casefold().split())


@dataclass(frozen=True, slots=True)
class StableFacilityIdContract:
    """Versioned contract for a deterministic, immutable facility identifier."""

    namespace: uuid.UUID = FACILITY_ID_NAMESPACE
    version: str = "facility-id-v1"
    separator: str = "\x1f"

    def canonical_key(
        self,
        facility_type: object,
        country_code: object,
        facility_id_govt: object,
    ) -> str | None:
        if facility_type is None or country_code is None or facility_id_govt is None:
            return None
        parts = tuple(
            _normalize_identity_part(value)
            for value in (facility_type, country_code, facility_id_govt)
        )
        if any(not part for part in parts):
            return None
        return self.separator.join((self.version, *parts))

    def generate(
        self,
        facility_type: object,
        country_code: object,
        facility_id_govt: object,
    ) -> str | None:
        canonical_key = self.canonical_key(
            facility_type,
            country_code,
            facility_id_govt,
        )
        if canonical_key is None:
            return None
        return str(uuid.uuid5(self.namespace, canonical_key))

    def spark_expression(
        self,
        facility_type: "Column",
        country_code: "Column",
        facility_id_govt: "Column",
    ) -> "Column":
        """Return a Spark expression implementing the same UUIDv5 contract."""
        from pyspark.sql import functions as f
        from pyspark.sql.types import StringType

        generate_udf = f.udf(self.generate, StringType())
        return generate_udf(facility_type, country_code, facility_id_govt)

    def add_to_dataframe(
        self,
        dataframe: "DataFrame",
        *,
        output_column: str,
        facility_type: str,
        country_column: str,
        govt_id_column: str,
        preserve_existing: bool = True,
    ) -> "DataFrame":
        """Add an ID, preserving a supplied non-null ID when requested."""
        from pyspark.sql import functions as f

        generated = self.spark_expression(
            f.lit(facility_type),
            f.col(country_column),
            f.col(govt_id_column),
        )
        if preserve_existing and output_column in dataframe.columns:
            generated = f.coalesce(f.col(output_column), generated)
        return dataframe.withColumn(output_column, generated)


STABLE_FACILITY_ID_CONTRACT = StableFacilityIdContract()


def generate_facility_id(
    facility_type: object,
    country_code: object,
    facility_id_govt: object,
) -> str | None:
    return STABLE_FACILITY_ID_CONTRACT.generate(
        facility_type,
        country_code,
        facility_id_govt,
    )
