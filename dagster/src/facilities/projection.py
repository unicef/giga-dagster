from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.facilities.specs import FACILITY_ID_COLUMN, FacilityGeolocationSpec

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


@dataclass(frozen=True, slots=True)
class FacilityProjections:
    core: "DataFrame"
    extension: "DataFrame"


def _source_column(
    dataframe: "DataFrame",
    source: str,
    target: str,
    *,
    allow_missing: bool,
) -> "Column":
    from pyspark.sql import functions as f

    if source in dataframe.columns:
        return f.col(source).alias(target)
    if allow_missing:
        return f.lit(None).cast("string").alias(target)
    raise ValueError(f"Approved facility data is missing required column: {source}")


def _signature(dataframe: "DataFrame", output_column: str) -> "DataFrame":
    from pyspark.sql import functions as f

    values = [
        f.coalesce(f.col(column).cast("string"), f.lit("<null>"))
        for column in sorted(dataframe.columns)
    ]
    return dataframe.withColumn(
        output_column,
        f.sha2(f.concat_ws("\x1f", *values), 256),
    )


def refresh_facility_signature(
    dataframe: "DataFrame",
    output_column: str,
) -> "DataFrame":
    """Recompute a projection signature after final schema type coercion."""
    if output_column in dataframe.columns:
        dataframe = dataframe.drop(output_column)
    return _signature(dataframe, output_column)


def project_facility_core(
    approved: "DataFrame",
    spec: FacilityGeolocationSpec,
    *,
    allow_missing: bool = False,
    include_signature: bool = True,
) -> "DataFrame":
    """Project an approved combined row into the canonical facility core shape."""
    from pyspark.sql import functions as f

    aliases = dict(spec.core_aliases)
    mapped_sources = set(aliases)
    selected = [
        _source_column(
            approved,
            source,
            target,
            allow_missing=allow_missing,
        )
        for source, target in spec.core_aliases
    ]
    selected.insert(2, f.lit(spec.facility_type).alias("facility_type"))
    selected.extend(
        _source_column(
            approved,
            source,
            source,
            allow_missing=allow_missing,
        )
        for source in spec.columns.core
        if source not in mapped_sources
    )
    core = approved.select(*selected)
    return _signature(core, "core_signature") if include_signature else core


def project_facility_extension(
    approved: "DataFrame",
    spec: FacilityGeolocationSpec,
    *,
    allow_missing: bool = False,
    include_signature: bool = True,
) -> "DataFrame":
    """Project specialized attributes with the canonical core foreign key."""
    selected = [
        _source_column(
            approved,
            spec.giga_id_column,
            FACILITY_ID_COLUMN,
            allow_missing=allow_missing,
        )
    ]
    selected.extend(
        _source_column(
            approved,
            source,
            source,
            allow_missing=allow_missing,
        )
        for source in spec.columns.extension
    )
    extension = approved.select(*selected)
    return (
        _signature(extension, "extension_signature")
        if include_signature
        else extension
    )


def split_approved_facilities(
    approved: "DataFrame",
    spec: FacilityGeolocationSpec,
    *,
    allow_missing: bool = False,
    include_signatures: bool = True,
) -> FacilityProjections:
    """Split one approved frame without filtering, deduplicating, or writing it."""
    return FacilityProjections(
        core=project_facility_core(
            approved,
            spec,
            allow_missing=allow_missing,
            include_signature=include_signatures,
        ),
        extension=project_facility_extension(
            approved,
            spec,
            allow_missing=allow_missing,
            include_signature=include_signatures,
        ),
    )
