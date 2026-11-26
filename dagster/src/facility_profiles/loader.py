"""Load facility profiles from ADLS (or a local directory for tests)."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from src.facility_profiles.schema import FacilityProfile

_PROFILES: dict[str, FacilityProfile] | None = None


class FacilityProfileError(RuntimeError):
    """Raised when facility profiles cannot be loaded or validated."""


def _parse_profile(schema_key: str, raw: str | bytes) -> FacilityProfile:
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise FacilityProfileError(
            f"Invalid YAML in facility profile '{schema_key}': {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise FacilityProfileError(
            f"Facility profile '{schema_key}' must be a YAML mapping"
        )
    try:
        return FacilityProfile(schema_key=schema_key, **data)
    except ValidationError as exc:
        raise FacilityProfileError(
            f"Facility profile '{schema_key}' failed validation:\n{exc}"
        ) from exc


def _load_from_local_dir(directory: Path) -> dict[str, FacilityProfile]:
    if not directory.is_dir():
        raise FacilityProfileError(
            f"Facility profiles local directory does not exist: {directory}"
        )
    profiles: dict[str, FacilityProfile] = {}
    for path in sorted(directory.glob("*.yaml")):
        schema_key = path.stem
        if schema_key in profiles:
            raise FacilityProfileError(
                f"Duplicate facility profile schema_key '{schema_key}'"
            )
        profiles[schema_key] = _parse_profile(schema_key, path.read_text())
    if not profiles:
        raise FacilityProfileError(
            f"No facility profile YAML files found under {directory}"
        )
    return profiles


def _load_from_adls(prefix: str) -> dict[str, FacilityProfile]:
    from src.utils.adls import ADLSFileClient

    normalized = prefix.strip("/")
    list_prefix = f"{normalized}/" if normalized else ""
    client = ADLSFileClient()
    profiles: dict[str, FacilityProfile] = {}
    found_any = False
    for file_data in client.list_paths_generator(list_prefix, recursive=False):
        if file_data.is_directory:
            continue
        name = file_data.name
        if not name.endswith(".yaml"):
            continue
        relative = name[len(list_prefix) :] if list_prefix else name
        if "/" in relative:
            continue
        found_any = True
        schema_key = Path(relative).stem
        if schema_key in profiles:
            raise FacilityProfileError(
                f"Duplicate facility profile schema_key '{schema_key}'"
            )
        raw = ADLSFileClient.download_raw(name)
        profiles[schema_key] = _parse_profile(schema_key, raw)

    if not found_any or not profiles:
        raise FacilityProfileError(
            f"No facility profile YAML files found under ADLS path "
            f"'{list_prefix or '/'}'. Upload profiles before starting Dagster."
        )
    return profiles


def load_facility_profiles(
    *,
    local_dir: Path | None = None,
    adls_prefix: str | None = None,
) -> dict[str, FacilityProfile]:
    from src.settings import settings

    directory = local_dir
    if directory is None and settings.FACILITY_PROFILES_LOCAL_DIR:
        directory = Path(settings.FACILITY_PROFILES_LOCAL_DIR)
    if directory is not None:
        return _load_from_local_dir(directory)

    prefix = adls_prefix or settings.FACILITY_PROFILES_ADLS_PREFIX
    return _load_from_adls(prefix)


def init_facility_profiles() -> dict[str, FacilityProfile]:
    global _PROFILES
    _PROFILES = load_facility_profiles()
    return _PROFILES


def get_facility_profiles() -> dict[str, FacilityProfile]:
    if _PROFILES is None:
        return init_facility_profiles()
    return _PROFILES


def get_facility_profile(schema_key: str) -> FacilityProfile:
    profiles = get_facility_profiles()
    try:
        return profiles[schema_key]
    except KeyError as exc:
        raise FacilityProfileError(
            f"No facility profile for schema_key '{schema_key}'. "
            f"Known keys: {sorted(profiles)}"
        ) from exc


def reset_facility_profiles_for_tests() -> None:
    global _PROFILES
    _PROFILES = None
