from src.constants import DataTier, constants


def test_data_tier_enum_values():
    assert DataTier.RAW.value == "raw"
    assert DataTier.BRONZE.value == "bronze"
    assert DataTier.SILVER.value == "silver"
    assert DataTier.GOLD.value == "gold"
    assert DataTier.STAGING.value == "staging"
    assert DataTier.MANUAL_REJECTED.value == "rejected"


def test_data_tier_enum_completeness():
    all_tiers = list(DataTier)
    assert len(all_tiers) >= 5
    tier_values = [t.value for t in all_tiers]
    assert "raw" in tier_values
    assert "bronze" in tier_values
    assert "silver" in tier_values
    assert "gold" in tier_values


def test_data_tier_comparison():
    assert DataTier.RAW != DataTier.BRONZE
    assert DataTier.SILVER == DataTier.SILVER
    assert DataTier.GOLD != DataTier.SILVER


def test_data_tier_in_list():
    tiers = [DataTier.RAW, DataTier.BRONZE]
    assert DataTier.RAW in tiers
    assert DataTier.GOLD not in tiers


def test_constants_type_mappings_exists():
    assert hasattr(constants, "TYPE_MAPPINGS")
    type_mappings = constants.TYPE_MAPPINGS
    assert type_mappings is not None


def test_constants_has_folder_configs():
    assert hasattr(constants, "gold_source_folder")
    folder = constants.gold_source_folder
    assert folder is not None
    assert isinstance(folder, str)


def test_constants_object_not_none():
    assert constants is not None
    attrs = [a for a in dir(constants) if not a.startswith("_")]
    assert len(attrs) >= 5
