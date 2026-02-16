from src.spark.config_expectations import Config


def test_config_initialization():
    config = Config()
    assert config.SIMILARITY_RATIO_CUTOFF == 0.7
    assert config.current_year >= 2024
    assert len(config.DATA_QUALITY_CHECKS_DESCRIPTIONS) > 0


def test_config_properties():
    config = Config()

    assert "school_id_giga" in config.NONEMPTY_COLUMNS_ALL
    assert "cellular_coverage_availability" in config.VALUES_DOMAIN_ALL
    assert "latitude" in config.VALUES_RANGE_ALL

    assert config.VALUES_DOMAIN_MASTER["cellular_coverage_availability"] == [
        "yes",
        "no",
    ]
    assert config.VALUES_RANGE_GEOLOCATION["latitude"] == {"min": -90, "max": 90}
