from src.utils.datahub import (
    add_glossary,
    add_platform_metadata,
    builders,
    column_metadata,
    create_domains,
    create_tags,
    create_validation_tab,
    datahub_ingest_nb_metadata,
    emit_dataset_metadata,
    emit_lineage,
    emitter,
    entity,
    graphql,
    identify_country_name,
    ingest_azure_ad,
    list_datasets,
    update_policies,
    validator,
)


def test_datahub_emitter():
    assert len(dir(emitter)) > 3


def test_datahub_entity():
    assert len(dir(entity)) > 3


def test_datahub_builders():
    assert len(dir(builders)) > 3


def test_datahub_column_metadata():
    assert len(dir(column_metadata)) > 3


def test_datahub_graphql():
    assert len(dir(graphql)) > 3


def test_datahub_validator():
    assert len(dir(validator)) > 3


def test_datahub_emit_dataset():
    assert len(dir(emit_dataset_metadata)) > 3


def test_datahub_emit_lineage():
    assert len(dir(emit_lineage)) > 3


def test_datahub_list_datasets():
    assert len(dir(list_datasets)) > 3


def test_datahub_add_glossary():
    assert len(dir(add_glossary)) > 3


def test_datahub_create_domains():
    assert len(dir(create_domains)) > 3


def test_datahub_create_tags():
    assert len(dir(create_tags)) > 3


def test_datahub_identify_country():
    assert len(dir(identify_country_name)) > 3


def test_datahub_update_policies():
    assert len(dir(update_policies)) > 3


def test_datahub_add_platform():
    assert len(dir(add_platform_metadata)) > 3


def test_datahub_validation_tab():
    assert len(dir(create_validation_tab)) > 3


def test_datahub_ingest_nb():
    assert len(dir(datahub_ingest_nb_metadata)) > 3


def test_datahub_ingest_azure_ad():
    assert len(dir(ingest_azure_ad)) > 3
