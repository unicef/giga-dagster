from datahub.ingestion.run.pipeline import Pipeline

from src.settings import settings


def add_business_glossary() -> None:
    # The pipeline configuration is similar to the recipe YAML files provided to the CLI tool.
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "datahub-business-glossary",
                "config": {
                    "file": "src/utils/datahub/datahub_business_glossary.yaml",
                    "enable_auto_id": True,  # recommended to set to true so datahub will auto-generate guids from your term names
                },
            },
            "sink": {
                "type": "datahub-rest",
                "config": {
                    "server": f"{settings.DATAHUB_METADATA_SERVER_URL}",
                    "token": f"{settings.DATAHUB_ACCESS_TOKEN}",
                },
            },
        },
    )

    # Run the pipeline and report the results.
    pipeline.run()
    pipeline.pretty_print_summary()


if __name__ == "__main__":
    add_business_glossary()
