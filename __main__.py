from databricks_toolkit.utils.config import PipelineConfig
from databricks_toolkit.pipelines.default_pipeline import run_pipeline


def main():
    config = PipelineConfig.from_args()
    run_pipeline(config)


if __name__ == "__main__":
    main()