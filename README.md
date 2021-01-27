# dagster_play

## Executing Pipelines

### From the command line, use the Dagster CLI

From the directory of the pipeline file, just run:

dagster pipeline execute -f xxx.py

### From Python, use Dagster’s Python API, for example:

```if __name__ == "__main__":
    result = execute_pipeline(hello_cereal_pipeline)
```

### From a GUI, use the Dagit tool to visualize the pipeline in Dagit, from the directory of pipeline file, just run:

dagit -f xxx.py

