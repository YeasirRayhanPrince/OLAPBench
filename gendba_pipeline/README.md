# GenDBA Pipeline

Self-contained dataset pipeline workspace for GenDBA dataset creation.

## Workspace Layout

- `run.py`: pipeline entrypoint
- `export.py`: JSONL exporter
- `dataset_pipeline.schema.json`: dataset-spec schema
- `configs/`: sample pipeline configs
- `output/<dataset>/query_sql/`: canonical query text inventory
- `output/<dataset>/physical_plan/`: raw and normalized physical plan artifacts
- `output/<dataset>/logical_plan/`: imported logical plan text when available
- `output/<dataset>/ir_physical_token/`: tokenized physical-plan IR
- `output/<dataset>/ir_logical_token/`: tokenized logical-plan IR
- `output/<dataset>/manifests/`: dataset-level manifests
- `output/<dataset>/runs/<run_id>/`: generated benchmark configs, target manifests, and result CSVs
- `output/<dataset>/exports/`: exported training JSONL

## Notes

- Physical plans come from OLAPBench.
- Logical IR is optional and can be imported per benchmark with `logical_ir.source_jsonl`.
- The workspace is kept under one folder so dataset artifacts do not get scattered through unrelated repo paths.
