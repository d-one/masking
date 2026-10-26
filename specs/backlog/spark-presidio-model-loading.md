---
ticket: TBD
links:
  - src/masking/mask_spark/operations/operation.py
  - src/masking/utils/entity_detection.py
  - docs/spark.md
owner: petmarco7
status: outline
affected_components:
  - src/masking/utils/entity_detection.py
  - src/masking/base_operations/operation_presidio.py
  - docs/spark.md
assumptions:
  - Measured locally: a pickled Presidio operation is about 1.15 GB because it carries the spaCy models; unpickling takes about 3.9 s per Spark task.
out_of_scope:
  - Presidio performance on pandas and polars.
  - Choice of spaCy models.
---

# Feature: Load Presidio Models on Spark Executors Instead of Shipping Them

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: TBD
- Related specs: none
- Related ADRs: none yet; the choice of loading strategy is architecture-relevant.

## Problem

Spark pickles each operation into every task. For `MaskPresidio` and the dict and string-match operations that hold an analyzer, the pickle includes the spaCy models (about 1.15 GB), so every task spends about 3.9 s deserializing before it masks a row, and executors hold one model copy per task.

## Proposed Behavior

The pickled operation carries the model names, not the loaded models. Each executor process loads the models once, on first use, and reuses them for every later task. [NEEDS CLARIFICATION: Can every target cluster install the spaCy models on executors (cluster library or init script), or must the models still ship with the job, for example as a broadcast archive?]

## Acceptance Criteria

1. **AC1** — Given a Presidio operation, when it is pickled, then the pickle is smaller than 1 MB.
1. **AC2** — Given a Spark job with several tasks per executor, when it masks free text, then each executor process loads the models at most once.
1. **AC3** — Masked output is identical to today's on the pandas, polars, and Spark backends.
1. **AC4** — `docs/spark.md` states the executor requirement for spaCy models.

## Tests

| AC  | Test type         | Location            |
| --- | ----------------- | ------------------- |
| AC1 | unit              | `tests/utils/`      |
| AC2 | Spark integration | `tests/mask_spark/` |
| AC3 | equivalence       | `tests/mask_spark/` |
