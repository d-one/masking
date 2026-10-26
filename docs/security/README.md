# Security

The library processes personally identifiable information (PII). Masked output is only as safe as the way its inputs, secrets, and concordance tables are handled.

## Concordance Tables

`pipeline.concordance_tables` and prescribed `concordance_table` arguments map clear values to masked values, so they reverse the masking.

- Store them with the same access controls as the clear data, or not at all.
- Do not log, print, or commit them outside synthetic examples.
- Call `pipeline.clear_concordance_tables()` before reusing a pipeline for data that must not be linked to earlier runs.

## Hashing

`HashOperation` and `YYYYHashOperation` hash with HMAC when a `secret` is given, and with a plain hash function otherwise.

- Without a secret, low-entropy values such as names, dates, or IDs can be recovered by hashing candidate values. Pass a `secret` for PII.
- Read the secret from a secret store or environment variable at runtime; never hard-code or commit it.
- The same secret and input give the same hash in every run and every backend. This links records across datasets; use a different secret where linking is unwanted.
- `YYYYHashOperation` keeps the year in clear text by design.

## Entity Detection

`MaskPresidio`, `MaskDictOperation`, `StringMatchOperation`, and `StringMatchDictOperation` detect PII in free text and dictionaries.

- Detection by spaCy models is probabilistic: it can miss entities or mask harmless words. Review a sample of the output before sharing it.
- Analysis runs locally with the installed spaCy models; the library sends no data to external services.
- `allow_list` keeps listed values in clear text; keep it to values that are not PII.

## Known Issues

| Issue                                                                                                                         | Mitigation until fixed                                        |
| ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `FakeNameOperation` with `reuse_existing=True` can output clear input names ([#4](https://github.com/d-one/masking/issues/4)) | Pass `reuse_existing=False`.                                  |
| Some exception messages contain the clear value being masked ([#5](https://github.com/d-one/masking/issues/5))                | Do not forward masking exceptions to logs readable by others. |

## Repository Data

- Tests, docs, and the sample files in `data/` contain synthetic values only. Never commit real personal data.
- The bundled sources in `src/masking/faker/sources/` and `src/masking/base_operations/sources/` are public statistics (BFS, swisstopo), not personal records.
