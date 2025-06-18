# Operation `fake_name`

Replaces entries with realistic, locale-aware synthetic names.

### Additional Parameters

- `locale` (`str`, optional):
    Specifies which geographicaly location is of interest. Default: `de_CH`.
- `gender` (`str | None`, optional):
    Specify which gendere is required for the generation of names. Admissible values:
    - `male`, `m`
    - `female`, `f`
    - `nonbinary`, `nb`
- `name_type` (`str`, optional): Specify what time of name one needs to generate. Default to `full`.
Admissible values:
    - `full`
    - `first`
    - `last`
- `reuse_existing` (`bool`, optional): Specify whether to reuse previously encountered names or not. Default to `True`.

### Behavior
- Generates new names based on the provided configuration;
- Substitute the entire cell-content with newly generated names;
