# Operation `fake_plz`

Replaces postal code (`PLZ`) values in a column with realistic, locale-specific synthetic data. Optionally, parts of the original PLZ can be preserved.

Newly generated PLZ are based on the data provided at:
```python
    _DOWNLOAD_URL: str = "https://data.geo.admin.ch/ch.swisstopo-vd.ortschaftenverzeichnis_plz/ortschaftenverzeichnis_plz/ortschaftenverzeichnis_plz_2056.csv.zip"
```

### Additional Parameters

- `preserve` (`str | tuple[str] | None`, optional):
  Specifies which parts of the original PLZ should be preserved. Admissible values are:
  - `district` : first digit form the left;
  - `area`: second digit form the left;
  - `route`: third digit from the left;
  - `postcode`: forth digit form the left;

- `locale` (`str`, default=`"de_CH"`):
  Locale code for generating PLZ codes.

### Behavior
- Uses locale-aware Faker logic to generate fake PLZ values;
- Supports partial preservation of the original PLZ format through `preserve` (only possible from left to right: e.g. if `area` needs to be preserved, then also `district` will be);
- The result is consistent with regional expectations based on the `locale`;
