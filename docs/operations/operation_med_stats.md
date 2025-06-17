# Operation `med_stats`

Maps PLZ  by replacing with MedStat region names, based on an official lookup table provided by the Swiss Federal Statistical Office (BFS).

For more information please have a look at:
    https://dam-api.bfs.admin.ch/hub/api/dam/assets/33347910/master


### Behavior
- Looks up a postal code (e.g., "8001") in a MedStat lookup table stored in an Excel file;
- Returns the corresponding MedStat region name (e.g., "Zürich Zentrum");
- If the postal code is not found, the original value is returned (e.g. foreign plz);
