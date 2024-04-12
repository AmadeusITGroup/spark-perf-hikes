# TODO


- [X] Thread contention
- [ ] RAF dfp zorder joins
- [ ] RAF dfp zorder merge
- [ ] WAF


- [ ] Specify spark version in bundle itself (do not hardcode it)
- [ ] Feature to be able to launch script both locally and in cluster (even if cluster settings are to be set manually)
- [ ] Version management (spark/delta/avro/...). Both local and Databricks.
    - [ ] Handle the fact that the versions used by the user are/are not the same as the one used to test the script
        - e.g. parameter to have warning or failure in case of wrong versions
- [ ] Extract common methods in a "library" (e.g. showMaxMinStats)
- [ ] push filtered airports dataset somewhere public and use it in the snippets 