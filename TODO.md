# TODO

- [X] Thread contention
- [x] RAF dfp zorder joins
- [x] RAF dfp zorder merge
- [x] WAF
- [ ] Zorder add select to take to form problem-solution

- [ ] Specify spark version in bundle itself (do not hardcode it)
- [x] Feature to be able to launch script both locally and in cluster (even if cluster settings are to be set manually)
- [ ] Version management (spark/delta/avro/...). Both local and Databricks.
    - [ ] Handle the fact that the versions used by the user are/are not the same as the one used to test the script
        - e.g. parameter to have warning or failure in case of wrong versions
- [ ] Extract common building-blocks in a dedicated folder (e.g. merge, streaming foreachBatch, showMaxMinStats, ...)

- [X] Push filtered airports dataset somewhere public and use it in the snippets 
- [X] Rename snippet file names to comply with convention problem-solution
- [X] Address different Spark versions
- [ ] All needed snippets are present
- [ ] For each snippet we have a correct name, documentation, and it works
- [X] For each snippet there is a section in the README.md
