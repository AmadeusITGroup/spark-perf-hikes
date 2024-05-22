# DONE

- [x] RAF dfp zorder joins
- [x] RAF dfp zorder merge
- [x] WAF
- [x] Feature to be able to launch script both locally and in cluster (even if cluster settings are to be set manually and script is only copied)
- [X] Push filtered airports dataset somewhere public and use it in the snippets 
- [X] Rename snippet file names to comply with convention problem-solution
- [X] Address different Spark versions
- [x] Specify spark version in snippet itself (do not hardcode it)
- [x] Handle the fact that the Spark version used by the user locally may not be the same as the one used to test the script (raise warning or failure in case of wrong Spark versions)
- [x] All needed snippets are present for SFO

# TODO

- [ ] TODO for each snippet today (especially zorder-select one)
    - [ ] For each snippet we have a correct name, documentation, and it works
    - [ ] Add WHAT TO LOOK FOR in comments
    - [ ] Be more explicit about where the snippet has been tested

- [ ] Create Databricks cluster from settings in the snippet
- [ ] Extract common building-blocks in a dedicated folder (e.g. merge, streaming foreachBatch, showMaxMinStats, ...)

