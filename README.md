## A migration tool for BigTable

This project is just a lightweight wrapper of migration commands to `org.apache.hadoop.hbase`.  Example migrations look like:

```
up:
  - type: com.bbmtek.bigtablemigration.model.CreateTable
    tableName: Status
  - type: com.bbmtek.bigtablemigration.model.CreateColumnFamily
    tableName: Status
    columnFamilyName: Default
```
All inspiration is derived from Ruby on Rails' simple migration system: http://edgeguides.rubyonrails.org/active_record_migrations.html

kthxbye
