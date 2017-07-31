## A migration tool for BigTable

This project is just a lightweight wrapper of migration commands to `org.apache.hadoop.hbase`.  Example migrations look like:

```yml
up:
  - type: com.bbmtek.bigtablemigration.model.CreateTable
    tableName: Status
  - type: com.bbmtek.bigtablemigration.model.CreateColumnFamily
    tableName: Status
    columnFamilyName: Default
```

To run it

```bash
java -jar bigtable-migration-1.0.0.3.jar --bigtablemigration.bigtable.project.id=your-project --bigtablemigration.bigtable.instance.id=your-instance --bigtablemigration.migration.dir=/your/migration/dir/migrate/
```

All inspiration is derived from Ruby on Rails' simple migration system: http://edgeguides.rubyonrails.org/active_record_migrations.html

kthxbye
