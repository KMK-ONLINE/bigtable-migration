package com.bbmtek.bigtablemigration

import com.google.cloud.bigtable.hbase.BigtableConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Before
import org.junit.Test
import org.springframework.boot.SpringApplication


/**
 * Created by woi on 25/07/17.
 */
class BigtableMigrationApplicationTest {

    lateinit var connection: Connection
    lateinit var admin: Admin

    @Before
    fun setUp() {
        val schemaMigrationsTableName = TableName.valueOf("SchemaMigrations")
        val feedsTableName = TableName.valueOf("Feeds")
        val statusTableName = TableName.valueOf("Status")
        connection = BigtableConfiguration.connect("bbm-dev", "test-instance")
        admin = connection.admin

        if(admin.tableExists(schemaMigrationsTableName)) {
            admin.deleteTable(schemaMigrationsTableName)
        }

        // delete all tables and column families defined in migration test (yaml file)
        if(admin.tableExists(feedsTableName)) {
            admin.deleteTable(feedsTableName)
        }
        if(admin.tableExists(statusTableName)) {
            admin.deleteTable(statusTableName)
        }
    }

    @Test
    fun `application is run for the first time`() {
        SpringApplication.run(BigtableMigrationApplication::class.java)

        `assert schema migration versions is existed`()
        `assert table is created`()
    }

    @Test
    fun `run all migrations that has not been executed`() {
        val hTableSchemaMigrationDescriptor = HTableDescriptor(TableName.valueOf("SchemaMigrations")).addFamily(HColumnDescriptor("SchemaMigrations"))
        val hTableStatusDescriptor = HTableDescriptor(TableName.valueOf("Status")).addFamily(HColumnDescriptor("Default"))
        admin.createTable(hTableSchemaMigrationDescriptor)
        admin.createTable(hTableStatusDescriptor)

        val table = connection.getTable(TableName.valueOf("SchemaMigrations"))
        val versionPut = Put(Bytes.toBytes("${Long.MAX_VALUE - 20170725170000}"))
                .addColumn(Bytes.toBytes("SchemaMigrations"), Bytes.toBytes("version"), Bytes.toBytes(20170725170000))
        table.put(versionPut)
        table.close()

        SpringApplication.run(BigtableMigrationApplication::class.java)

        `assert schema migration versions is existed`()
        `assert table is created`()
    }

    @Test
    fun `application not running any migration when SchemaMigrations have same versions as migrate folder`() {
        val hTableSchemaMigrationDescriptor = HTableDescriptor(TableName.valueOf("SchemaMigrations")).addFamily(HColumnDescriptor("SchemaMigrations"))
        val hTableStatusDescriptor = HTableDescriptor(TableName.valueOf("Status")).addFamily(HColumnDescriptor("Default"))
        val hTableFeedsDescriptor = HTableDescriptor(TableName.valueOf("Feeds")).addFamily(HColumnDescriptor("Default"))
        admin.createTable(hTableSchemaMigrationDescriptor)
        admin.createTable(hTableStatusDescriptor)
        admin.createTable(hTableFeedsDescriptor)

        val table = connection.getTable(TableName.valueOf("SchemaMigrations"))
        val versionOne = Put(Bytes.toBytes("${Long.MAX_VALUE - 20170725170000}"))
                .addColumn(Bytes.toBytes("SchemaMigrations"), Bytes.toBytes("version"), Bytes.toBytes(20170725170000))
        table.put(versionOne)
        table.close()

        val versionTwo = Put(Bytes.toBytes("${Long.MAX_VALUE - 20170725173000}"))
                .addColumn(Bytes.toBytes("SchemaMigrations"), Bytes.toBytes("version"), Bytes.toBytes(20170725173000))
        table.put(versionTwo)
        table.close()

        SpringApplication.run(BigtableMigrationApplication::class.java)

        `assert schema migration versions is existed`()
        `assert table is created`()
    }

    private fun `assert schema migration versions is existed`() {
        assert(admin.tableExists(TableName.valueOf("SchemaMigrations")))
        assert(admin.tableExists(TableName.valueOf("Status")))
        assert(connection.getTable(TableName.valueOf("SchemaMigrations")).exists(Get(Bytes.toBytes("${Long.MAX_VALUE - 20170725170000}"))))
        assert(connection.getTable(TableName.valueOf("SchemaMigrations")).exists(Get(Bytes.toBytes("${Long.MAX_VALUE - 20170725173000}"))))
    }

    private fun `assert table is created`() {
        assert(admin.getTableDescriptor(TableName.valueOf("Status")).hasFamily(Bytes.toBytes("Default")))
        assert(admin.tableExists(TableName.valueOf("Feeds")))
        assert(admin.getTableDescriptor(TableName.valueOf("Feeds")).hasFamily(Bytes.toBytes("Default")))
    }

}