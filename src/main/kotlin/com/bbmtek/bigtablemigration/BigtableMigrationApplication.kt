package com.bbmtek.bigtablemigration

import com.bbmtek.bigtablemigration.dao.SchemaMigrationsDAO
import com.bbmtek.bigtablemigration.dao.impl.BigTableMigrationDAO
import com.bbmtek.bigtablemigration.model.CreateColumnFamily
import com.bbmtek.bigtablemigration.model.CreateTable
import com.bbmtek.bigtablemigration.model.Migrations
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import java.io.File

@SpringBootApplication
class BigtableMigrationApplication: CommandLineRunner {
    @Autowired
    lateinit var bigTableConnection: Connection

    @Autowired
    lateinit var schemaMigrationsDAO: SchemaMigrationsDAO

    @Value("\${bigtablemigration.migration.dir}")
    lateinit var migrationDir: String

    override fun run(vararg args: String?) {
        val admin = bigTableConnection.admin
        if(!admin.tableExists(TableName.valueOf("SchemaMigrations"))) {
            val schemaMigrationTableDescriptor = HTableDescriptor(TableName.valueOf("SchemaMigrations"))
            schemaMigrationTableDescriptor.addFamily(HColumnDescriptor("SchemaMigrations"))
            admin.createTable(schemaMigrationTableDescriptor)
        }

        val schemaMigrationLastVersion = schemaMigrationsDAO.getLastVersion()

        val migrations = getBigTableMigrations(schemaMigrationLastVersion)
        migrations.forEach {
            it.up.forEach {
                when(it) {
                    is CreateTable -> {
                        admin.createTable(HTableDescriptor(TableName.valueOf(it.tableName)))
                    }
                    is CreateColumnFamily -> {
                        admin.addColumn(TableName.valueOf(it.tableName), HColumnDescriptor(it.columnFamilyName))
                    }
                }
            }
            schemaMigrationsDAO.writeVersion(it.version)
        }

        admin.close()
    }

    private fun getBigTableMigrations(lastVersion: Long): List<Migrations> {
        val migrationFiles = File(migrationDir).listFiles()
        val migrations = arrayListOf<Migrations>()
        if(migrationFiles != null) {
            val filteredFiles =  migrationFiles.filter {
                it.nameWithoutExtension.split("_")[0].toLong() > lastVersion
            }

            val objectMapper = ObjectMapper(YAMLFactory())
            filteredFiles.mapTo(migrations) {
                objectMapper.readValue(it, Migrations::class.java)
                        .copy(version = it.nameWithoutExtension.split("_")[0].toLong())
            }
        }
        return migrations
    }
}

fun main(args: Array<String>) {
    SpringApplication.run(BigtableMigrationApplication::class.java, *args)
}
