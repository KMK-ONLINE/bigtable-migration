package com.bbmtek.bigtablemigration.dao.impl

import com.bbmtek.bigtablemigration.dao.SchemaMigrationsDAO
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.hadoop.hbase.util.Bytes
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository

/**
 * Created by woi on 21/07/17.
 */
@Repository
class BigTableSchemaMigrationsDAO: SchemaMigrationsDAO {
    @Autowired
    lateinit var bigTableconnection: Connection

    override fun writeVersion(version: Long) {
        val table = bigTableconnection.getTable(TableName.valueOf("SchemaMigrations"))
        val versionPut = Put(Bytes.toBytes("${Long.MAX_VALUE - version}"))
                .addColumn(Bytes.toBytes("SchemaMigrations"), Bytes.toBytes("version"), Bytes.toBytes(version))
        table.put(versionPut)
        table.close()
    }

    override fun getLastVersion(): Long {
        val table = bigTableconnection.getTable(TableName.valueOf("SchemaMigrations"))
        val scan = Scan().setFilter(PageFilter(1))

        val scanner = table.getScanner(scan)
        val result = scanner.next()

        table.close()

        return if(result == null) -1 else Bytes.toLong(result.getValue(Bytes.toBytes("SchemaMigrations"), Bytes.toBytes("version")))
    }
}