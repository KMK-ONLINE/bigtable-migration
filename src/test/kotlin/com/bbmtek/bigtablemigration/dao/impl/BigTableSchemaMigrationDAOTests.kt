package com.bbmtek.bigtablemigration.dao.impl

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.filter.PageFilter
import org.apache.hadoop.hbase.util.Bytes
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.mockito.ArgumentCaptor
import org.mockito.Matchers
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.MockitoAnnotations

/**
 * Created by woi on 24/07/17.
 */
class BigTableSchemaMigrationDAOTests {

    @Mock
    lateinit var mockBigTableConnection: Connection
    @Mock
    lateinit var mockSchemaMigrationsTable: Table
    @Mock
    lateinit var mockResultScanner: ResultScanner
    lateinit var bigTableSchemaMigrationsDAO: BigTableSchemaMigrationsDAO

    @Before
    fun setup() {
        MockitoAnnotations.initMocks(this)
        `when`(mockBigTableConnection.getTable(TableName.valueOf("SchemaMigrations"))).thenReturn(mockSchemaMigrationsTable)
        bigTableSchemaMigrationsDAO = BigTableSchemaMigrationsDAO()
        bigTableSchemaMigrationsDAO.bigTableconnection = mockBigTableConnection
    }

    @Test
    fun `get last schema migrations version when there is previously executed migration`() {
        val expectedLastVersion = 100L
        val expectedScan =  Scan().setFilter(PageFilter(1))
        val scanCaptor = ArgumentCaptor.forClass(Scan::class.java)
        val mockResult = mock(Result::class.java)

        `when`(mockSchemaMigrationsTable.getScanner(Matchers.any(Scan::class.java))).thenReturn(mockResultScanner)
        `when`(mockResultScanner.next()).thenReturn(mockResult)
        `when`(mockResult.getValue(Bytes.toBytes("SchemaMigrations"), Bytes.toBytes("version"))).thenReturn(Bytes.toBytes(expectedLastVersion))

        val lastVersion = bigTableSchemaMigrationsDAO.getLastVersion()

        verify(mockSchemaMigrationsTable).getScanner(scanCaptor.capture())
        Assert.assertEquals(expectedScan.toString(), scanCaptor.value.toString())
        Assert.assertEquals(expectedLastVersion, lastVersion)
        verify(mockSchemaMigrationsTable).close()
    }

    @Test
    fun `get last schema migrations version when there is no previously executed migration (empty table)`() {
        val expectedLastVersion = -1L
        val expectedScan =  Scan().setFilter(PageFilter(1))
        val scanCaptor = ArgumentCaptor.forClass(Scan::class.java)

        `when`(mockSchemaMigrationsTable.getScanner(Matchers.any(Scan::class.java))).thenReturn(mockResultScanner)
        `when`(mockResultScanner.next()).thenReturn(null)

        val lastVersion = bigTableSchemaMigrationsDAO.getLastVersion()

        verify(mockSchemaMigrationsTable).getScanner(scanCaptor.capture())
        Assert.assertEquals(expectedScan.toString(), scanCaptor.value.toString())
        Assert.assertEquals(expectedLastVersion, lastVersion)
        verify(mockSchemaMigrationsTable).close()
    }

    @Test
    fun `write version`() {
        val migrationTimestamp = System.currentTimeMillis()
        val expectedPut = Put(Bytes.toBytes("${Long.MAX_VALUE - migrationTimestamp}"))
                            .addColumn(Bytes.toBytes("SchemaMigrations"), Bytes.toBytes("version"), Bytes.toBytes(migrationTimestamp))
        val putCaptor = ArgumentCaptor.forClass(Put::class.java)

        bigTableSchemaMigrationsDAO.writeVersion(migrationTimestamp)

        verify(mockSchemaMigrationsTable).put(putCaptor.capture())
        Assert.assertEquals(expectedPut.toString(), putCaptor.value.toString())
        verify(mockSchemaMigrationsTable).close()
    }
}