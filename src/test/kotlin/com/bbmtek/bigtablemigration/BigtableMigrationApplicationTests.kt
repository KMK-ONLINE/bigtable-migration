package com.bbmtek.bigtablemigration

import com.bbmtek.bigtablemigration.dao.SchemaMigrationsDAO
import com.bbmtek.bigtablemigration.dao.impl.BigTableMigrationDAO
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.junit.Before
import org.junit.Test
import org.mockito.Matchers
import org.mockito.Mock
import org.mockito.Mockito.*
import org.mockito.MockitoAnnotations

class BigtableMigrationApplicationTests {

	@Mock
	lateinit var mockBigTableConnection: Connection
	@Mock
	lateinit var mockAdmin: Admin
	@Mock
	lateinit var mockSchemaMigrationsDAO: SchemaMigrationsDAO

	lateinit var application: BigtableMigrationApplication

	@Before
	fun setUp() {
		MockitoAnnotations.initMocks(this)
		application = BigtableMigrationApplication()
		application.bigTableConnection = mockBigTableConnection
		application.schemaMigrationsDAO = mockSchemaMigrationsDAO
		application.migrationDir = "./migration"

		`when`(mockBigTableConnection.admin).thenReturn(mockAdmin)
		`when`(mockAdmin.tableExists(TableName.valueOf("SchemaMigrations"))).thenReturn(true)
	}

	@Test
	fun contextLoads() {
	}

	@Test
	fun `run migration when there is a schema migrations table`() {
		application.run()

		verify(mockAdmin, times(1)).tableExists(TableName.valueOf("SchemaMigrations"))
		verify(mockAdmin, times(0)).createTable(Matchers.any(HTableDescriptor::class.java))
	}

	@Test
	fun `run migration when there is no schema migrations table`() {
		`when`(mockAdmin.tableExists(TableName.valueOf("SchemaMigrations"))).thenReturn(false)
		val expectedMigrationMetadataTableDescriptor = HTableDescriptor(TableName.valueOf("SchemaMigrations"))
		expectedMigrationMetadataTableDescriptor.addFamily(HColumnDescriptor("SchemaMigrations"))

		application.run()

		verify(mockAdmin, times(1)).tableExists(TableName.valueOf("SchemaMigrations"))
		verify(mockAdmin, times(1)).createTable(expectedMigrationMetadataTableDescriptor)
	}

	@Test
	fun `when run migration it gets migration items to be executed`() {
	}
}
