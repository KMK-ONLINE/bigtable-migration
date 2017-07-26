package com.bbmtek.bigtablemigration.dao

/**
 * Created by woi on 21/07/17.
 */
interface SchemaMigrationsDAO {
    fun getLastVersion(): Long
    fun writeVersion(version: Long)
}