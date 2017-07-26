package com.bbmtek.bigtablemigration.model

import com.fasterxml.jackson.annotation.JsonTypeInfo

/**
 * Created by woi on 24/07/17.
 */
data class Migrations(val up: List<MigrationUp> = arrayListOf(), val version: Long = 0)

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "type")
abstract class MigrationUp(var tableName: String, var columnFamilyName: String? = null)

class CreateTable(tableName: String = "") : MigrationUp(tableName)
class CreateColumnFamily(tableName: String = "", columnFamilyName: String = "") : MigrationUp(tableName, columnFamilyName)
