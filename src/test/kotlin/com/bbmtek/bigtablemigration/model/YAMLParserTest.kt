package com.bbmtek.bigtablemigration.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.junit.Assert.assertEquals
import org.junit.Test


/**
 * Created by woi on 25/07/17.
 */
class YAMLParserTest {

    @Test
    fun `convertYamlToObject`() {
        val mapper = ObjectMapper(YAMLFactory())
        val stringYaml ="""
                        up:
                            - type: com.bbmtek.bigtablemigration.model.CreateTable
                              tableName: status
                            - type: com.bbmtek.bigtablemigration.model.CreateColumnFamily
                              tableName: status
                              columnFamilyName: basic
                        """
        val migrations = mapper.readValue(stringYaml, Migrations::class.java)
        assertEquals(2, migrations.up.size)
        assertEquals("basic", migrations.up[1].columnFamilyName)
    }
}
