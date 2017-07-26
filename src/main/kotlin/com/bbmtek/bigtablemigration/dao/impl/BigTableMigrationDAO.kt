package com.bbmtek.bigtablemigration.dao.impl

import com.bbmtek.bigtablemigration.model.Migrations
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.File

/**
 * Created by woi on 25/07/17.
 */
@Component
class BigTableMigrationDAO {

    @Value("\${bigtablemigration.migration.dir}")
    lateinit var migrationDir: String


}