package com.bbmtek.bigtablemigration.config

import com.google.cloud.bigtable.hbase.BigtableConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


/**
 * Created by woi on 07/07/17.
 */
@Configuration
class BigTableConfig {
    @Value("\${bigtablemigration.bigtable.project.id}")
    lateinit var projectId: String
    @Value("\${bigtablemigration.bigtable.instance.id}")
    lateinit var instanceId: String

    @Bean
    fun connection() : Connection = BigtableConfiguration.connect(projectId, instanceId)
}