
package com.citi.olympus.gru.dataconnector.DataConnector

import com.citi.olympus.gru.dataconnector.util.AvroProcessingUtility
import com.citi.olympus.gru.dataconnector.util.AvroProcessingUtility.{
  CsvWriteConfig,
  SchemaRegistryConfig
}
import com.citi.olympus.gru.dataconnector.util.SparkLogger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object KafkaAvroProcessor extends SparkLogger {

  def processKafkaAvroBatch(
      spark: SparkSession,
      avroBatchDF: DataFrame,
      mergedCommonProperties: Map[String, String]
  ): Unit = {

    logger.info("Starting Kafka Avro processing")

    if (avroBatchDF.rdd.isEmpty()) {
      logger.info("No Avro Kafka records available")
      return
    }

    validateKafkaColumns(avroBatchDF)

    val cachedAvroBatchDF =
      avroBatchDF.persist(
        StorageLevel.MEMORY_AND_DISK
      )

    try {

      val (
        validHeaderDF,
        invalidHeaderDF
      ) =
        AvroProcessingUtility.parseHeaders(
          cachedAvroBatchDF
        )

      val invalidHeaderCount =
        invalidHeaderDF.count()

      val validHeaderCount =
        validHeaderDF.count()

      logger.info(
        s"Avro header result: valid=$validHeaderCount, invalid=$invalidHeaderCount"
      )

      if (invalidHeaderCount > 0) {

        val headerFailureDF =
          invalidHeaderDF.withColumn(
            "failure_stage",
            lit("HEADER_PARSE")
          )

        writeFailureRecords(
          headerFailureDF,
          failurePath(
            mergedCommonProperties,
            "header_parse"
          ),
          mergedCommonProperties
        )
      }

      if (validHeaderCount == 0) {
        logger.info(
          "No records with valid Confluent Avro headers"
        )
        return
      }

      val schemaRegistryConfig =
        buildSchemaRegistryConfig(
          mergedCommonProperties
        )

      val csvWriteConfig =
        buildCsvWriteConfig(
          mergedCommonProperties
        )

      val (
        csvReadyDF,
        decodeFailedDF
      ) =
        AvroProcessingUtility
          .decodeAndFlattenWithSchemaRegistry(
            validHeaderDF,
            schemaRegistryConfig,
            csvWriteConfig.stripRootPrefixes
          )

      val decodeFailureCount =
        decodeFailedDF.count()

      val csvReadyCount =
        csvReadyDF.count()

      logger.info(
        s"Avro decode result: success=$csvReadyCount, failed=$decodeFailureCount"
      )

      if (decodeFailureCount > 0) {

        val decodeFailureOutputDF =
          decodeFailedDF.withColumn(
            "failure_stage",
            lit("AVRO_DECODE")
          )

        writeFailureRecords(
          decodeFailureOutputDF,
          failurePath(
            mergedCommonProperties,
            "avro_decode"
          ),
          mergedCommonProperties
        )
      }

      if (csvReadyCount > 0) {

        AvroProcessingUtility.writeCsv(
          csvReadyDF,
          csvWriteConfig
        )

        logger.info(
          s"Successfully wrote $csvReadyCount Avro records to ${csvWriteConfig.outputPath}"
        )
      } else {
        logger.info(
          "No successfully decoded Avro records available for CSV output"
        )
      }

    } catch {

      case exception: Exception =>

        logger.error(
          "Kafka Avro processing failed",
          exception
        )

        throw exception

    } finally {

      cachedAvroBatchDF.unpersist()

      logger.info(
        "Released cached Kafka Avro batch"
      )
    }
  }

  private def buildSchemaRegistryConfig(
      properties: Map[String, String]
  ): SchemaRegistryConfig = {

    SchemaRegistryConfig(
      baseUrl =
        requiredProperty(
          properties,
          "schema_registry_url"
        ),

      connectTimeoutMs =
        properties
          .getOrElse(
            "schema_registry_connect_timeout_ms",
            "10000"
          )
          .toInt,

      readTimeoutMs =
        properties
          .getOrElse(
            "schema_registry_read_timeout_ms",
            "30000"
          )
          .toInt
    )
  }

  private def buildCsvWriteConfig(
      properties: Map[String, String]
  ): CsvWriteConfig = {

    val stripRootPrefixes =
      properties
        .getOrElse(
          "avro_csv_strip_root_prefixes",
          ""
        )
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .toSeq

    CsvWriteConfig(
      outputPath =
        requiredProperty(
          properties,
          "avro_csv_output_hdfs_path"
        ),

      mode =
        properties.getOrElse(
          "avro_csv_write_mode",
          "append"
        ),

      header =
        properties.getOrElse(
          "avro_csv_header",
          "true"
        ),

      delimiter =
        properties.getOrElse(
          "avro_csv_delimiter",
          ","
        ),

      quote =
        properties.getOrElse(
          "avro_csv_quote",
          "\""
        ),

      escape =
        properties.getOrElse(
          "avro_csv_escape",
          "\\"
        ),

      nullValue =
        properties.getOrElse(
          "avro_csv_null_value",
          ""
        ),

      multiLine =
        properties.getOrElse(
          "avro_csv_multiline",
          "false"
        ),

      stripRootPrefixes =
        stripRootPrefixes
    )
  }

  private def writeFailureRecords(
      failedDF: DataFrame,
      outputPath: String,
      properties: Map[String, String]
  ): Unit = {

    val writeMode =
      properties.getOrElse(
        "avro_failed_record_write_mode",
        "append"
      )

    failedDF.write
      .mode(writeMode)
      .json(outputPath)
  }

  private def failurePath(
      properties: Map[String, String],
      failureStage: String
  ): String = {

    val basePath =
      requiredProperty(
        properties,
        "avro_failed_record_hdfs_path"
      )

    s"${basePath.stripSuffix("/")}/$failureStage"
  }

  private def validateKafkaColumns(
      kafkaBatchDF: DataFrame
  ): Unit = {

    val requiredColumns = Set(
      "topic",
      "partition",
      "offset",
      "timestamp",
      "key",
      "value"
    )

    val missingColumns =
      requiredColumns.diff(
        kafkaBatchDF.columns.toSet
      )

    require(
      missingColumns.isEmpty,
      s"Kafka Avro batch is missing required columns: ${missingColumns.mkString(", ")}"
    )
  }

  private def requiredProperty(
      properties: Map[String, String],
      propertyName: String
  ): String = {

    properties
      .get(propertyName)
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse {
        throw new IllegalArgumentException(
          s"Required property '$propertyName' is missing or empty"
        )
      }
  }
}
