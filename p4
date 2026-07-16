private def routeKafkaMessages(
    spark: SparkSession,
    filteredKafkaBatchDF: DataFrame,
    hdfsSinkPath: String,
    mergedCommonProperties: Map[String, String]
): Unit = {

  logger.info(
    "Starting Kafka message format routing"
  )

  if (filteredKafkaBatchDF.rdd.isEmpty()) {
    logger.info(
      "Kafka batch is empty; nothing to route"
    )
    return
  }

  val classifiedKafkaBatchDF =
    filteredKafkaBatchDF
      .withColumn(
        "is_confluent_avro_candidate",
        isConfluentAvroCandidateUdf(
          col("value")
        )
      )
      .persist(
        StorageLevel.MEMORY_AND_DISK
      )

  try {

    val regularKafkaBatchDF =
      classifiedKafkaBatchDF
        .filter(
          !col("is_confluent_avro_candidate")
        )
        .drop(
          "is_confluent_avro_candidate"
        )

    val avroKafkaBatchDF =
      classifiedKafkaBatchDF
        .filter(
          col("is_confluent_avro_candidate")
        )
        .drop(
          "is_confluent_avro_candidate"
        )

    val regularCount =
      regularKafkaBatchDF.count()

    val avroCount =
      avroKafkaBatchDF.count()

    logger.info(
      s"Kafka routing completed: " +
        s"regular=$regularCount, " +
        s"avroCandidates=$avroCount"
    )

    if (regularCount > 0) {

      logger.info(
        "Running existing regular Kafka processing"
      )

      processKafkaMessages(
        spark,
        regularKafkaBatchDF,
        hdfsSinkPath,
        mergedCommonProperties
      )
    }

    if (avroCount > 0) {

      logger.info(
        "Running Kafka Avro processing"
      )

      KafkaAvroProcessor.processKafkaAvroBatch(
        spark,
        avroKafkaBatchDF,
        mergedCommonProperties
      )
    }

  } catch {

    case exception: Exception =>

      logger.error(
        "Kafka message routing failed",
        exception
      )

      throw exception

  } finally {

    classifiedKafkaBatchDF.unpersist()

    logger.info(
      "Released classified Kafka batch"
    )
  }
}
