Review the existing Scala/Spark Kafka project and implement an integration test that runs the application’s real main processing flow for one specific Kafka topic.

Requirements:

1. Inspect the existing main class, topic-selection logic, Kafka configuration, Spark configuration, processing flow, output-writing logic, and current testing framework. Follow existing naming conventions.

2. Create an integration test using the testing framework already configured in the project.

3. The topic name must be controlled from the test class:

```scala
val testTopicName = "REPLACE_WITH_TEST_TOPIC_NAME"
```

4. Pass the topic name into the main application using the project’s existing configuration mechanism. If this is not currently supported, add an optional argument:

```text
--topic-name <value>
```

5. When `--topic-name` is provided, the application must select and process only that topic. Apply the filter before creating Kafka subscriptions or launching topic-specific processing.

6. Do not hardcode the test topic name in production code. When `--topic-name` is absent, preserve the existing production behavior of processing all configured topics.

7. The test must launch the actual main application and traverse the existing end-to-end flow:

```text
Main program
    → Load configuration
    → Select only the test topic
    → Connect to Kafka
    → Read messages
    → Process/transform messages
    → Write output locally
```

Do not duplicate production processing logic inside the test.

8. Override the output location from the test and write the result to:

```text
file:///C:/temp/kafka-integration/<topic-name>/
```

Use Windows-safe path handling. Do not use an unescaped value such as `C:\temp`.

9. Run Spark in local mode:

```text
local[*]
```

This test override must not change the Spark master used in UAT or production.

10. If the existing `main()` method calls `System.exit()` or cannot be tested safely, extract the processing execution into:

```scala
def run(args: Array[String]): Unit
```

Keep the production entry point unchanged:

```scala
def main(args: Array[String]): Unit = run(args)
```

The integration test can then call `run(args)`.

11. The test may clean only its exact topic-specific output directory before execution. It must never delete the entire `C:/temp` directory.

12. After execution, validate that:

* Only the topic specified in the test was selected.
* No other configured topics were processed.
* The output directory was created.
* At least one actual data file was generated.
* The generated data file is not empty.
* Spark control files such as `_SUCCESS`, CRC files, and hidden files are excluded from data-file validation.
* The test fails with a meaningful message if the topic does not exist, contains no messages, or produces no output.

13. Reuse the project’s existing Kafka bootstrap servers, authentication, metadata, schema registry, and application configuration. Do not hardcode credentials, server addresses, or secrets.

14. Preserve the existing processing behavior for String, Binary, Avro, or other supported message formats. The selected topic must follow its normal production routing and processing logic.

15. If the Kafka consumer is streaming continuously, add a test-safe termination condition using an existing option such as `availableNow`, `once`, a maximum message count, or a configurable timeout. Do not leave the integration test running indefinitely.

16. Add clear comments explaining:

* Why the topic is controlled from the test class.
* Where the topic filtering occurs.
* How the test ensures no other topic is processed.
* Why local Spark and output overrides do not affect production.

Make the minimum necessary production-code changes.

After implementation, provide:

* Files created or modified.
* Explanation of each change.
* The exact Maven command for running only this integration test on Windows.
* Required Kafka messages and configuration needed before running the test.
