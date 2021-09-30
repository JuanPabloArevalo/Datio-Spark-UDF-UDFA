Feature: Feature for CrashCourse
  Scenario: Run a service process
    Given The id of the process as 'CrashCourse'
    And The env 'INPUT_PATH' targeting the resource file 'data/olympics.csv'
    And The env 'INPUT_SCHEMA_PATH' targeting the resource file 'schema/inputSchema.json'
    And The env 'OUTPUT_PATH' targeting the target file 'output'
    And The env 'OUTPUT_SCHEMA_PATH' targeting the resource file 'schema/outputSchema.json'
    And A config file with the contents:
      """
      CrashCourse {
        inputSchemaPath = ${?INPUT_SCHEMA_PATH}
        outputSchemaPath = ${?OUTPUT_SCHEMA_PATH}
        inputPath = ${?INPUT_PATH}
        outputPath = ${?OUTPUT_PATH}
      }
      """
    When Executing the Launcher
    Then The exit code should be 0
