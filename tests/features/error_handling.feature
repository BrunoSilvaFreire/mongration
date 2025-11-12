Feature: Error Handling
  As a developer
  I want to test error handling in migrations
  So that I can ensure failures are handled gracefully

  Background:
    Given a clean MongoDB test database

  Scenario: Failed mongration handling
    When I run the mongration "failing_migration.py"
    Then the mongration status should be "FAILED"
    And the error should be logged

  Scenario: Invalid mongration file
    When I attempt to run a non-existent mongration file
    Then an error should be raised
    And the error message should indicate file not found

  Scenario: Mongration with syntax error
    When I attempt to run a mongration with syntax errors
    Then the mongration should fail to load
    And a syntax error should be reported

  Scenario: Missing required mongration function
    When I attempt to run a mongration without the mongration function
    Then an error should be raised
    And the error should indicate missing mongration function

  Scenario: Invalid MongoDB connection
    When I attempt to run a mongration with invalid connection URL
    Then the mongration should fail
    And a connection error should be logged

  Scenario: Operation on non-existent collection
    Given I have no collections in the database
    When I run the mongration "missing_source_migration.py"
    Then the mongration should complete
    And no documents should be processed

  Scenario: Destination write failure
    When I run the mongration "destination_failure_migration.py"
    Then the mongration status should be "FAILED"
    And the write error should be logged

  Scenario: Transformation error in phase
    Given a clean MongoDB test database
    Given I have a collection "error_collection" with documents
      | name  | value |
      | item1 | 100   |
      | item2 | 200   |
    When I run the mongration "transformation_error_migration.py"
    Then the mongration status should be "FAILED"
    And the transformation error should be logged

  Scenario: Import from missing file
    When I run the mongration "missing_file_import_migration.py"
    Then the mongration status should be "FAILED"
    And a file not found error should be logged

  Scenario: Invalid operation configuration
    When I attempt to run a mongration with invalid operation setup
    Then an error should be raised
    And the configuration error should be reported
