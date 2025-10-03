Feature: MongoDB Migrations Management
  As a developer
  I want to manage MongoDB schema migrations
  So that I can evolve my database schema in a controlled manner

  Background:
    Given a clean MongoDB test database

  Scenario: Running a simple mongration
    When I run the mongration "simple_migration.py"
    Then the collection should exist in the database
    And the mongration status should be "COMPLETED"

  Scenario: Running multiple phases in a mongration
    When I run the mongration "index_migration.py"
    Then all phases should be executed in order
    And the mongration status should be "COMPLETED"

  Scenario: Dry run mode
    Given I run the mongration "simple_migration.py"
    When I run the mongration in dry run mode
    Then the collection should not exist in the database
    And the mongration status should be "ABSENT"

  Scenario: Failed mongration handling
    When I run the mongration "failing_migration.py"
    Then the mongration status should be "FAILED"
    And the error should be logged

  Scenario: Manipulating mongration status
    Given I have a mongration script
    And the mongration status is "ABSENT"
    When I manipulate the mongration status to "COMPLETED"
    Then the mongration status should be "COMPLETED"

  Scenario: Stateless mongration
    Given I run the mongration "stateless_migration.py"
    When I run the mongration multiple times
    Then the mongration should execute successfully each time
    And no state should be tracked

  Scenario: Migration dependencies
    Given I run the mongrations
      | script           |
      | simple_migration |
      | index_migration  |
    When I run the mongrations directory
    Then the mongrations should execute in dependency order
    And all mongrations should be "COMPLETED"

  Scenario: Index creation operation
    When I run the mongration "index_migration.py"
    Then the index should exist on the collection
    And the mongration status should be "COMPLETED"

  Scenario: Data aggregation operation
    Given I have a collection with test data
    When I run the mongration "aggregation_migration.py"
    Then the aggregated data should be processed correctly
    And the mongration status should be "COMPLETED"