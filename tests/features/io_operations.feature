Feature: IO Operations
  As a developer
  I want to test input/output operations
  So that I can ensure data flows correctly between phases

  Background:
    Given a clean MongoDB test database

  Scenario: Collection source with filter
    Given I have a collection "mixed_collection" with mixed data
    When I run the mongration "filtered_source_migration.py"
    Then the collection "filtered_collection" should exist
    And the mongration status should be "COMPLETED"

  Scenario: Aggregation source
    Given I have a collection "test_collection" with categorized data
    When I run the mongration "aggregation_source_migration.py"
    Then the collection "aggregated_source" should exist
    And the mongration status should be "COMPLETED"

  Scenario: Pipe between phases
    Given I have a collection "test_collection" with documents
      | name  | age | score |
      | Alice | 30  | 85    |
      | Bob   | 35  | 78    |
    When I run the mongration "multi_phase_migration.py"
    Then data should flow through the pipe
    And the mongration status should be "COMPLETED"
