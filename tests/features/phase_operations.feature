Feature: Phase Operations
  As a developer
  I want to test various phase operations
  So that I can ensure data transformations work correctly

  Background:
    Given a clean MongoDB test database

  Scenario: Python document transformation
    Given I have a collection "test_collection" with documents
      | name    | age | score |
      | Alice   | 30  | 85    |
      | Bob     | 25  | 92    |
      | Charlie | 35  | 78    |
    When I run the mongration "python_transform_migration.py"
    Then the collection "transformed_collection" should exist
    And the collection "transformed_collection" should have documents with field "name_upper"
    And the mongration status should be "COMPLETED"

  Scenario: Generator operation without source
    When I run the mongration "generator_migration.py"
    Then the collection "generated_collection" should exist
    And the collection "generated_collection" should contain 10 documents
    And the mongration status should be "COMPLETED"

  Scenario: Streaming aggregation with large dataset
    Given I have a collection "large_collection" with 1000 documents
    When I run the mongration "streaming_migration.py"
    Then the collection "streamed_collection" should exist
    And the collection "streamed_collection" should contain 10 documents
    And the mongration status should be "COMPLETED"

  Scenario: Collection rename operation
    Given I have a collection "test_collection" with documents
      | name  | value |
      | Item1 | 100   |
      | Item2 | 200   |
    When I run the mongration "rename_migration.py"
    Then the collection "test_collection" should not exist
    And the collection "renamed_collection" should exist
    And the mongration status should be "COMPLETED"

  Scenario: Multiple phase dependencies
    Given I have a collection "test_collection" with documents
      | name  | age | score |
      | Alice | 30  | 85    |
      | Bob   | 35  | 78    |
      | Carol | 25  | 92    |
    When I run the mongration "multi_phase_migration.py"
    Then the collection "senior_users" should exist
    And all phases should be executed in order
    And the mongration status should be "COMPLETED"
