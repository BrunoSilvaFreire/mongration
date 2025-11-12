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

  Scenario: File destination operation
    Given I have a collection "file_dest_collection" with documents
      | name  | value |
      | Doc1  | 100   |
      | Doc2  | 200   |
      | Doc3  | 300   |
    When I run the mongration "file_destination_migration.py"
    Then the output file should exist
    And the output file should contain formatted documents
    And the mongration status should be "COMPLETED"

  Scenario: Delete collection operation
    Given a clean MongoDB test database
    Given I have a collection "collection_to_delete" with documents
      | name  | value |
      | temp1 | 100   |
      | temp2 | 200   |
    When I run the mongration "delete_collection_migration.py"
    Then the collection "collection_to_delete" should not exist
    And the mongration status should be "COMPLETED"

  Scenario: Phase without destination
    Given I have a collection "test_collection" with documents
      | name  | value |
      | data1 | 100   |
      | data2 | 200   |
    When I run the mongration "export_migration.py"
    Then the export operation should complete successfully
    And no destination collection should be created
    And the mongration status should be "COMPLETED"

  Scenario: Aggregation with options
    Given I have a collection "test_collection" with 100 documents
    When I run the mongration "aggregation_with_options_migration.py"
    Then the aggregation should use custom options
    And the result collection should contain correct data
    And the mongration status should be "COMPLETED"
