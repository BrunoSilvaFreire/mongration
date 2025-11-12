Feature: File Operations
  As a developer
  I want to test file-based import/export operations
  So that I can ensure data can be moved between files and databases

  Background:
    Given a clean MongoDB test database

  Scenario: Export collection to file
    Given I have a collection "export_test_collection" with documents
      | name  | age | email           |
      | Alice | 30  | alice@test.com  |
      | Bob   | 25  | bob@test.com    |
      | Carol | 35  | carol@test.com  |
    When I run the mongration "export_migration.py"
    Then the export file should exist
    And the mongration status should be "COMPLETED"
    And the export file should contain 3 documents

  Scenario: Import data from file
    Given I have a JSON file with test documents
    When I run the mongration "import_migration.py"
    Then the collection "imported_collection" should exist
    And the collection "imported_collection" should have the imported documents
    And the mongration status should be "COMPLETED"

  Scenario: Export and reimport data
    Given I have a collection "original_collection" with documents
      | name  | value |
      | Item1 | 100   |
      | Item2 | 200   |
      | Item3 | 300   |
    When I run the mongration "export_migration.py"
    And I run the mongration "import_migration.py"
    Then the collection "imported_collection" should exist
    And the data should match the original collection
    And both mongrations should be "COMPLETED"

  Scenario: Export with transformation
    Given I have a collection "transform_export_collection" with documents
      | name    | score |
      | Alice   | 85    |
      | Bob     | 92    |
      | Charlie | 78    |
    When I run the mongration "export_with_transform_migration.py"
    Then the export file should exist
    And the exported documents should be transformed
    And the mongration status should be "COMPLETED"

  Scenario: Import with validation
    Given I have a JSON file with documents requiring validation
    When I run the mongration "import_with_validation_migration.py"
    Then the collection "validated_collection" should exist
    And all imported documents should be validated
    And the mongration status should be "COMPLETED"

  Scenario: Export to multiple formats
    Given I have a collection "multi_export_collection" with documents
      | name  | data      |
      | doc1  | content1  |
      | doc2  | content2  |
    When I run the mongration "multi_format_export_migration.py"
    Then multiple export files should exist
    And each file should contain the correct format
    And the mongration status should be "COMPLETED"
