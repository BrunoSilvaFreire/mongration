Feature: Advanced Operations
  As a developer
  I want to test advanced migration features
  So that I can handle complex schema transformations

  Background:
    Given a clean MongoDB test database

  Scenario: UUID conversion migration
    Given I have a collection "uuid_test_collection" with string IDs
    When I run the mongration "uuid_conversion_migration.py"
    Then all IDs should be converted to UUID
    And the mongration status should be "COMPLETED"

  Scenario: UUID conversion with legacy field
    Given I have a collection "uuid_legacy_collection" with string IDs
    When I run the mongration "uuid_legacy_migration.py"
    Then all IDs should be converted to UUID
    And the legacy string IDs should be preserved
    And the mongration status should be "COMPLETED"

  Scenario: Multiple index creation
    Given I have a collection "test_collection" with documents
      | name  | age | score | email          |
      | Alice | 30  | 85    | alice@test.com |
      | Bob   | 25  | 92    | bob@test.com   |
    When I run the mongration "multiple_indexes_migration.py"
    Then the indexes should be created
    And the mongration status should be "COMPLETED"

  Scenario: Schema migration with data transformation
    Given I have a collection "old_schema_collection" with old schema documents
    When I run the mongration "schema_transformation_migration.py"
    Then the collection "new_schema_collection" should exist
    And all documents should have new schema
    And the mongration status should be "COMPLETED"
