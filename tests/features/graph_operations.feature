Feature: Graph and Dependency Operations
  As a developer
  I want to test dependency graph operations
  So that I can ensure proper execution order

  Background:
    Given a clean MongoDB test database

  Scenario: Parallel execution of independent phases
    Given I have a collection "source1" with 10 documents
    And I have a collection "source2" with 10 documents
    When I run the mongration "parallel_migration.py"
    Then the collection "result1" should exist
    And the collection "result2" should exist
    And the mongration status should be "COMPLETED"

  Scenario: Complex dependency chain
    Given I have a collection "source" with 100 documents
    When I run the mongration "complex_dependency_migration.py"
    Then the collection "merged" should exist
    And the mongration status should be "COMPLETED"
