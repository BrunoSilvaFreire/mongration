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

  Scenario: Graph with multiple roots
    Given I have multiple source collections
    When I run the mongration "multi_root_graph_migration.py"
    Then all independent phases should execute in parallel
    And dependent phases should wait for their sources
    And the mongration status should be "COMPLETED"

  Scenario: Graph traversal with error in dependency
    Given a clean MongoDB test database
    Given I have a collection "test_collection" with documents
      | name  | value |
      | doc1  | 100   |
      | doc2  | 200   |
    When I run the mongration "failing_dependency_migration.py"
    Then the failing phase should stop
    And dependent phases should not execute
    And the mongration status should be "FAILED"

  Scenario: Circular dependency detection
    When I attempt to run a mongration with circular dependencies
    Then the mongration should fail immediately
    And an error about circular dependencies should be raised

  Scenario: Graph with long dependency chain
    Given I have a collection "chain_source" with documents
      | name  | value |
      | start | 1     |
    When I run the mongration "long_chain_migration.py"
    Then all phases should execute in correct order
    And the final collection should contain processed data
    And the mongration status should be "COMPLETED"

  Scenario: Phase with multiple dependencies
    Given I have collections "source_a" and "source_b" with documents
    When I run the mongration "multi_dependency_migration.py"
    Then the phase should wait for all dependencies
    And the merged result should be correct
    And the mongration status should be "COMPLETED"
