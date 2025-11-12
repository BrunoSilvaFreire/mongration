Feature: State Management
  As a developer
  I want to test migration state management
  So that I can ensure migrations track their execution status

  Background:
    Given a clean MongoDB test database

  Scenario: Stateless migration re-execution
    When I run the mongration "stateless_migration.py"
    And I run the mongration "stateless_migration.py"
    Then the mongration should execute successfully each time
    And no state should be tracked

  Scenario: Migration status manipulation
    Given I run the mongration "simple_migration.py"
    And the mongration status is "COMPLETED"
    When I manipulate the mongration status to "ABSENT"
    Then the mongration status should be "ABSENT"

  Scenario: Migration state persists across runs
    When I run the mongration "simple_migration.py"
    Then the mongration status should be "COMPLETED"
    When I attempt to run the mongration "simple_migration.py" again
    Then the mongration should be skipped
    And the mongration status should remain "COMPLETED"

  Scenario: Failed migration state tracking
    When I run the mongration "failing_migration.py"
    Then the mongration status should be "FAILED"
    When I check the state database
    Then the failure should be recorded with error details

  Scenario: Work in progress state
    When I start running a long-running mongration
    And I check the status before completion
    Then the mongration status should be "COMPLETED"

  Scenario: Phase execution tracking
    When I run the mongration "multi_phase_migration.py"
    Then the state should track which phases were executed
    And the state should record document counts for each phase

  Scenario: Manipulate status with custom index
    When I manipulate a new mongration with index 100
    Then the mongration should have status "COMPLETED"
    And the mongration should be stored with index 100

  Scenario: State collection structure validation
    When I run the mongration "simple_migration.py"
    Then the state collection should exist
    And the state document should have required fields
    And the state document should have correct structure

  Scenario: Multiple migrations state isolation
    When I run the mongration "simple_migration.py"
    And I run the mongration "index_migration.py"
    Then each mongration should have separate state entries
    And both should have status "COMPLETED"

  Scenario: State cleanup for re-run
    Given I run the mongration "simple_migration.py"
    And the mongration status is "COMPLETED"
    When I manipulate the mongration status to "ABSENT"
    And I run the mongration "simple_migration.py" again
    Then the mongration should execute successfully
    And the mongration status should be "COMPLETED"
