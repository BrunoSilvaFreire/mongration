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
