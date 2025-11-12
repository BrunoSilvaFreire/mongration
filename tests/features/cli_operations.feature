Feature: CLI Operations
  As a developer
  I want to test command line interface functionality
  So that I can ensure all CLI commands work correctly

  Background:
    Given a clean MongoDB test database

  Scenario: Run mongration via CLI with --mongration flag
    When I run mongrate with "--mongration simple_migration.py"
    Then the command should succeed
    And the mongration status should be "COMPLETED"

  Scenario: Run multiple mongrations via CLI with --mongrations-dir flag
    When I run mongrate with "--mongrations-dir" pointing to test migrations
    Then the command should succeed
    And all mongrations should be executed

  Scenario: Run mongration in dry-run mode via CLI
    When I run mongrate with "--dry-run" and "--mongration simple_migration.py"
    Then the command should succeed
    And no changes should be made to the database
    And the mongration status should be "ABSENT"

  Scenario: Manipulate mongration status via CLI
    Given I have a mongration "simple_migration.py"
    When I run mongrate manipulate with "--status COMPLETED"
    Then the command should succeed
    And the mongration status should be "COMPLETED"

  Scenario: Manipulate mongration status with custom index
    Given I have a mongration "simple_migration.py"
    When I run mongrate manipulate with "--status COMPLETED" and "--index 999"
    Then the command should succeed
    And the mongration should have index 999
    And the mongration status should be "COMPLETED"

  Scenario: CLI error handling for missing parameters
    When I run mongrate without required parameters
    Then the command should fail
    And an error message should be displayed

  Scenario: CLI error handling for invalid mongration path
    When I run mongrate with an invalid mongration path
    Then the command should fail
    And an appropriate error message should be shown

  Scenario: CLI error handling for invalid command
    When I run mongrate with an unknown command
    Then the command should fail
    And an error message should indicate unknown command

  Scenario: Run mongration programmatically
    When I call run_mongration_from_args with valid parameters
    Then the function should return 0
    And the mongration should execute successfully

  Scenario: Programmatic error handling
    When I call run_mongration_from_args with invalid parameters
    Then the function should return non-zero exit code
    And errors should be logged
