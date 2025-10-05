Feature: Error Handling
  As a developer
  I want to test error handling in migrations
  So that I can ensure failures are handled gracefully

  Background:
    Given a clean MongoDB test database

  Scenario: Failed mongration handling
    When I run the mongration "failing_migration.py"
    Then the mongration status should be "FAILED"
    And the error should be logged
