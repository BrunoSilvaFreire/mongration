"""
Mongrations Functional Tests

This package contains functional tests for the mongrations MongoDB migration tool.
Tests are implemented using the Behave BDD framework.

Main components:
- features/: Gherkin feature files describing test scenarios
- features/steps/: Python step definitions implementing the scenarios
- fixtures/: Test utilities and sample data
- reports/: Test execution reports (generated)

To run tests:
    cd tests
    behave

To run specific scenarios:
    behave features/mongrations.feature

For more information, see tests/README.md
"""

__version__ = "0.1.0"
__author__ = "Bruno Silva Freire"