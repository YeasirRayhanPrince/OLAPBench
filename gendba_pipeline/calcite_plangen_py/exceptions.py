"""
Custom exceptions for the Calcite plangen wrapper.
"""


class PlanGenerationError(Exception):
    """Raised when plan generation fails."""

    def __init__(self, message, query_name=None, stderr=None):
        self.message = message
        self.query_name = query_name
        self.stderr = stderr
        super().__init__(self.message)

    def __str__(self):
        msg = self.message
        if self.query_name:
            msg += f" (query: {self.query_name})"
        if self.stderr:
            msg += f"\n{self.stderr}"
        return msg


class QueryFileNotFoundError(PlanGenerationError):
    """Raised when a query file cannot be found."""

    pass


class InvalidSchemaError(PlanGenerationError):
    """Raised when schema file is invalid."""

    pass


class ToolNotFoundError(PlanGenerationError):
    """Raised when the plangen tool binary cannot be found."""

    pass
