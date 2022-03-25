import json
from typing import Dict, Optional

from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
from rtree import index

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)


# This class defines a Metric to support your Expectation.
# For most ColumnExpectations, the main business logic for calculation will live in this class.
class ColumnValuesDistinctPolygons(ColumnAggregateMetricProvider):

    # This is the id string that will be used to reference your metric.
    metric_name = "column_values.distinct_polygons"

    # This method implements the core logic for the PandasExecutionEngine
    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        shapes = column

        for pos, shape in enumerate(shapes):
            idx.insert(pos, shape.bounds)

        for shape in shapes:
            merged_shapes = unary_union([shapes[pos] for pos in idx.intersection(shape.bounds) if shapes[pos] != shape])
            intersections.append(shape.intersection(merged_shapes))

        intersection = unary_union(intersections)

        return intersection.is_empty

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesPolygonToBeDistinct(ColumnExpectation):
    """
    This expectation checks whether polygons in a column are distinct and not intersect.
    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "shapes_intersecting": [
                    Polygon([[0, 0], [2, 0], [2, 2], [0, 2]]),
                    Polygon([[0, 1], [3, 1], [2, 3]]),
                    Polygon([[3, 0], [4, 0], [4, 1], [0, 1]])
                ],
                "shapes_touching": [
                    Polygon([[0, 0], [2, 0], [2, 2], [0, 2]]),
                    Polygon([[0, 2], [0, 3], [2, 2]]),
                    Polygon([[2, 0], [4, 0], [4, 2], [2, 2]])
                ],
                "shapes_distinct": [
                    Polygon([[0, 0], [2, 0], [2, 2], [0, 2]]),
                    Polygon([[0, 3], [0, 4], [3, 3]]),
                    Polygon([[4, 0], [6, 0], [6, 4], [4, 4]])
                ]
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "shapes_distinct",
                        "empty": True,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "shapes_intersecting",
                        "empty": False},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    metric_dependencies = ("column_values.distinct_polygons",)

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("empty",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """

        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        # # Check other things in configuration.kwargs and raise Exceptions if needed
        # try:
        #     assert (
        #         ...
        #     ), "message"
        #     assert (
        #         ...
        #     ), "message"
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))

        return True
    
    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        raise NotImplementedError

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesPolygonToBeDistinct().print_diagnostic_checklist()
