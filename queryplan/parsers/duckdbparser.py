from queryplan.parsers.planparser import PlanParser
from queryplan.plannode import InnerNode, LeafNode, PlanNode
from queryplan.queryoperator import ArrayUnnest, CustomOperator, DBMSType, GroupBy, InlineTable, Iteration, IterationScan, Join, PipelineBreakerScan, QueryOperator, Result, Select, SetOperation, Sort, TableScan, Temp, Window
from queryplan.queryplan import QueryPlan


class DuckDBParser(PlanParser):

    def __init__(self, include_system_representation=True):
        super().__init__()
        self.include_system_representation = include_system_representation
        self.op_counter = 0

    # Plain EXPLAIN uses different names for some operators than ANALYZE does.
    _PLAIN_NAME_MAP: dict[str, str] = {
        "SEQ_SCAN": "TABLE_SCAN",
    }

    @staticmethod
    def _normalize_plain_node(node: dict) -> dict:
        """Convert a plain EXPLAIN (FORMAT JSON) node to ANALYZE-compatible format.

        Plain format uses 'name' instead of 'operator_type' and has no
        'operator_cardinality'. Rename and inject from Estimated Cardinality.

        For operators that lack Estimated Cardinality (e.g. TOP_N), fall back to
        the first child's cardinality, capped by the Top (LIMIT) value if present.
        """
        result = dict(node)
        if "name" in result and "operator_type" not in result:
            name = result.pop("name")
            result["operator_type"] = DuckDBParser._PLAIN_NAME_MAP.get(name, name)
        # Normalize children first so their cardinalities are available below.
        if "children" in result:
            result["children"] = [
                DuckDBParser._normalize_plain_node(child)
                for child in result["children"]
            ]
        if "operator_cardinality" not in result:
            estimated = result.get("extra_info", {}).get("Estimated Cardinality")
            if estimated is not None:
                result["operator_cardinality"] = int(estimated)
            elif result.get("children"):
                # Propagate first child's cardinality (covers TOP_N and similar).
                child_card = result["children"][0].get("operator_cardinality", 0)
                limit_str = result.get("extra_info", {}).get("Top")
                if limit_str is not None:
                    try:
                        result["operator_cardinality"] = min(int(limit_str), child_card)
                    except (ValueError, TypeError):
                        result["operator_cardinality"] = child_card
                else:
                    result["operator_cardinality"] = child_card
            else:
                result["operator_cardinality"] = 0
        return result

    def parse_json_plan(self, query: str, json_plan) -> QueryPlan:
        if isinstance(json_plan, list):
            # Plain EXPLAIN (FORMAT JSON): top-level is a list of root nodes
            root_node = self._normalize_plain_node(json_plan[0])
        else:
            # EXPLAIN (FORMAT JSON, ANALYZE): unwrap the EXPLAIN_ANALYZE wrapper
            assert len(json_plan["children"]) == 1
            json_plan = json_plan["children"][0]
            assert json_plan["operator_type"] == "EXPLAIN_ANALYZE" and len(json_plan["children"]) == 1
            root_node = json_plan["children"][0]

        plan = self.build_initial_plan(root_node)
        root = InnerNode(Result(-1), exact_cardinality=plan.exact_cardinality,
                         estimated_cardinality=plan.estimated_cardinality, children=[plan],
                         system_representation="// added by benchy")
        return QueryPlan(text=query, plan=root)

    def build_initial_plan(self, json_plan: dict) -> PlanNode:
        operator_type = json_plan["operator_type"]
        operator_id = self.op_counter
        self.op_counter += 1
        operator = self.create_empty_operator(operator_type, operator_id)
        operator.fill(json_plan, DBMSType.DuckDB)

        estimated_cardinality = int(json_plan["extra_info"]["Estimated Cardinality"]) if "Estimated Cardinality" in json_plan["extra_info"] else None
        exact_cardinality = json_plan["operator_cardinality"]

        # append full duckdb representation (excluding children) to operator
        system_representation = None
        if self.include_system_representation:
            system_representation = json_plan.copy()
            for child_key in ["children"]:
                if child_key in system_representation:
                    system_representation.pop(child_key)

        if self.is_leaf_operator(json_plan):
            # Has no children
            return LeafNode(operator, estimated_cardinality=estimated_cardinality, exact_cardinality=exact_cardinality, system_representation=system_representation)
        else:
            children = []
            for child in json_plan["children"]:
                children.append(self.build_initial_plan(child))

            if operator_type == "PROJECTION":
                assert len(children) == 1
                if children[0].estimated_cardinality is None:
                    children[0].estimated_cardinality = estimated_cardinality
                return children[0]
            
            return InnerNode(operator, estimated_cardinality=estimated_cardinality, exact_cardinality=exact_cardinality, children=children, system_representation=system_representation)

    def create_empty_operator(self, operator_name: str, operator_id: int) -> QueryOperator:
        match operator_name:
            case "ORDER_BY":
                return Sort(operator_id)
            case "HASH_GROUP_BY" | "PERFECT_HASH_GROUP_BY" | "SIMPLE_AGGREGATE" | "UNGROUPED_AGGREGATE":
                return GroupBy(operator_id)
            case "PROJECTION":
                return CustomOperator("Projection", operator_id)
            case "COLUMN_DATA_SCAN" | "DUMMY_SCAN":
                return InlineTable(operator_id)
            case "TABLE_SCAN":
                return TableScan(operator_id)
            case "DELIM_SCAN":
                return CustomOperator("DelimScan", operator_id)
            case operator_name if operator_name.endswith("JOIN"):
                return Join(operator_id)
            case "TOP_N":
                return Sort(operator_id)
            case "FILTER":
                return Select(operator_id)
            case "LIMIT" | "STREAMING_LIMIT":
                return CustomOperator("Limit", operator_id)
            case "EMPTY_RESULT":
                return CustomOperator("EmptyResult", operator_id)
            case "UNION":
                return SetOperation(operator_id)
            case "CROSS_PRODUCT":
                return CustomOperator("CrossProduct", operator_id)
            case "WINDOW" | "STREAMING_WINDOW":
                return Window(operator_id)
            case "CTE":
                return Temp(operator_id)
            case "CTE_SCAN":
                return PipelineBreakerScan(operator_id)
            case "RECURSIVE_CTE":
                return Iteration(operator_id)
            case "RECURSIVE_CTE_SCAN":
                return IterationScan(operator_id)
            case "UNNEST":
                return ArrayUnnest(operator_id)
            case "INOUT_FUNCTION":
                return CustomOperator("INOUT_FUNCTION", operator_id)
            case other:
                raise ValueError(f"'{other}' is not a recognized DUCKDB operator")

    def is_leaf_operator(self, json_plan) -> bool:
        if not json_plan["children"]:
            return True
        return False
