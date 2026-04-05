"""Template flow definitions for the Flowfile template gallery.

Each template is defined as a function that returns a FlowfileData object.
Templates use 'read' nodes pointing to CSV files in the local template_data directory.
"""

from collections.abc import Callable
from pathlib import Path

from flowfile_core.schemas.schemas import FlowfileData, FlowfileNode, FlowfileSettings
from flowfile_core.templates.models import FlowTemplateMeta

# Type alias for template builder functions
TemplateBuilder = Callable[[Path], FlowfileData]


def _make_read_node(
    node_id: int, filename: str, data_dir: Path, x: int, y: int, outputs: list[int]
) -> FlowfileNode:
    """Helper to create a read node pointing to a local CSV file."""
    file_path = str(data_dir / filename)
    return FlowfileNode(
        id=node_id,
        type="read",
        is_start_node=True,
        x_position=x,
        y_position=y,
        outputs=outputs,
        setting_input={
            "cache_results": False,
            "received_file": {
                "path": file_path,
                "file_type": "csv",
                "table_settings": {"file_type": "csv"},
            },
        },
    )


# ---------------------------------------------------------------------------
# Template 1: Sales Data Overview (Beginner)
# ---------------------------------------------------------------------------
def _build_sales_data_overview(data_dir: Path) -> FlowfileData:
    nodes = [
        _make_read_node(1, "sales_data.csv", data_dir, x=0, y=200, outputs=[2]),
        FlowfileNode(
            id=2,
            type="filter",
            x_position=250,
            y_position=200,
            input_ids=[1],
            outputs=[3],
            setting_input={
                "cache_results": False,
                "filter_input": {
                    "mode": "basic",
                    "basic_filter": {
                        "field": "region",
                        "operator": "equals",
                        "value": "North",
                    },
                },
            },
        ),
        FlowfileNode(
            id=3,
            type="group_by",
            x_position=500,
            y_position=200,
            input_ids=[2],
            outputs=[4],
            setting_input={
                "cache_results": False,
                "groupby_input": {
                    "agg_cols": [
                        {"old_name": "product", "agg": "groupby", "new_name": "product"},
                        {"old_name": "amount", "agg": "sum", "new_name": "total_revenue"},
                        {"old_name": "amount", "agg": "count", "new_name": "order_count"},
                        {"old_name": "amount", "agg": "mean", "new_name": "avg_order_value"},
                    ]
                },
            },
        ),
        FlowfileNode(
            id=4,
            type="sort",
            x_position=750,
            y_position=200,
            input_ids=[3],
            outputs=[5],
            setting_input={
                "cache_results": False,
                "sort_input": [{"column": "total_revenue", "how": "descending"}],
            },
        ),
        FlowfileNode(
            id=5,
            type="explore_data",
            x_position=1000,
            y_position=200,
            input_ids=[4],
            outputs=[],
            setting_input={"cache_results": False, "graphic_walker_input": None},
        ),
    ]
    return FlowfileData(
        flowfile_version="0.6.3",
        flowfile_id=1,
        flowfile_name="Sales Data Overview",
        flowfile_settings=FlowfileSettings(),
        nodes=nodes,
    )


# ---------------------------------------------------------------------------
# Template 2: Customer Deduplication (Beginner)
# ---------------------------------------------------------------------------
def _build_customer_deduplication(data_dir: Path) -> FlowfileData:
    nodes = [
        _make_read_node(1, "customers.csv", data_dir, x=0, y=200, outputs=[2]),
        FlowfileNode(
            id=2,
            type="unique",
            x_position=250,
            y_position=200,
            input_ids=[1],
            outputs=[3],
            setting_input={
                "cache_results": False,
                "unique_input": {"columns": ["email"], "strategy": "first"},
            },
        ),
        FlowfileNode(
            id=3,
            type="select",
            x_position=500,
            y_position=200,
            input_ids=[2],
            outputs=[4],
            setting_input={
                "cache_results": False,
                "keep_missing": False,
                "select_input": [
                    {"old_name": "customer_id", "new_name": "id"},
                    {"old_name": "name", "new_name": "customer_name"},
                    {"old_name": "email"},
                    {"old_name": "city"},
                    {"old_name": "segment"},
                ],
                "sorted_by": "none",
            },
        ),
        FlowfileNode(
            id=4,
            type="explore_data",
            x_position=750,
            y_position=200,
            input_ids=[3],
            outputs=[],
            setting_input={"cache_results": False, "graphic_walker_input": None},
        ),
    ]
    return FlowfileData(
        flowfile_version="0.6.3",
        flowfile_id=1,
        flowfile_name="Customer Deduplication",
        flowfile_settings=FlowfileSettings(),
        nodes=nodes,
    )


# ---------------------------------------------------------------------------
# Template 3: Employee Directory Cleanup (Beginner)
# ---------------------------------------------------------------------------
def _build_employee_directory_cleanup(data_dir: Path) -> FlowfileData:
    nodes = [
        _make_read_node(1, "employees.csv", data_dir, x=0, y=200, outputs=[2]),
        FlowfileNode(
            id=2,
            type="formula",
            x_position=250,
            y_position=200,
            input_ids=[1],
            outputs=[3],
            setting_input={
                "cache_results": False,
                "function": {
                    "field": {"name": "full_name", "data_type": "Auto"},
                    "function": 'concat([first_name], " ", [last_name])',
                },
            },
        ),
        FlowfileNode(
            id=3,
            type="filter",
            x_position=500,
            y_position=200,
            input_ids=[2],
            outputs=[4],
            setting_input={
                "cache_results": False,
                "filter_input": {
                    "mode": "basic",
                    "basic_filter": {
                        "field": "status",
                        "operator": "equals",
                        "value": "Active",
                    },
                },
            },
        ),
        FlowfileNode(
            id=4,
            type="sort",
            x_position=750,
            y_position=200,
            input_ids=[3],
            outputs=[5],
            setting_input={
                "cache_results": False,
                "sort_input": [
                    {"column": "department", "how": "ascending"},
                    {"column": "salary", "how": "descending"},
                ],
            },
        ),
        FlowfileNode(
            id=5,
            type="explore_data",
            x_position=1000,
            y_position=200,
            input_ids=[4],
            outputs=[],
            setting_input={"cache_results": False, "graphic_walker_input": None},
        ),
    ]
    return FlowfileData(
        flowfile_version="0.6.3",
        flowfile_id=1,
        flowfile_name="Employee Directory Cleanup",
        flowfile_settings=FlowfileSettings(),
        nodes=nodes,
    )


# ---------------------------------------------------------------------------
# Template 4: Order Enrichment - Join (Intermediate)
# ---------------------------------------------------------------------------
def _build_order_enrichment(data_dir: Path) -> FlowfileData:
    nodes = [
        _make_read_node(1, "orders.csv", data_dir, x=0, y=100, outputs=[3]),
        _make_read_node(2, "products.csv", data_dir, x=0, y=350, outputs=[3]),
        FlowfileNode(
            id=3,
            type="join",
            x_position=300,
            y_position=200,
            input_ids=[1],
            right_input_id=2,
            outputs=[4],
            setting_input={
                "cache_results": False,
                "auto_generate_selection": True,
                "verify_integrity": True,
                "join_input": {
                    "join_mapping": [{"left_col": "product_id", "right_col": "product_id"}],
                    "left_select": {
                        "select": [
                            {"old_name": "order_id"},
                            {"old_name": "customer_id"},
                            {"old_name": "quantity"},
                            {"old_name": "order_date"},
                        ]
                    },
                    "right_select": {
                        "select": [
                            {"old_name": "name", "new_name": "product_name"},
                            {"old_name": "category"},
                            {"old_name": "unit_price"},
                        ]
                    },
                    "how": "left",
                },
                "auto_keep_all": True,
                "auto_keep_right": True,
                "auto_keep_left": True,
            },
        ),
        FlowfileNode(
            id=4,
            type="formula",
            x_position=550,
            y_position=200,
            input_ids=[3],
            outputs=[5],
            setting_input={
                "cache_results": False,
                "function": {
                    "field": {"name": "total_price", "data_type": "Auto"},
                    "function": "[quantity] * [unit_price]",
                },
            },
        ),
        FlowfileNode(
            id=5,
            type="select",
            x_position=800,
            y_position=200,
            input_ids=[4],
            outputs=[6],
            setting_input={
                "cache_results": False,
                "keep_missing": False,
                "select_input": [
                    {"old_name": "order_id"},
                    {"old_name": "customer_id"},
                    {"old_name": "product_name"},
                    {"old_name": "category"},
                    {"old_name": "quantity"},
                    {"old_name": "unit_price"},
                    {"old_name": "total_price"},
                    {"old_name": "order_date"},
                ],
                "sorted_by": "none",
            },
        ),
        FlowfileNode(
            id=6,
            type="explore_data",
            x_position=1050,
            y_position=200,
            input_ids=[5],
            outputs=[],
            setting_input={"cache_results": False, "graphic_walker_input": None},
        ),
    ]
    return FlowfileData(
        flowfile_version="0.6.3",
        flowfile_id=1,
        flowfile_name="Order Enrichment",
        flowfile_settings=FlowfileSettings(),
        nodes=nodes,
    )


# ---------------------------------------------------------------------------
# Template 5: Survey Results Pivot (Intermediate)
# ---------------------------------------------------------------------------
def _build_survey_results_pivot(data_dir: Path) -> FlowfileData:
    nodes = [
        _make_read_node(1, "survey_responses.csv", data_dir, x=0, y=200, outputs=[2]),
        FlowfileNode(
            id=2,
            type="group_by",
            x_position=250,
            y_position=200,
            input_ids=[1],
            outputs=[3],
            setting_input={
                "cache_results": False,
                "groupby_input": {
                    "agg_cols": [
                        {"old_name": "respondent_id", "agg": "groupby", "new_name": "respondent_id"},
                        {"old_name": "question", "agg": "groupby", "new_name": "question"},
                        {"old_name": "rating", "agg": "mean", "new_name": "avg_rating"},
                    ]
                },
            },
        ),
        FlowfileNode(
            id=3,
            type="pivot",
            x_position=500,
            y_position=200,
            input_ids=[2],
            outputs=[4],
            setting_input={
                "cache_results": False,
                "pivot_input": {
                    "index_columns": ["respondent_id"],
                    "pivot_column": "question",
                    "value_col": "avg_rating",
                    "aggregations": ["mean"],
                },
                "output_fields": None,
            },
        ),
        FlowfileNode(
            id=4,
            type="explore_data",
            x_position=750,
            y_position=200,
            input_ids=[3],
            outputs=[],
            setting_input={"cache_results": False, "graphic_walker_input": None},
        ),
    ]
    return FlowfileData(
        flowfile_version="0.6.3",
        flowfile_id=1,
        flowfile_name="Survey Results Pivot",
        flowfile_settings=FlowfileSettings(),
        nodes=nodes,
    )


# ---------------------------------------------------------------------------
# Template 6: Web Analytics Funnel (Intermediate)
# ---------------------------------------------------------------------------
def _build_web_analytics_funnel(data_dir: Path) -> FlowfileData:
    nodes = [
        _make_read_node(1, "page_views.csv", data_dir, x=0, y=200, outputs=[2]),
        FlowfileNode(
            id=2,
            type="filter",
            x_position=250,
            y_position=200,
            input_ids=[1],
            outputs=[3, 6],
            setting_input={
                "cache_results": False,
                "filter_input": {
                    "mode": "basic",
                    "basic_filter": {
                        "field": "device",
                        "operator": "equals",
                        "value": "mobile",
                    },
                },
            },
        ),
        FlowfileNode(
            id=3,
            type="group_by",
            x_position=500,
            y_position=100,
            input_ids=[2],
            outputs=[4],
            setting_input={
                "cache_results": False,
                "groupby_input": {
                    "agg_cols": [
                        {"old_name": "page", "agg": "groupby", "new_name": "page"},
                        {"old_name": "event_id", "agg": "count", "new_name": "visit_count"},
                        {"old_name": "user_id", "agg": "n_unique", "new_name": "unique_visitors"},
                    ]
                },
            },
        ),
        FlowfileNode(
            id=4,
            type="sort",
            x_position=750,
            y_position=100,
            input_ids=[3],
            outputs=[5],
            setting_input={
                "cache_results": False,
                "sort_input": [{"column": "visit_count", "how": "descending"}],
            },
        ),
        FlowfileNode(
            id=5,
            type="explore_data",
            x_position=1000,
            y_position=100,
            input_ids=[4],
            outputs=[],
            setting_input={"cache_results": False, "graphic_walker_input": None},
        ),
        FlowfileNode(
            id=6,
            type="record_count",
            x_position=500,
            y_position=350,
            input_ids=[2],
            outputs=[],
            setting_input={"cache_results": False},
        ),
    ]
    return FlowfileData(
        flowfile_version="0.6.3",
        flowfile_id=1,
        flowfile_name="Web Analytics Funnel",
        flowfile_settings=FlowfileSettings(),
        nodes=nodes,
    )


# ---------------------------------------------------------------------------
# Template 7: Multi-Source Customer 360 (Advanced)
# ---------------------------------------------------------------------------
def _build_customer_360(data_dir: Path) -> FlowfileData:
    nodes = [
        _make_read_node(1, "customers.csv", data_dir, x=0, y=50, outputs=[4]),
        _make_read_node(2, "orders.csv", data_dir, x=0, y=250, outputs=[4]),
        _make_read_node(3, "support_tickets.csv", data_dir, x=0, y=450, outputs=[5]),
        # Join customers + orders
        FlowfileNode(
            id=4,
            type="join",
            x_position=300,
            y_position=150,
            input_ids=[1],
            right_input_id=2,
            outputs=[5],
            setting_input={
                "cache_results": False,
                "auto_generate_selection": True,
                "verify_integrity": True,
                "join_input": {
                    "join_mapping": [{"left_col": "customer_id", "right_col": "customer_id"}],
                    "left_select": {
                        "select": [
                            {"old_name": "customer_id"},
                            {"old_name": "name"},
                            {"old_name": "segment"},
                            {"old_name": "city"},
                        ]
                    },
                    "right_select": {
                        "select": [
                            {"old_name": "order_id"},
                            {"old_name": "quantity"},
                            {"old_name": "order_date"},
                        ]
                    },
                    "how": "left",
                },
                "auto_keep_all": True,
                "auto_keep_right": True,
                "auto_keep_left": True,
            },
        ),
        # Join result + support tickets
        FlowfileNode(
            id=5,
            type="join",
            x_position=600,
            y_position=250,
            input_ids=[4],
            right_input_id=3,
            outputs=[6],
            setting_input={
                "cache_results": False,
                "auto_generate_selection": True,
                "verify_integrity": True,
                "join_input": {
                    "join_mapping": [{"left_col": "customer_id", "right_col": "customer_id"}],
                    "left_select": {
                        "select": [
                            {"old_name": "customer_id"},
                            {"old_name": "name"},
                            {"old_name": "segment"},
                            {"old_name": "city"},
                            {"old_name": "order_id"},
                            {"old_name": "quantity"},
                        ]
                    },
                    "right_select": {
                        "select": [
                            {"old_name": "ticket_id"},
                            {"old_name": "priority"},
                        ]
                    },
                    "how": "left",
                },
                "auto_keep_all": True,
                "auto_keep_right": True,
                "auto_keep_left": True,
            },
        ),
        # Group by customer
        FlowfileNode(
            id=6,
            type="group_by",
            x_position=900,
            y_position=250,
            input_ids=[5],
            outputs=[7],
            setting_input={
                "cache_results": False,
                "groupby_input": {
                    "agg_cols": [
                        {"old_name": "customer_id", "agg": "groupby", "new_name": "customer_id"},
                        {"old_name": "name", "agg": "first", "new_name": "name"},
                        {"old_name": "segment", "agg": "first", "new_name": "segment"},
                        {"old_name": "city", "agg": "first", "new_name": "city"},
                        {"old_name": "order_id", "agg": "n_unique", "new_name": "total_orders"},
                        {"old_name": "quantity", "agg": "sum", "new_name": "total_items"},
                        {"old_name": "ticket_id", "agg": "n_unique", "new_name": "total_tickets"},
                    ]
                },
            },
        ),
        # Formula: customer score
        FlowfileNode(
            id=7,
            type="formula",
            x_position=1150,
            y_position=250,
            input_ids=[6],
            outputs=[8],
            setting_input={
                "cache_results": False,
                "function": {
                    "field": {"name": "customer_score", "data_type": "Auto"},
                    "function": "[total_orders] * 10 + [total_items] - [total_tickets] * 5",
                },
            },
        ),
        FlowfileNode(
            id=8,
            type="sort",
            x_position=1400,
            y_position=250,
            input_ids=[7],
            outputs=[9],
            setting_input={
                "cache_results": False,
                "sort_input": [{"column": "customer_score", "how": "descending"}],
            },
        ),
        FlowfileNode(
            id=9,
            type="explore_data",
            x_position=1650,
            y_position=250,
            input_ids=[8],
            outputs=[],
            setting_input={"cache_results": False, "graphic_walker_input": None},
        ),
    ]
    return FlowfileData(
        flowfile_version="0.6.3",
        flowfile_id=1,
        flowfile_name="Customer 360",
        flowfile_settings=FlowfileSettings(),
        nodes=nodes,
    )


# ---------------------------------------------------------------------------
# Template 8: Product Catalog Fuzzy Match (Advanced)
# ---------------------------------------------------------------------------
def _build_product_fuzzy_match(data_dir: Path) -> FlowfileData:
    nodes = [
        _make_read_node(1, "internal_products.csv", data_dir, x=0, y=100, outputs=[3]),
        _make_read_node(2, "supplier_products.csv", data_dir, x=0, y=350, outputs=[3]),
        FlowfileNode(
            id=3,
            type="fuzzy_match",
            x_position=300,
            y_position=200,
            input_ids=[1],
            right_input_id=2,
            outputs=[4],
            setting_input={
                "cache_results": False,
                "auto_generate_selection": True,
                "verify_integrity": False,
                "join_input": {
                    "join_mapping": [
                        {
                            "left_col": "product_name",
                            "right_col": "product_name",
                        }
                    ],
                    "left_select": {
                        "select": [
                            {"old_name": "id", "new_name": "internal_id"},
                            {"old_name": "product_name", "new_name": "internal_name"},
                            {"old_name": "sku"},
                        ]
                    },
                    "right_select": {
                        "select": [
                            {"old_name": "supplier_id"},
                            {"old_name": "product_name", "new_name": "supplier_name"},
                            {"old_name": "supplier"},
                        ]
                    },
                    "how": "inner",
                },
                "auto_keep_all": True,
                "auto_keep_right": True,
                "auto_keep_left": True,
            },
        ),
        FlowfileNode(
            id=4,
            type="filter",
            x_position=550,
            y_position=200,
            input_ids=[3],
            outputs=[5],
            setting_input={
                "cache_results": False,
                "filter_input": {
                    "mode": "advanced",
                    "advanced_filter": "[score] >= 80",
                },
            },
        ),
        FlowfileNode(
            id=5,
            type="explore_data",
            x_position=800,
            y_position=200,
            input_ids=[4],
            outputs=[],
            setting_input={"cache_results": False, "graphic_walker_input": None},
        ),
    ]
    return FlowfileData(
        flowfile_version="0.6.3",
        flowfile_id=1,
        flowfile_name="Product Fuzzy Match",
        flowfile_settings=FlowfileSettings(),
        nodes=nodes,
    )


# ---------------------------------------------------------------------------
# Template Registry
# ---------------------------------------------------------------------------
_TEMPLATE_REGISTRY: dict[str, tuple[FlowTemplateMeta, TemplateBuilder, list[str]]] = {
    "sales_data_overview": (
        FlowTemplateMeta(
            template_id="sales_data_overview",
            name="Sales Data Overview",
            description="Filter sales by region, group by product to see revenue totals, and sort by top sellers.",
            category="Beginner",
            tags=["filter", "group_by", "sort"],
            node_count=5,
            icon="bar_chart",
        ),
        _build_sales_data_overview,
        ["sales_data.csv"],
    ),
    "customer_deduplication": (
        FlowTemplateMeta(
            template_id="customer_deduplication",
            name="Customer Deduplication",
            description="Remove duplicate customer records by email and clean up column names.",
            category="Beginner",
            tags=["unique", "select"],
            node_count=4,
            icon="people",
        ),
        _build_customer_deduplication,
        ["customers.csv"],
    ),
    "employee_directory_cleanup": (
        FlowTemplateMeta(
            template_id="employee_directory_cleanup",
            name="Employee Directory Cleanup",
            description="Create a full name column, filter to active employees, and sort by department.",
            category="Beginner",
            tags=["formula", "filter", "sort"],
            node_count=5,
            icon="badge",
        ),
        _build_employee_directory_cleanup,
        ["employees.csv"],
    ),
    "order_enrichment": (
        FlowTemplateMeta(
            template_id="order_enrichment",
            name="Order Enrichment (Join)",
            description="Join orders with product details, calculate total price, and select final columns.",
            category="Intermediate",
            tags=["join", "formula", "select"],
            node_count=6,
            icon="join_inner",
        ),
        _build_order_enrichment,
        ["orders.csv", "products.csv"],
    ),
    "survey_results_pivot": (
        FlowTemplateMeta(
            template_id="survey_results_pivot",
            name="Survey Results Pivot",
            description="Aggregate survey ratings by question and pivot into a wide-format comparison table.",
            category="Intermediate",
            tags=["group_by", "pivot"],
            node_count=4,
            icon="pivot_table_chart",
        ),
        _build_survey_results_pivot,
        ["survey_responses.csv"],
    ),
    "web_analytics_funnel": (
        FlowTemplateMeta(
            template_id="web_analytics_funnel",
            name="Web Analytics Funnel",
            description="Analyze mobile page views: group by page, sort by visits, and count total records.",
            category="Intermediate",
            tags=["filter", "group_by", "sort", "record_count"],
            node_count=6,
            icon="analytics",
        ),
        _build_web_analytics_funnel,
        ["page_views.csv"],
    ),
    "customer_360": (
        FlowTemplateMeta(
            template_id="customer_360",
            name="Multi-Source Customer 360",
            description="Join customers, orders, and support tickets to build a scored customer profile.",
            category="Advanced",
            tags=["join", "group_by", "formula", "sort"],
            node_count=9,
            icon="hub",
        ),
        _build_customer_360,
        ["customers.csv", "orders.csv", "support_tickets.csv"],
    ),
    "product_fuzzy_match": (
        FlowTemplateMeta(
            template_id="product_fuzzy_match",
            name="Product Catalog Fuzzy Match",
            description="Match internal product catalog against supplier listings using fuzzy name matching.",
            category="Advanced",
            tags=["fuzzy_match", "filter"],
            node_count=5,
            icon="compare_arrows",
        ),
        _build_product_fuzzy_match,
        ["internal_products.csv", "supplier_products.csv"],
    ),
}


def get_all_template_metas() -> list[FlowTemplateMeta]:
    """Returns metadata for all available templates."""
    return [meta for meta, _, _ in _TEMPLATE_REGISTRY.values()]


def get_template_required_files(template_id: str) -> list[str]:
    """Returns the list of CSV filenames required by a template."""
    entry = _TEMPLATE_REGISTRY.get(template_id)
    if entry is None:
        raise ValueError(f"Unknown template: {template_id}")
    return entry[2]


def get_template_flowfile_data(template_id: str, data_dir: Path) -> FlowfileData:
    """Builds and returns the FlowfileData for a template.

    Args:
        template_id: The template identifier.
        data_dir: Local directory containing the CSV data files.

    Returns:
        A FlowfileData object ready to be imported as a flow.
    """
    entry = _TEMPLATE_REGISTRY.get(template_id)
    if entry is None:
        raise ValueError(f"Unknown template: {template_id}")
    _, builder, _ = entry
    return builder(data_dir)
