import polars as pl
import pytest

from flowfile_frame import FlowFrame, col, create_flow_graph, lit, when


class TestJoins:
    """Tests focusing specifically on join operations."""
    graph = create_flow_graph()
    @pytest.fixture
    def customers(self):
        """Create a customer dataset."""
        data = {
            "customer_id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com",
                      "david@example.com", "eve@example.com"],
            "country": ["USA", "Canada", "UK", "Australia", "France"]
        }
        return FlowFrame(data, flow_graph=self.graph)

    @pytest.fixture
    def orders(self):
        """Create an orders dataset."""
        data = {
            "order_id": [101, 102, 103, 104, 105, 106, 107],
            "customer_id": [1, 2, 3, 1, 2, 6, None],  # customer 6 doesn't exist, and one null
            "amount": [150.50, 200.75, 300.25, 120.80, 85.60, 500.00, 75.25],
            "order_date": ["2023-01-15", "2023-02-10", "2023-03-05",
                           "2023-04-20", "2023-05-15", "2023-06-10", "2023-07-05"]
        }
        return FlowFrame(data, flow_graph=self.graph)

    @pytest.fixture
    def products(self):
        """Create a product dataset."""
        data = {
            "product_id": ["P1", "P2", "P3", "P4", "P5"],
            "name": ["Laptop", "Phone", "Tablet", "Monitor", "Printer"],
            "category": ["Electronics", "Electronics", "Electronics", "Computer", "Computer"],
            "price": [1200.00, 800.00, 500.00, 350.00, 280.00]
        }
        return FlowFrame(data, flow_graph=self.graph)

    @pytest.fixture
    def order_items(self):
        """Create an order_items dataset."""
        data = {
            "order_id": [101, 101, 102, 103, 104, 105, 106, 107],
            "product_id": ["P1", "P2", "P3", "P4", "P1", "P2", "P5", "P3"],
            "quantity": [1, 1, 2, 1, 1, 3, 1, 2],
            "price_at_order": [1200.00, 800.00, 500.00, 350.00, 1200.00, 800.00, 280.00, 500.00]
        }
        return FlowFrame(data, flow_graph=self.graph)

    def test_inner_join(self, customers, orders):
        """Test inner join operation."""
        result = customers.join(orders, on="customer_id", how="inner").collect()

        assert len(result) == 5  # 5 matching orders

        assert set(result.columns) == {
            "customer_id",
            "name",
            "email",
            "country",
            "order_id",
            "amount",
            "order_date",
        }

        alice_orders = result.filter(pl.col("name") == "Alice")
        assert len(alice_orders) == 2
        assert set(alice_orders["order_id"].to_list()) == {101, 104}

        assert 106 not in result["order_id"].to_list()  # customer_id=6 doesn't exist
        assert 107 not in result["order_id"].to_list()  # customer_id=None doesn't match

    def test_left_join(self, customers, orders):
        """Test left join operation."""
        result = customers.join(orders, on="customer_id", how="left").collect()

        assert len(result) == 7

        customer_ids = (
            result.filter(pl.col("name").is_not_null())["customer_id"].unique().sort()
        )
        assert customer_ids.to_list() == [1, 2, 3, 4, 5]

        eve_rows = result.filter(pl.col("name") == "Eve")
        assert len(eve_rows) == 1
        assert eve_rows["order_id"][0] is None
        assert eve_rows["amount"][0] is None

    def test_right_join(self, customers, orders):
        """Test right join operation."""
        result = customers.join(orders, on="customer_id", how="right").collect()
        assert len(result) == 7  # All 7 orders (including those without matching customers)

        assert set(result["order_id"].to_list()) == {101, 102, 103, 104, 105, 106, 107}

        order_106 = result.filter(pl.col("order_id") == 106)
        assert len(order_106) == 1
        assert order_106["name"][0] is None
        assert order_106["email"][0] is None
        assert order_106["country"][0] is None

        order_107 = result.filter(pl.col("order_id") == 107)
        assert len(order_107) == 1
        assert order_107["name"][0] is None

    def test_cross_join(self, customers, orders):
        result = customers.join(orders, how='cross')
        assert result.collect().shape[0] == 35
        assert result.get_node_settings().node_type == 'cross_join', 'Should use native cross join method'

    def test_outer_join(self, customers, orders):
        """Test outer join operation."""
        result = customers.join(orders, on="customer_id", how="outer").collect()

        assert len(result) >= 9  # 5 customers + 2 orders with no match

        customer_ids = result.filter(pl.col("name").is_not_null())["customer_id"].unique().sort()
        assert customer_ids.to_list() == [1, 2, 3, 4, 5]

        assert set(result.filter(pl.col("order_id").is_not_null())["order_id"]) == {101, 102, 103, 104, 105, 106, 107}

        eve_rows = result.filter(pl.col("name") == "Eve")
        assert len(eve_rows) == 1
        assert eve_rows["order_id"][0] is None

        order_106 = result.filter(pl.col("order_id") == 106)
        assert len(order_106) == 1
        assert order_106["name"][0] is None

    def test_semi_join(self, customers, orders):
        """Test semi join operation."""
        result = customers.join(orders, on="customer_id", how="semi").collect()

        assert len(result) == 3  # Only customers who have at least one order

        assert set(result["customer_id"].to_list()) == {1, 2, 3}

        assert "order_id" not in result.columns
        assert "amount" not in result.columns

    def test_anti_join(self, customers, orders):
        """Test anti join operation."""
        result = customers.join(orders, on="customer_id", how="anti").collect()

        assert len(result) == 2  # Only customers who have no orders

        assert set(result["customer_id"].to_list()) == {4, 5}  # David and Eve

        assert "order_id" not in result.columns
        assert "amount" not in result.columns

    def test_join_with_different_column_names(self, customers, orders):
        """Test join using different column names."""
        orders_data = {
            "order_id": [101, 102, 103, 104, 105],
            "buyer_id": [1, 2, 3, 1, 2],  # renamed from customer_id
            "amount": [150.50, 200.75, 300.25, 120.80, 85.60]
        }
        modified_orders = FlowFrame(orders_data, flow_graph=self.graph)

        result = customers.join(
            modified_orders,
            left_on="customer_id",
            right_on="buyer_id"
        ).collect()

        assert len(result) == 5  # 5 matching orders

        assert set(result.columns) == {"customer_id", "name", "email", "country",
                                       "order_id", "amount"}

        alice_orders = result.filter(pl.col("name") == "Alice")
        assert len(alice_orders) == 2
        assert set(alice_orders["order_id"].to_list()) == {101, 104}

    def test_join_with_suffix(self, customers, products):
        """Test join with custom suffix."""
        customers_simple = customers.select("customer_id", "name")
        products_simple = products.select("product_id", "name")  # Both have 'name' column

        result = customers_simple.join(
            products_simple,
            how="cross",
            suffix="_product"
        ).collect()

        assert "name" in result.columns  # Original from left side
        assert "name_product" in result.columns  # Renamed from right side

    def test_multi_level_join(self, customers, orders, order_items, products):
        """Test complex multi-level join scenario."""
        customer_orders = customers.join(
            orders,
            on="customer_id",
            how="inner"
        )

        with_items = customer_orders.join(
            order_items,
            on="order_id",
            how="right"
        )

        full_data = with_items.join(
            products,
            left_on="product_id",
            right_on="product_id",
            how="inner"
        )

        result = full_data.collect()

        assert len(result) == len(order_items.collect())

        customer_cols = {"customer_id", "name", "email", "country"}
        order_cols = {"order_id", "amount", "order_date"}
        item_cols = {"product_id", "quantity", "price_at_order"}

        result_cols = set(result.columns)
        for col_set in [customer_cols, order_cols, item_cols]:
            assert col_set.issubset(result_cols)

        order_101 = result.filter(pl.col("order_id") == 101)
        assert len(order_101) == 2  # Two items in this order
        assert order_101["name"][0] == "Alice"  # Customer name
        assert "Laptop" in order_101["name_right"].to_list()  # Product name
        assert "Phone" in order_101["name_right"].to_list()   # Product name

    def test_join_with_expression(self, customers, orders):
        """Test joining with an expression condition."""
        orders_with_year = orders.with_columns(
            col("order_date").cast(pl.Date).dt.year().alias("order_year")
        )

        customers_with_year = customers.with_columns(
            lit(2023).alias("active_year")
        )

        result = customers_with_year.join(
            orders_with_year,
            left_on="active_year",
            right_on="order_year",
            how="inner"
        ).collect()

        # All orders should match since they're all from 2023
        assert len(result) == (len(orders.collect()) * len(customers_with_year.collect()))

        customers_varied_years = customers.with_columns(
            (col("customer_id") + 2020).alias("active_year")  # customer_id 3 -> year 2023
        )

        restricted_result = customers_varied_years.join(
            orders_with_year,
            left_on="active_year",
            right_on="order_year",
            how="inner"
        ).collect()

        # Only orders from 2023 where customer has active_year=2023 should match
        assert len(restricted_result) == len(orders.collect())
        # Customer 3 (Charlie) should be the only match
        assert set(restricted_result["name"].unique().to_list()) == {"Charlie"}

    def test_join_columns_with_cast(self, customers, orders):
        """Test joining with columns that need casting."""
        customers_str_id = customers.with_columns(
            col("customer_id").cast(pl.String).alias("customer_id_str")
        )

        orders_str_id = orders.with_columns(
            col("customer_id").cast(pl.String).alias("customer_id_str")
        ).filter(col("customer_id").is_not_null())  # Filter out null IDs

        flow_frame = customers_str_id.join(
            orders_str_id,
            left_on="customer_id_str",
            right_on="customer_id_str",
            how="inner"
        )
        assert flow_frame.get_node_settings().node_type == 'join', 'Should be able to map this to join node'
        result = flow_frame.collect()
        assert len(result) == 5  # 5 matching orders with non-null IDs

        assert "customer_id" in result.columns
        assert "customer_id_str" in result.columns
        assert "customer_id_right" in result.columns

    def test_join_with_multiple_columns(self, customers, orders):
        """Test join using multiple columns."""
        customers_with_region = customers.with_columns(
            lit("North").alias("region")
        )

        orders_with_region = orders.with_columns(
            (when(col("order_id") > 104).then(lit("South")).otherwise(lit("North"))).alias("region")
        )
        result = customers_with_region.join(
            orders_with_region,
            left_on=["customer_id", col("region")],
            right_on=["customer_id", col("region")],
            how="inner"
        ).collect()

        assert len(result) < len(orders.filter(col("customer_id").is_not_null()).collect())
        assert set(result["region"].unique().to_list()) == {"North"}
        assert all(oid <= 104 for oid in result["order_id"].to_list())

    def test_join_with_expressions(self, customers, orders):
        customers_with_region = customers.with_columns(lit("North").alias("region"))

        orders_with_region = orders.with_columns(
            (when(col("order_id") > 104).then(lit("south")).otherwise(lit("north"))).alias("region")
        )
        result = customers_with_region.join(
            orders_with_region,
            left_on=["customer_id", col("region").str.to_lowercase()],
            right_on=["customer_id", col("region").str.to_lowercase()],
            how="inner"
        ).collect()
        assert len(result) < len(orders.filter(col("customer_id").is_not_null()).collect())
        assert set(result["region"].unique().to_list()) == {"North"}
        assert all(oid <= 104 for oid in result["order_id"].to_list())


if __name__ == "__main__":
    pytest.main([__file__])
