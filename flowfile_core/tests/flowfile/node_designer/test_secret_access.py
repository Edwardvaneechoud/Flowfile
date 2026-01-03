"""
Tests for secret access functionality in custom nodes.

This module tests:
- SecretSelector UI component and .secret_value property
- SecretLeakScanner output scanning
- NodeSettings.set_secret_context() for injecting execution context
- NodeSettings helper methods (get_value, get_all_components)
- End-to-end integration with flow graphs using real database
"""
import pytest
import polars as pl
from polars.testing import assert_frame_equal
from typing import Dict, Any, List
from pydantic import SecretStr
import uuid

from flowfile_core.flowfile.node_designer import (
    CustomNodeBase,
    NodeSettings,
    Section,
    TextInput,
    NumericInput,
    ToggleSwitch,
    SecretSelector,
    AvailableSecrets,
)
from flowfile_core.flowfile.node_designer.output_scanner import SecretLeakScanner
from flowfile_core.flowfile.node_designer.ui_components import FlowfileInComponent

# Database and secret manager imports
from flowfile_core.database.connection import get_db_context
from flowfile_core.database import models as db_models
from flowfile_core.secret_manager.secret_manager import (
    store_secret,
    get_encrypted_secret,
    decrypt_secret,
    encrypt_secret,
)
from flowfile_core.auth.models import SecretInput

# Flow graph imports for integration tests
from flowfile_core.configs.node_store import add_to_custom_node_store
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas


# =============================================================================
# Test Utilities
# =============================================================================

def create_flowfile_handler() -> FlowfileHandler:
    handler = FlowfileHandler()
    return handler


def create_graph(flow_id: int = 1, execution_mode: str = 'Development') -> FlowGraph:
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(
        flow_id=flow_id,
        name=f'flow_{flow_id}',
        path='.',
        execution_mode=execution_mode
    ))
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data: List[Dict], node_id: int = 1) -> FlowGraph:
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type='manual_input'
    )
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData.from_pylist(data)
    )
    graph.add_manual_input(input_file)
    return graph


def add_custom_node_to_graph(
        graph: FlowGraph,
        custom_node_class: type,
        node_id: int,
        settings: Dict,
        user_id: int = None
) -> FlowGraph:
    """Helper to add a custom node to a graph with settings."""
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type=custom_node_class().item,
        is_user_defined=True
    )
    graph.add_node_promise(node_promise)
    user_defined_node = custom_node_class.from_settings(settings)
    node_settings = input_schema.UserDefinedNode(
        flow_id=graph.flow_id,
        node_id=node_id,
        settings=settings,
        is_user_defined=True,
        user_id=user_id
    )
    graph.add_user_defined_node(
        custom_node=user_defined_node,
        user_defined_node_settings=node_settings
    )
    return graph


def handle_run_info(run_info):
    """Helper to handle run results and raise on failure."""
    if not run_info.success:
        raise AssertionError(f"Graph run failed: {run_info.error_message}")


def get_test_user_id() -> int:
    """Get the ID of the local_user created by setup_test_db fixture."""
    with get_db_context() as db:
        user = db.query(db_models.User).filter(
            db_models.User.username == "local_user"
        ).first()
        if user:
            return user.id
        raise ValueError("local_user not found - ensure setup_test_db fixture ran")


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_secret_value() -> str:
    """A sample secret value for testing."""
    return "super-secret-api-key-12345"


@pytest.fixture
def sample_secrets_set(sample_secret_value) -> set:
    """A set of secret values for testing scanning."""
    return {sample_secret_value, "another-secret-password"}


@pytest.fixture
def test_user_id() -> int:
    """Get or create a test user and return their ID."""
    return get_test_user_id()


@pytest.fixture
def stored_secret(test_user_id) -> tuple[str, str]:
    """
    Creates a real secret in the database for testing.
    Returns (secret_name, secret_value).
    Cleans up after the test.
    """
    secret_name = f"test_secret_{uuid.uuid4().hex[:8]}"
    secret_value = "my-super-secret-api-key-xyz123"

    with get_db_context() as db:
        secret_input = SecretInput(name=secret_name, value=SecretStr(secret_value))
        store_secret(db, secret_input, test_user_id)

    yield secret_name, secret_value

    # Cleanup
    with get_db_context() as db:
        db_secret = db.query(db_models.Secret).filter(
            db_models.Secret.name == secret_name,
            db_models.Secret.user_id == test_user_id
        ).first()
        if db_secret:
            db.delete(db_secret)
            db.commit()


@pytest.fixture
def settings_with_secret_selector():
    """NodeSettings with a SecretSelector component."""
    return NodeSettings(
        config=Section(
            title="API Configuration",
            api_key=SecretSelector(
                label="API Key",
                description="Select your API key",
                required=True
            ),
            endpoint=TextInput(
                label="Endpoint URL",
                default="https://api.example.com"
            )
        )
    )


@pytest.fixture
def SecretUsingNode():
    """A custom node that uses SecretSelector.secret_value property."""
    class APIConnectorNode(CustomNodeBase):
        node_name: str = "API Connector"
        node_group: str = "custom"
        node_category: str = "Integration"
        intro: str = "Connects to an API using secrets"
        title: str = "API Connector"
        number_of_inputs: int = 1
        number_of_outputs: int = 1
        settings_schema: NodeSettings = NodeSettings(
            config=Section(
                title="API Configuration",
                api_key=SecretSelector(
                    label="API Key",
                    description="Select your API key"
                ),
                prefix=TextInput(
                    label="Prefix",
                    default="authenticated_"
                )
            )
        )

        def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
            if not inputs:
                return pl.DataFrame()

            input_df = inputs[0]

            # Access the secret using .secret_value property (consistent with other .value patterns)
            api_key = self.settings_schema.config.api_key.secret_value

            if api_key is None:
                # No secret configured, pass through
                return input_df

            # Use the secret (just mark that we authenticated, don't leak the secret!)
            prefix = self.settings_schema.config.prefix.value or "authenticated_"
            return input_df.with_columns(
                pl.lit(f"{prefix}success").alias("auth_status")
            )

    return APIConnectorNode


@pytest.fixture
def LeakyNode():
    """A custom node that accidentally leaks a secret in its output."""
    class SecretLeakerNode(CustomNodeBase):
        node_name: str = "Secret Leaker"
        node_group: str = "custom"
        node_category: str = "Testing"
        intro: str = "Accidentally leaks secrets (for testing)"
        title: str = "Secret Leaker"
        number_of_inputs: int = 1
        number_of_outputs: int = 1
        settings_schema: NodeSettings = NodeSettings(
            config=Section(
                title="Config",
                api_key=SecretSelector(label="API Key")
            )
        )

        def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
            if not inputs:
                return pl.DataFrame()
            input_df = inputs[0]

            # Access secret using .secret_value property
            api_key = self.settings_schema.config.api_key.secret_value

            if api_key:
                # BAD: Accidentally including the secret in output!
                return input_df.with_columns(
                    pl.lit(f"key={api_key.get_secret_value()}").alias("leaked_data")
                )
            return input_df

    return SecretLeakerNode


class TestSecretSelector:
    """Tests for the SecretSelector UI component."""

    def test_initialization_defaults(self):
        """Tests that SecretSelector initializes with correct defaults."""
        selector = SecretSelector(label="My Secret")

        assert selector.label == "My Secret"
        assert selector.component_type == "SecretSelector"
        assert selector.options == AvailableSecrets
        assert selector.required is False
        assert selector.description is None
        assert selector.input_type == "secret"
        assert selector.name_prefix is None

    def test_initialization_with_all_params(self):
        """Tests SecretSelector with all parameters specified."""
        selector = SecretSelector(
            label="API Key",
            description="Your API key for authentication",
            required=True,
            name_prefix="api_"
        )

        assert selector.label == "API Key"
        assert selector.description == "Your API key for authentication"
        assert selector.required is True
        assert selector.name_prefix == "api_"

    def test_model_dump_signals_frontend(self):
        """Tests that model_dump produces frontend-friendly schema."""
        selector = SecretSelector(label="Secret", name_prefix="db_")
        data = selector.model_dump()

        assert data["component_type"] == "SecretSelector"
        assert data["options"] == {"__type__": "AvailableSecrets"}
        assert data["name_prefix"] == "db_"
        assert data["input_type"] == "secret"

    def test_set_and_get_value(self):
        """Tests setting and getting values on SecretSelector."""
        selector = SecretSelector(label="API Key")

        assert selector.value is None

        selector.set_value("my_api_key_secret")
        assert selector.value == "my_api_key_secret"


# =============================================================================
# Tests for SecretLeakScanner
# =============================================================================

class TestSecretLeakScanner:
    """Tests for the SecretLeakScanner utility class."""

    def test_build_variants_includes_original(self, sample_secret_value):
        """Tests that _build_variants includes the original secret."""
        secrets = {sample_secret_value}
        variants = SecretLeakScanner._build_variants(secrets)

        assert sample_secret_value in variants

    def test_build_variants_includes_base64(self, sample_secret_value):
        """Tests that _build_variants includes base64 encoding."""
        import base64
        secrets = {sample_secret_value}
        variants = SecretLeakScanner._build_variants(secrets)

        expected_b64 = base64.b64encode(sample_secret_value.encode()).decode()
        assert expected_b64 in variants

    def test_build_variants_includes_hex(self, sample_secret_value):
        """Tests that _build_variants includes hex encoding."""
        secrets = {sample_secret_value}
        variants = SecretLeakScanner._build_variants(secrets)

        expected_hex = sample_secret_value.encode().hex()
        assert expected_hex in variants

    def test_build_variants_handles_empty_secrets(self):
        """Tests that _build_variants handles empty secret sets."""
        variants = SecretLeakScanner._build_variants(set())
        assert variants == set()

    def test_build_variants_handles_empty_string(self):
        """Tests that _build_variants skips empty strings."""
        variants = SecretLeakScanner._build_variants({""})
        assert "" not in variants

    def test_scan_dataframe_no_secrets(self):
        """Tests scanning when no secrets are provided."""
        df = pl.DataFrame({"col": ["value1", "value2"]})
        result = SecretLeakScanner.scan_dataframe(df, set())

        assert_frame_equal(result, df)

    def test_scan_dataframe_empty_df(self, sample_secrets_set):
        """Tests scanning an empty DataFrame."""
        df = pl.DataFrame({"col": []})
        result = SecretLeakScanner.scan_dataframe(df, sample_secrets_set)

        assert result.is_empty()

    def test_scan_dataframe_no_leaks(self, sample_secrets_set):
        """Tests scanning a DataFrame with no secret leaks."""
        df = pl.DataFrame({
            "name": ["Alice", "Bob"],
            "value": ["public_data", "more_public"]
        })
        result = SecretLeakScanner.scan_dataframe(df, sample_secrets_set)

        assert_frame_equal(result, df)

    def test_scan_dataframe_detects_and_redacts_leak(self, sample_secret_value):
        """Tests that leaked secrets are detected and redacted."""
        df = pl.DataFrame({
            "name": ["Alice", "Bob"],
            "data": [f"Token: {sample_secret_value}", "Clean data"]
        })

        result = SecretLeakScanner.scan_dataframe(
            df,
            {sample_secret_value},
            node_name="TestNode"
        )

        # Secret should be redacted
        assert sample_secret_value not in str(result["data"].to_list())
        assert "***REDACTED***" in result["data"].to_list()[0]
        # Other data should be unchanged
        assert result["name"].to_list() == ["Alice", "Bob"]
        assert result["data"].to_list()[1] == "Clean data"

    def test_scan_dataframe_skips_non_string_columns(self, sample_secret_value):
        """Tests that non-string columns are not scanned."""
        df = pl.DataFrame({
            "numbers": [123, 456],
            "floats": [1.5, 2.5],
            "text": ["safe", "data"]
        })

        result = SecretLeakScanner.scan_dataframe(df, {sample_secret_value})
        assert_frame_equal(result, df)

    def test_scan_dataframe_redacts_base64_encoded_leak(self, sample_secret_value):
        """Tests that base64-encoded secrets are also redacted."""
        import base64
        encoded = base64.b64encode(sample_secret_value.encode()).decode()

        df = pl.DataFrame({
            "data": [f"Encoded: {encoded}"]
        })

        result = SecretLeakScanner.scan_dataframe(df, {sample_secret_value})

        assert encoded not in str(result["data"].to_list())
        assert "***REDACTED***" in result["data"].to_list()[0]

    def test_scan_string_no_secrets(self):
        """Tests scanning a string with no secrets."""
        result = SecretLeakScanner.scan_string("Hello world", set())
        assert result == "Hello world"

    def test_scan_string_empty_string(self, sample_secrets_set):
        """Tests scanning an empty string."""
        result = SecretLeakScanner.scan_string("", sample_secrets_set)
        assert result == ""

    def test_scan_string_with_leak(self, sample_secret_value):
        """Tests that secrets in strings are redacted."""
        text = f"Error: API key {sample_secret_value} is invalid"
        result = SecretLeakScanner.scan_string(text, {sample_secret_value})

        assert sample_secret_value not in result
        assert "***REDACTED***" in result
        assert "Error: API key" in result

    def test_scan_string_multiple_occurrences(self, sample_secret_value):
        """Tests redaction of multiple occurrences."""
        text = f"Key: {sample_secret_value}, Again: {sample_secret_value}"
        result = SecretLeakScanner.scan_string(text, {sample_secret_value})

        assert result.count("***REDACTED***") == 2
        assert sample_secret_value not in result

    def test_scan_dict_simple(self, sample_secret_value):
        """Tests scanning a simple dictionary."""
        data = {
            "message": f"Key is {sample_secret_value}",
            "status": "error"
        }
        result = SecretLeakScanner.scan_dict(data, {sample_secret_value})

        assert sample_secret_value not in result["message"]
        assert "***REDACTED***" in result["message"]
        assert result["status"] == "error"

    def test_scan_dict_nested(self, sample_secret_value):
        """Tests scanning nested dictionaries."""
        data = {
            "outer": {
                "inner": {
                    "secret": sample_secret_value
                }
            }
        }
        result = SecretLeakScanner.scan_dict(data, {sample_secret_value})

        assert result["outer"]["inner"]["secret"] == "***REDACTED***"

    def test_scan_dict_with_lists(self, sample_secret_value):
        """Tests scanning dictionaries containing lists."""
        data = {
            "items": ["safe", sample_secret_value, "also_safe"]
        }
        result = SecretLeakScanner.scan_dict(data, {sample_secret_value})

        assert result["items"][0] == "safe"
        assert result["items"][1] == "***REDACTED***"
        assert result["items"][2] == "also_safe"

    def test_scan_dict_with_tuples(self, sample_secret_value):
        """Tests scanning dictionaries containing tuples."""
        data = {
            "coords": ("x", sample_secret_value, "z")
        }
        result = SecretLeakScanner.scan_dict(data, {sample_secret_value})

        assert isinstance(result["coords"], tuple)
        assert result["coords"][1] == "***REDACTED***"

    def test_scan_dict_empty(self, sample_secrets_set):
        """Tests scanning empty dictionaries."""
        result = SecretLeakScanner.scan_dict({}, sample_secrets_set)
        assert result == {}

    def test_scan_dict_no_secrets(self):
        """Tests scanning when no secrets provided."""
        data = {"key": "value"}
        result = SecretLeakScanner.scan_dict(data, set())
        assert result == data


# =============================================================================
# Tests for NodeSettings Helper Methods
# =============================================================================

class TestNodeSettingsHelperMethods:
    """Tests for the new get_value and get_all_components methods."""

    @pytest.fixture
    def complex_settings(self):
        """Creates NodeSettings with multiple sections and components."""
        return NodeSettings(
            section_a=Section(
                title="Section A",
                text_field=TextInput(label="Text", default="hello"),
                number_field=NumericInput(label="Number", default=42)
            ),
            section_b=Section(
                title="Section B",
                toggle_field=ToggleSwitch(label="Toggle", default=True),
                secret_field=SecretSelector(label="Secret")
            )
        )

    def test_get_value_from_section(self, complex_settings):
        """Tests getting a value from within a section."""
        assert complex_settings.get_value("text_field") == "hello"
        assert complex_settings.get_value("number_field") == 42
        assert complex_settings.get_value("toggle_field") is True

    def test_get_value_nonexistent_field(self, complex_settings):
        """Tests that get_value returns None for nonexistent fields."""
        assert complex_settings.get_value("nonexistent") is None

    def test_get_value_after_update(self, complex_settings):
        """Tests get_value after updating a field."""
        complex_settings.section_a.text_field.set_value("updated")
        assert complex_settings.get_value("text_field") == "updated"

    def test_get_all_components(self, complex_settings):
        """Tests retrieving all components from settings."""
        components = complex_settings.get_all_components()

        assert "text_field" in components
        assert "number_field" in components
        assert "toggle_field" in components
        assert "secret_field" in components

        assert isinstance(components["text_field"], TextInput)
        assert isinstance(components["secret_field"], SecretSelector)

    def test_get_all_components_empty_settings(self):
        """Tests get_all_components on empty settings."""
        settings = NodeSettings()
        components = settings.get_all_components()
        assert components == {}


# =============================================================================
# Tests for CustomNodeBase and NodeSettings Secret Context
# =============================================================================

class TestCustomNodeBaseSecretMethods:
    """Tests for secret-related methods on CustomNodeBase and NodeSettings."""

    @pytest.fixture
    def node_class_with_secrets(self):
        """Creates a node class with secret selectors."""
        class SecretNode(CustomNodeBase):
            node_name: str = "Secret Node"
            node_group: str = "custom"
            intro: str = "Node with secrets"
            title: str = "Secret Node"
            number_of_inputs: int = 1
            number_of_outputs: int = 1
            settings_schema: NodeSettings = NodeSettings(
                config=Section(
                    title="Config",
                    api_key=SecretSelector(label="API Key"),
                    db_password=SecretSelector(label="DB Password"),
                    regular_input=TextInput(label="Regular", default="value")
                )
            )

            def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
                return inputs[0] if inputs else pl.DataFrame()

        return SecretNode

    def testset_execution_context(self, node_class_with_secrets):
        """Tests setting the execution context on CustomNodeBase."""
        node = node_class_with_secrets()
        node.set_execution_context(user_id=123)

        assert node._user_id == 123
        assert node.accessed_secrets == set()

    def testset_execution_context_clears_accessed_secrets(self, node_class_with_secrets):
        """Tests that setting context clears previously accessed secrets."""
        node = node_class_with_secrets()
        node._accessed_secrets = {"old_secret"}

        node.set_execution_context(user_id=456)

        assert node.accessed_secrets == set()

    def test_get_accessed_secrets_returns_copy(self, node_class_with_secrets):
        """Tests that get_accessed_secrets returns a copy."""
        node = node_class_with_secrets()
        node._accessed_secrets = {"secret1", "secret2"}

        accessed = node.get_accessed_secrets()
        accessed.add("secret3")  # Modify the copy

        # Original should be unchanged
        assert "secret3" not in node._accessed_secrets

    def test_get_secret_names(self, node_class_with_secrets):
        """Tests getting names of SecretSelector fields."""
        node = node_class_with_secrets()
        secret_names = node.get_secret_names()

        assert "api_key" in secret_names
        assert "db_password" in secret_names
        assert "regular_input" not in secret_names

    def test_get_secret_names_empty_schema(self):
        """Tests get_secret_names with no settings schema."""

        class EmptyNode(CustomNodeBase):
            node_name: str = "Empty"
            settings_schema: NodeSettings | None = None  # Need the type annotation!

            def process(self, *inputs):
                return inputs[0] if inputs else pl.DataFrame()

        node = EmptyNode()
        assert node.get_secret_names() == []

class TestNodeSettingsSecretContext:
    """Tests for NodeSettings.set_secret_context() method."""

    @pytest.fixture
    def settings_with_multiple_secrets(self):
        """Creates NodeSettings with multiple SecretSelectors."""
        return NodeSettings(
            config=Section(
                title="Config",
                api_key=SecretSelector(label="API Key"),
                db_password=SecretSelector(label="DB Password"),
                regular_input=TextInput(label="Regular", default="value")
            )
        )

    def test_set_secret_context_injects_user_id(self, settings_with_multiple_secrets):
        """Tests that set_secret_context injects user_id into SecretSelectors."""
        accessed_secrets = set()
        settings_with_multiple_secrets.set_secret_context(
            user_id=42,
            accessed_secrets=accessed_secrets
        )

        # Check that both SecretSelectors got the context
        assert settings_with_multiple_secrets.config.api_key._user_id == 42
        assert settings_with_multiple_secrets.config.db_password._user_id == 42

    def test_set_secret_context_injects_tracking_set(self, settings_with_multiple_secrets):
        """Tests that set_secret_context injects the tracking set."""
        accessed_secrets = set()
        settings_with_multiple_secrets.set_secret_context(
            user_id=42,
            accessed_secrets=accessed_secrets
        )

        # Both should reference the same set
        assert settings_with_multiple_secrets.config.api_key._accessed_secrets is accessed_secrets
        assert settings_with_multiple_secrets.config.db_password._accessed_secrets is accessed_secrets

    def test_regular_inputs_not_affected(self, settings_with_multiple_secrets):
        """Tests that non-SecretSelector components are not affected."""
        accessed_secrets = set()
        settings_with_multiple_secrets.set_secret_context(
            user_id=42,
            accessed_secrets=accessed_secrets
        )

        # TextInput should not have these attributes
        regular = settings_with_multiple_secrets.config.regular_input
        assert not hasattr(regular, '_user_id') or regular._user_id is None


class TestSecretSelectorSecretValue:
    """Tests for SecretSelector.secret_value property."""

    def test_secret_value_without_context_raises(self):
        """Tests that accessing secret_value without context raises error."""
        selector = SecretSelector(label="API Key")
        selector.set_value("some_secret_name")

        with pytest.raises(ValueError, match="Secret can only be accessed during node execution"):
            _ = selector.secret_value

    def test_secret_value_returns_none_when_no_value_set(self, test_user_id):
        """Tests that secret_value returns None when no secret name is set."""
        selector = SecretSelector(label="API Key")
        selector.set_execution_context(user_id=test_user_id, accessed_secrets=set())
        # Don't set a value

        result = selector.secret_value
        assert result is None

    def test_secret_value_with_real_database(self, stored_secret, test_user_id):
        """Tests secret_value property with actual database secret."""
        secret_name, secret_value = stored_secret

        selector = SecretSelector(label="API Key")
        accessed_secrets = set()
        selector.set_execution_context(user_id=test_user_id, accessed_secrets=accessed_secrets)
        selector.set_value(secret_name)

        result = selector.secret_value

        assert result is not None
        assert result.get_secret_value() == secret_value
        # Check that the secret was tracked
        assert secret_value in accessed_secrets

    def test_secret_value_not_found_raises(self, test_user_id):
        """Tests secret_value raises when secret doesn't exist in database."""
        selector = SecretSelector(label="API Key")
        selector.set_execution_context(user_id=test_user_id, accessed_secrets=set())
        selector.set_value("nonexistent_secret_xyz")

        with pytest.raises(ValueError, match="Secret 'nonexistent_secret_xyz' not found"):
            _ = selector.secret_value

    def test_secret_value_tracks_access(self, stored_secret, test_user_id):
        """Tests that accessing secret_value adds to accessed_secrets."""
        secret_name, secret_value = stored_secret

        selector = SecretSelector(label="API Key")
        accessed_secrets = set()
        selector.set_execution_context(user_id=test_user_id, accessed_secrets=accessed_secrets)
        selector.set_value(secret_name)

        # Access the secret
        _ = selector.secret_value

        # The actual decrypted value should be in the tracking set
        assert secret_value in accessed_secrets


# =============================================================================
# Tests for Secret Manager Functions
# =============================================================================

class TestSecretManagerIntegration:
    """Tests for the secret manager functions with real database."""

    def test_store_and_retrieve_secret(self, test_user_id):
        """Tests storing and retrieving a secret."""
        secret_name = f"test_secret_{uuid.uuid4().hex[:8]}"
        secret_value = "test-secret-value-12345"

        try:
            # Store
            with get_db_context() as db:
                secret_input = SecretInput(name=secret_name, value=SecretStr(secret_value))
                stored = store_secret(db, secret_input, test_user_id)
                assert stored.name == secret_name

            # Retrieve
            encrypted = get_encrypted_secret(test_user_id, secret_name)
            assert encrypted is not None

            # Decrypt
            decrypted = decrypt_secret(encrypted)
            assert decrypted.get_secret_value() == secret_value

        finally:
            # Cleanup
            with get_db_context() as db:
                db_secret = db.query(db_models.Secret).filter(
                    db_models.Secret.name == secret_name,
                    db_models.Secret.user_id == test_user_id
                ).first()
                if db_secret:
                    db.delete(db_secret)
                    db.commit()

    def test_get_nonexistent_secret(self, test_user_id):
        """Tests that getting a nonexistent secret returns None."""
        result = get_encrypted_secret(test_user_id, "definitely_not_a_real_secret")
        assert result is None

    def test_encrypt_decrypt_roundtrip(self):
        """Tests that encrypt/decrypt is a proper roundtrip."""
        original = "my-secret-password-!@#$%"
        encrypted = encrypt_secret(original)
        decrypted = decrypt_secret(encrypted)

        assert decrypted.get_secret_value() == original
        assert encrypted != original  # Should be different


# =============================================================================
# Integration Tests with Flow Graph
# =============================================================================

class TestSecretAccessFlowGraphIntegration:
    """Integration tests for secret access in flow graph execution."""

    def test_node_with_secret_in_graph(self, SecretUsingNode, stored_secret, test_user_id):
        """Tests a custom node that uses secrets in a full graph execution."""
        secret_name, secret_value = stored_secret
        add_to_custom_node_store(SecretUsingNode)

        graph = create_graph()
        add_manual_input(graph, [{"id": 1}, {"id": 2}], node_id=1)

        settings = {
            "config": {
                "api_key": secret_name,
                "prefix": "auth_"
            }
        }
        add_custom_node_to_graph(
            graph, SecretUsingNode, node_id=2,
            settings=settings, user_id=test_user_id
        )
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(2).get_resulting_data()
        result_dict = result.to_dict()

        # Should have the auth_status column but NOT the actual secret
        assert "auth_status" in result_dict
        assert result_dict["auth_status"] == ["auth_success", "auth_success"]
        # Secret should NOT be in the output
        assert secret_value not in str(result_dict)

    def test_leaky_node_gets_secret_redacted(self, LeakyNode, stored_secret, test_user_id):
        """Tests that the output scanner catches and redacts leaked secrets."""
        secret_name, secret_value = stored_secret
        add_to_custom_node_store(LeakyNode)
        graph = create_graph()
        add_manual_input(graph, [{"id": 1}], node_id=1)

        settings = {
            "config": {
                "api_key": secret_name
            }
        }
        add_custom_node_to_graph(
            graph, LeakyNode, node_id=2,
            settings=settings, user_id=test_user_id
        )
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(2).get_resulting_data()
        result_dict = result.to_dict()

        # The leaked_data column should exist but secret should be redacted
        assert "leaked_data" in result_dict
        leaked_value = result_dict["leaked_data"][0]
        assert secret_value not in leaked_value
        assert "***REDACTED***" in leaked_value

    def test_node_without_secret_passes_through(self, SecretUsingNode, test_user_id):
        """Tests that a node without a secret configured just passes data through."""
        add_to_custom_node_store(SecretUsingNode)

        graph = create_graph()
        add_manual_input(graph, [{"id": 1}, {"id": 2}], node_id=1)

        # Don't set api_key
        settings = {
            "config": {
                "prefix": "test_"
            }
        }
        add_custom_node_to_graph(
            graph, SecretUsingNode, node_id=2,
            settings=settings, user_id=test_user_id
        )
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        run_result = graph.run_graph()
        handle_run_info(run_result)

        result = graph.get_node(2).get_resulting_data()
        result_dict = result.to_dict()

        # Should just pass through without auth_status column
        assert "id" in result_dict
        assert result_dict["id"] == [1, 2]


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_scanner_handles_none_values_in_dataframe(self):
        """Tests scanner handles null values gracefully."""
        df = pl.DataFrame({
            "data": ["value1", None, "value2"]
        })

        result = SecretLeakScanner.scan_dataframe(df, {"some_secret"})

        # Should complete without error
        assert result.height == 3

    def test_scanner_handles_special_characters_in_secrets(self):
        """Tests scanner handles secrets with special regex characters."""
        special_secret = "password+with.special*chars?"
        df = pl.DataFrame({
            "data": [f"Key: {special_secret}"]
        })

        result = SecretLeakScanner.scan_dataframe(df, {special_secret})

        # Should redact even with special characters
        assert special_secret not in result["data"].to_list()[0]

    def test_scanner_performance_large_dataframe(self):
        """Tests that scanner uses sampling for large DataFrames."""
        # Create a large DataFrame
        large_df = pl.DataFrame({
            "data": ["safe_value"] * 10000
        })

        # Should complete quickly due to sampling (only checks first 500)
        result = SecretLeakScanner.scan_dataframe(large_df, {"not_present"})

        assert result.height == 10000

    def test_get_value_with_direct_field(self):
        """Tests get_value works with fields directly on NodeSettings."""
        class DirectSettings(NodeSettings):
            direct_field: TextInput = TextInput(label="Direct", default="direct_value")

        settings = DirectSettings()
        # This tests the model_fields path in get_value
        assert settings.get_value("direct_field") == "direct_value"


# =============================================================================
# Security-Focused Tests
# =============================================================================

class TestSecurityBehavior:
    """Tests focused on security properties."""

    def test_secrets_not_logged(self):
        """Tests that the scanner doesn't include secrets in logs."""
        import logging
        import io

        # Set up log capture
        log_stream = io.StringIO()
        handler = logging.StreamHandler(log_stream)
        handler.setLevel(logging.WARNING)

        logger = logging.getLogger('flowfile_core.flowfile.node_designer.output_scanner')
        logger.addHandler(handler)

        try:
            secret = "super-secret-123"
            df = pl.DataFrame({"data": [f"leak: {secret}"]})

            SecretLeakScanner.scan_dataframe(df, {secret}, node_name="TestNode")

            log_output = log_stream.getvalue()

            # Logs should not contain the actual secret
            assert secret not in log_output
            # But should mention there was a leak
            assert "SECRET LEAK DETECTED" in log_output or log_output == ""
        finally:
            logger.removeHandler(handler)

    def test_secret_str_prevents_accidental_exposure(self):
        """Tests that SecretStr is used properly to prevent exposure."""
        secret_str = SecretStr("my-secret")

        # repr should not show the value
        assert "my-secret" not in repr(secret_str)
        assert "my-secret" not in str(secret_str)

        # Only get_secret_value reveals it
        assert secret_str.get_secret_value() == "my-secret"

    def test_encrypted_value_differs_from_original(self, test_user_id):
        """Tests that encrypted secrets don't contain the original value."""
        secret_name = f"test_encryption_{uuid.uuid4().hex[:8]}"
        secret_value = "plaintext-secret-value"

        try:
            with get_db_context() as db:
                secret_input = SecretInput(name=secret_name, value=SecretStr(secret_value))
                stored = store_secret(db, secret_input, test_user_id)

                # The encrypted value should not contain the plaintext
                assert secret_value not in stored.encrypted_value
        finally:
            with get_db_context() as db:
                db_secret = db.query(db_models.Secret).filter(
                    db_models.Secret.name == secret_name,
                    db_models.Secret.user_id == test_user_id
                ).first()
                if db_secret:
                    db.delete(db_secret)
                    db.commit()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])