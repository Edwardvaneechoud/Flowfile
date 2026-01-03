# flowfile_core/flowfile_core/secret_manager/output_scanner.py

"""
Output scanner for detecting accidentally leaked secrets.

This is a safety net that catches honest mistakes like accidentally
including a secret value in a DataFrame column or error message.
"""

import base64
import logging
from typing import Set, List
import polars as pl

logger = logging.getLogger(__name__)


class SecretLeakScanner:
    """
    Scans outputs for accidentally leaked secrets.

    This catches honest mistakes like:
    - df.with_columns(pl.lit(secret))
    - Including secret in error message
    - Accidentally returning secret in a computed column

    Not designed to stop determined attackers - just prevents accidents.
    """

    @staticmethod
    def _build_variants(secrets: Set[str]) -> Set[str]:
        """Build a set of secret variants including common encodings."""
        variants = set()
        for secret in secrets:
            if not secret:
                continue
            variants.add(secret)
            # Common encodings that might be used accidentally
            try:
                variants.add(base64.b64encode(secret.encode()).decode())
            except Exception:
                pass
            try:
                variants.add(base64.urlsafe_b64encode(secret.encode()).decode())
            except Exception:
                pass
            try:
                # Hex encoding
                variants.add(secret.encode().hex())
            except Exception:
                pass
        return variants

    @staticmethod
    def scan_dataframe(
            df: pl.DataFrame,
            secrets: Set[str],
            node_name: str = ""
    ) -> pl.DataFrame:
        """
        Scan DataFrame for secrets and redact if found.

        Args:
            df: The DataFrame to scan
            secrets: Set of secret values that were accessed
            node_name: Name of the node (for logging)

        Returns:
            DataFrame with secrets redacted (if any found)
        """
        if not secrets or df.is_empty():
            return df

        variants = SecretLeakScanner._build_variants(secrets)
        if not variants:
            return df

        result = df
        leaked_columns: List[str] = []

        for col in df.columns:
            # Only scan string columns
            if df[col].dtype != pl.Utf8:
                continue

            # Sample check for performance (don't scan entire huge columns)
            sample_size = min(500, df.height)
            sample = df[col].drop_nulls().head(sample_size).to_list()

            # Check if any sample values contain any variant
            column_has_leak = False
            for value in sample:
                if value:
                    value_str = str(value)
                    if any(variant in value_str for variant in variants):
                        column_has_leak = True
                        break

            if column_has_leak:
                leaked_columns.append(col)
                # Redact all variants from this column
                for variant in variants:
                    result = result.with_columns(
                        pl.col(col).str.replace_all(variant, "***REDACTED***", literal=True)
                    )

        if leaked_columns:
            logger.warning(
                f"⚠️  SECRET LEAK DETECTED in node '{node_name}'\n"
                f"   Columns affected: {leaked_columns}\n"
                f"   Secret values have been redacted from the output.\n"
                f"   Please review your node code to avoid exposing secrets in data."
            )

        return result

    @staticmethod
    def scan_string(text: str, secrets: Set[str]) -> str:
        """
        Scan and redact secrets from a string (e.g., error message).

        Args:
            text: The string to scan
            secrets: Set of secret values that were accessed

        Returns:
            String with secrets redacted
        """
        if not secrets or not text:
            return text

        variants = SecretLeakScanner._build_variants(secrets)
        result = text

        for variant in variants:
            if variant in result:
                result = result.replace(variant, "***REDACTED***")

        return result

    @staticmethod
    def scan_dict(data: dict, secrets: Set[str]) -> dict:
        """
        Recursively scan and redact secrets from a dictionary.

        Args:
            data: The dictionary to scan
            secrets: Set of secret values that were accessed

        Returns:
            Dictionary with secrets redacted
        """
        if not secrets or not data:
            return data

        def _scan_value(value):
            if isinstance(value, str):
                return SecretLeakScanner.scan_string(value, secrets)
            elif isinstance(value, dict):
                return {k: _scan_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [_scan_value(v) for v in value]
            elif isinstance(value, tuple):
                return tuple(_scan_value(v) for v in value)
            return value

        return _scan_value(data)
