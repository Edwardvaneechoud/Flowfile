"""Canonical identity of an Alteryx tool, from its raw ``Plugin`` string or macro path.

Only names Alteryx itself chose may leave the machine — in telemetry or in a public
issue title: the class name of a plugin in an official namespace, or a shipped macro's
filename. Anything else is a vendor plugin or a user's own macro and collapses to a
fixed key. An official namespace this list does not know yet also collapses; losing
that signal is the price of never guessing. Extend the lists rather than loosen the rule.
"""

CUSTOM_PLUGIN = "custom_plugin"
USER_MACRO = "user_macro"
MACRO_SUFFIX = ".yxmc"
MAX_KEY_LENGTH = 64

OFFICIAL_NAMESPACES = frozenset(
    {"AlteryxBasePluginsGui", "AlteryxGuiToolkit", "AlteryxSpatialPluginsGui", "AlteryxConnectorGui"}
)
SHIPPED_MACROS = frozenset(
    {"cleanse", "countrecords", "imputation", "multifieldbinning", "selectrecords", "weightedavg"}
)


def tool_key(plugin: str) -> str:
    """``"Filter"`` for an official plugin, ``"macro_cleanse"`` for a shipped macro, else a fixed key."""
    namespace, _, rest = plugin.partition(".")
    if rest and namespace in OFFICIAL_NAMESPACES:
        name = rest.rsplit(".", 1)[-1]
        return name if name.isidentifier() and len(name) <= MAX_KEY_LENGTH else CUSTOM_PLUGIN
    if plugin.lower().endswith(MACRO_SUFFIX):
        stem = plugin.replace("\\", "/").rsplit("/", 1)[-1][: -len(MACRO_SUFFIX)].lower()
        return f"macro_{stem}" if stem in SHIPPED_MACROS else USER_MACRO
    return CUSTOM_PLUGIN


def is_official(key: str) -> bool:
    """True when *key* names something Alteryx ships, so a node for it can be requested."""
    return key not in (CUSTOM_PLUGIN, USER_MACRO)
