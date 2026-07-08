# Custom Node Tutorial: Build an Emoji Generator

This tutorial builds one custom node end to end — an Emoji Generator that maps numeric values to emojis. It is a small but complete example of the [Node Designer](creating-custom-nodes.md) SDK: a multi-section settings schema, type-filtered column selection, and a `process()` method that transforms an incoming frame.

!!! info "What you'll build"
    By the end you will have an "Emoji Generator" node that:

    - Maps a numeric column to mood-based emojis.
    - Offers seven emoji themes (performance, temperature, money, and so on).
    - Includes intensity controls and an optional random sparkle.
    - Uses a two-section settings panel.

## Prerequisites

- Flowfile installed and working (`pip install flowfile`)
- Basic understanding of Python
- Familiarity with Polars (helpful but not required)

## Step 1: Set up your development environment

First, locate your custom-nodes directory:

```bash
# Check if the directory exists
ls ~/.flowfile/user_defined_nodes/

# If it doesn't exist, start Flowfile once to create it
flowfile run ui
```

Create your node file:

```bash
touch ~/.flowfile/user_defined_nodes/emoji_generator.py
```

## Step 2: Import the SDK

Everything comes from the canonical `flowfile.node_designer` import, aliased `nd`:

```python
import random

import polars as pl

from flowfile import node_designer as nd
```

Reference components as `nd.CustomNodeBase`, `nd.Section`, `nd.ColumnSelector`, and so on. This is the only Flowfile import a node file may use — a node runs in an isolated process (the worker or a kernel) that has the SDK but not the rest of Flowfile.

## Step 3: Design the settings schema

We'll create a two-section UI: one for mood detection and one for styling options.

### First section

```python
class EmojiMoodSection(nd.Section):
    source_column: nd.ColumnSelector = nd.ColumnSelector(
        label="Analyze This Column",
        multiple=False,
        required=True,
        data_types=nd.Types.Numeric,  # only numeric columns
    )

    mood_type: nd.SingleSelect = nd.SingleSelect(
        label="Emoji Mood Logic",
        options=[
            ("performance", "Performance Based (High = 😎, Low = 😰)"),
            ("temperature", "Temperature (Hot = 🔥, Cold = 🧊)"),
            ("money", "Money Mode (Rich = 🤑, Poor = 😢)"),
            ("energy", "Energy Level (High = 🚀, Low = 🔋)"),
            ("love", "Love Meter (High = 😍, Low = 💔)"),
            ("chaos", "Pure Chaos (Random emojis)"),
            ("pizza", "Pizza Scale (Everything becomes pizza)"),
        ],
        default="performance",
    )

    threshold_value: nd.NumericInput = nd.NumericInput(
        label="Mood Threshold",
        default=50.0,
        min_value=0,
        max_value=100,
    )

    emoji_column_name: nd.TextInput = nd.TextInput(
        label="New Emoji Column Name",
        default="mood_emoji",
        placeholder="Name your emoji column...",
    )
```

### Second section

```python
class EmojiStyleSection(nd.Section):
    emoji_intensity: nd.SingleSelect = nd.SingleSelect(
        label="Emoji Intensity",
        options=[
            ("subtle", "Subtle (One emoji)"),
            ("normal", "Normal (1-2 emojis)"),
            ("extra", "Extra (2-3 emojis)"),
            ("maximum", "MAXIMUM OVERDRIVE"),
        ],
        default="normal",
    )

    add_random_sparkle: nd.ToggleSwitch = nd.ToggleSwitch(
        label="Add Random Sparkles",
        default=True,
        description="Randomly sprinkle a sparkle for extra pizzazz",
    )
```

### Combine into settings

```python
class EmojiSettings(nd.NodeSettings):
    mood_config: EmojiMoodSection = EmojiMoodSection(
        title="Mood Detection",
        description="Configure how to detect the vibe of your data",
    )

    style_options: EmojiStyleSection = EmojiStyleSection(
        title="Emoji Style",
        description="Fine-tune your emoji experience",
    )
```

## Step 4: Create the node class

```python
class EmojiGenerator(nd.CustomNodeBase):
    # Node metadata — how it appears in Flowfile
    node_name: str = "Emoji Generator"
    node_category: str = "Fun Stuff"   # a "Fun Stuff" group appears in the palette
    title: str = "Emoji Generator"
    intro: str = "Add an emoji column derived from a numeric column."

    # I/O configuration
    number_of_inputs: int = 1
    number_of_outputs: int = 1

    # Link to the settings schema
    settings_schema: EmojiSettings = EmojiSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        # Implemented in the next step
        ...
```

`node_category = "Fun Stuff"` creates a **Fun Stuff** group in the palette; the default category `"Custom"` would put the node under **User Defined Operations** instead.

## Step 5: Implement the processing logic

`process` receives its inputs as a variadic `*inputs`; this node has one input, so it reads `inputs[0]`. Inputs are `pl.LazyFrame`. The emoji mapping uses a per-row Python callback (`map_elements`), which is an eager operation, so we `.collect()` the input first:

```python
def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
    input_df = inputs[0].collect()

    # Read settings once, up front
    column_name = self.settings_schema.mood_config.source_column.value
    mood_type = self.settings_schema.mood_config.mood_type.value
    threshold = self.settings_schema.mood_config.threshold_value.value
    emoji_col_name = self.settings_schema.mood_config.emoji_column_name.value
    intensity = self.settings_schema.style_options.emoji_intensity.value
    add_sparkle = self.settings_schema.style_options.add_random_sparkle.value

    emoji_sets = {
        "performance": {"high": ["😎", "💪", "🏆", "🌟", "💯", "🔥"], "low": ["😰", "😓", "📉", "😢", "💔", "😵"]},
        "temperature": {"high": ["🔥", "🌋", "☀️", "🥵", "♨️", "🏖️"], "low": ["🧊", "❄️", "⛄", "🥶", "🏔️", "🐧"]},
        "money": {"high": ["🤑", "💰", "💎", "🏦", "🪙", "📈"], "low": ["😢", "💸", "📉", "🏚️", "😭", "📊"]},
        "energy": {"high": ["🚀", "⚡", "💥", "🎯", "🏃", "🎪"], "low": ["🔋", "😴", "🛌", "🐌", "🥱", "💤"]},
        "love": {"high": ["😍", "❤️", "💕", "🥰", "💘", "💝"], "low": ["💔", "😢", "😭", "🥀", "😔", "🖤"]},
        "chaos": {"high": ["🦖", "🎸", "🚁", "🎪", "🦜", "🎭"], "low": ["🥔", "🧦", "📎", "🦷", "🧲", "🪣"]},
        "pizza": {"high": ["🍕"], "low": ["🍕"]},
    }

    def get_emoji(value):
        if value is None:
            return "❓"
        emoji_list = emoji_sets.get(mood_type, emoji_sets["performance"])
        if mood_type == "chaos":
            base_emoji = random.choice(emoji_list["high"] + emoji_list["low"])
        elif mood_type == "pizza":
            base_emoji = "🍕"
        else:
            base_emoji = random.choice(emoji_list["high"] if value >= threshold else emoji_list["low"])

        if intensity == "extra":
            base_emoji += random.choice(["✨", "💫", "⭐", ""])
        elif intensity == "maximum":
            base_emoji += "".join(random.choices(["🎉", "🚀", "💥", "🌈", "✨", "🔥"], k=3))

        if add_sparkle and random.random() > 0.7:
            base_emoji += "✨"
        return base_emoji

    result = input_df.with_columns(
        pl.col(column_name)
        .map_elements(get_emoji, return_dtype=pl.String)
        .alias(emoji_col_name)
    )
    return result
```

Returning the eager `DataFrame` is fine — the framework normalizes it. (You could also return `result.lazy()`; the effect is the same.)

## Step 6: Test your node

You have two ways to check the node works.

### In the designer (fastest loop)

Open the node in the [Node Designer](../users/visual-editor/node-designer.md), go to the **Test** tab, paste a small numeric sample, and run. You see the output grid, logs, and any error without touching a flow.

### In a flow

1. **Save the file.** Flowfile hot-reloads the directory — no restart. If Flowfile was already open, click **Rescan** in the Node Designer browser to pick up the new file.
2. **Create a test flow.**
   - Add a "Manual Input" node. The `source_column` selector filters to numeric columns and the threshold logic compares numbers, so use numeric `value` entries (not quoted strings):
    ```
    [
      {"name": "bob", "value": 21},
      {"name": "magret", "value": 62.1},
      {"name": "fish", "value": 1.2},
      {"name": "dog", "value": 20}
    ]
    ```

   - Find your "Emoji Generator" under the **Fun Stuff** group in the palette.
   - Connect it to the manual input.
   - Configure the settings and run.

<details markdown="1">
<summary>Visual overview of the result</summary>

![Flow visualized](../assets/images/developers/emoji_settings.png)

</details>

### Performance notes

1. Prefer Polars expressions over Python loops where the logic allows it, and keep the frame lazy when you can.
2. `map_elements` (used here) runs a Python callback per row — fine for a small demo, slower on large data. It forces the `.collect()` at the top of `process`.
3. Read settings once at the top of `process`, not inside the row callback.

## Complete working example

The complete node is the assembly of Steps 3–5: the two `Section` classes and `EmojiSettings` from Step 3, the `EmojiGenerator` class from Step 4, and its `process()` body from Step 5, in one file. Save it as `~/.flowfile/user_defined_nodes/emoji_generator.py`.

## Recap

You built a custom node with a two-section settings panel, type-filtered column selection, and a `process()` method that maps values to emojis. See [Creating Custom Nodes](creating-custom-nodes.md) for the full component catalog, execution environments, multi-input/multi-output nodes, and the `SecretSelector` for API-backed nodes.
