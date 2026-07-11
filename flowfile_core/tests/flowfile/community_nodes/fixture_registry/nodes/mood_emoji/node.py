import random

import polars as pl

from flowfile import node_designer as nd


class MoodEmojiSettings(nd.NodeSettings):
    mood_config: nd.Section = nd.Section(
        title="Mood Detection",
        description="Configure how to detect the vibe of your data",
        source_column=nd.ColumnSelector(
            label="Analyze This Column",
            multiple=False,
            required=True,
            data_types=nd.Types.Numeric,
        ),
        mood_type=nd.SingleSelect(
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
        ),
        threshold_value=nd.NumericInput(label="Mood Threshold", default=50.0, min_value=0, max_value=100),
        emoji_column_name=nd.TextInput(label="New Emoji Column Name", default="mood_emoji"),
    )
    style_options: nd.Section = nd.Section(
        title="Emoji Style",
        description="Fine-tune your emoji experience",
        emoji_intensity=nd.SingleSelect(
            label="Emoji Intensity",
            options=[
                ("subtle", "Subtle (One emoji)"),
                ("normal", "Normal (1-2 emojis)"),
                ("extra", "Extra (2-3 emojis)"),
                ("maximum", "MAXIMUM OVERDRIVE"),
            ],
            default="normal",
        ),
        add_random_sparkle=nd.ToggleSwitch(
            label="Add Random Sparkles",
            default=True,
            description="Randomly sprinkle a sparkle for extra pizzazz",
        ),
    )


class MoodEmoji(nd.CustomNodeBase):
    node_name: str = "Mood Emoji"
    node_category: str = "Fun"
    title: str = "Mood Emoji"
    intro: str = "Add an emoji column derived from a numeric mood column."

    author: str = "edwardvaneechoud"
    version: str = "1.0.0"
    tags: list[str] = ["fun", "emoji", "text"]

    settings_schema: MoodEmojiSettings = MoodEmojiSettings()

    example_inputs: list[dict[str, list]] = [
        {"name": ["bob", "magret", "fish", "dog"], "value": [21.0, 62.1, 1.2, 20.0]},
    ]
    example_settings: dict[str, dict] = {
        "mood_config": {
            "source_column": "value",
            "mood_type": "performance",
            "threshold_value": 50.0,
            "emoji_column_name": "mood_emoji",
        },
        "style_options": {
            "emoji_intensity": "normal",
            "add_random_sparkle": True,
        },
    }

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        input_df = inputs[0].collect()

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

        return input_df.with_columns(
            pl.col(column_name)
            .map_elements(get_emoji, return_dtype=pl.String)
            .alias(emoji_col_name)
        )
