from flowfile_core.flowfile.code_generator.base import ConverterMixinBase
from flowfile_core.schemas import input_schema, transform_schema


class JoinHandlersMixin(ConverterMixinBase):
    """Join node handlers (standard / semi-anti / cross-join, key transforms, post-processing)."""

    def _handle_join(self, settings: input_schema.NodeJoin, var_name: str, input_vars: dict[str, str]) -> None:
        """Handle join nodes by routing to appropriate join type handler.

        This is the main entry point for processing join operations. It determines
        the type of join and delegates to the appropriate handler method.

        Args:
            settings: NodeJoin settings containing join configuration
            var_name: Name of the variable to store the joined DataFrame
            input_vars: Dictionary mapping input names to DataFrame variable names

        Returns:
            None: Modifies internal state by adding generated code
        """
        left_df = input_vars.get("main", input_vars.get("main_0", "df_left"))
        right_df = input_vars.get("right", input_vars.get("main_1", "df_right"))
        if left_df == right_df:
            right_df = "df_right"
            self._add_code(f"{right_df} = {left_df}")

        if settings.join_input.how in ("semi", "anti"):
            self._handle_semi_anti_join(settings, var_name, left_df, right_df)
        else:
            self._handle_standard_join(settings, var_name, left_df, right_df)

    def _handle_semi_anti_join(
        self, settings: input_schema.NodeJoin, var_name: str, left_df: str, right_df: str
    ) -> None:
        """Handle semi and anti joins which only return rows from the left DataFrame.

        Semi joins return rows from left DataFrame that have matches in right.
        Anti joins return rows from left DataFrame that have no matches in right.
        These joins are simpler as they don't require column management from right DataFrame.

        Args:
            settings: NodeJoin settings containing join configuration
            var_name: Name of the variable to store the result
            left_df: Variable name of the left DataFrame
            right_df: Variable name of the right DataFrame

        Returns:
            None: Modifies internal state by adding generated code
        """
        join_input_manager = transform_schema.JoinInputManager(settings.join_input)
        join_input_manager.auto_rename()
        left_on, right_on = self._get_join_keys(join_input_manager)

        # Semi/anti joins only return left columns, so apply the left-side
        # select/drop/rename (the right side is suppressed) to mirror FlowDataEngine.join.
        left_renames = {
            column.old_name: column.new_name
            for column in join_input_manager.left_select.renames
            if column.old_name != column.new_name and (column.keep or column.join_key)
        }
        left_drop_columns = [
            column.old_name
            for column in join_input_manager.left_select.renames
            if not column.keep and not column.join_key
        ]
        if left_renames:
            self._add_code(f"{left_df} = {left_df}.rename({left_renames})")
        if left_drop_columns:
            self._add_code(f"{left_df} = {left_df}.drop({left_drop_columns})")

        # Semi/anti joins have no post-processing, so no outer parens are needed.
        self._add_code(f"{var_name} = {left_df}.join(")
        self._add_code(f"        {right_df},")
        self._add_code(f"        left_on={left_on},")
        self._add_code(f"        right_on={right_on},")
        self._add_code(f'        how="{settings.join_input.how}"')
        self._add_code("    )")

    def _handle_standard_join(
        self, settings: input_schema.NodeJoin, var_name: str, left_df: str, right_df: str
    ) -> None:
        """Handle standard joins (left, right, inner, outer) with full column management.

        Standard joins may include columns from both DataFrames and require careful
        management of column names, duplicates, and transformations. This method
        orchestrates the complete join process including pre/post transformations.

        Process:
        1. Auto-rename columns to avoid conflicts
        2. Extract join keys
        3. Apply pre-join transformations (renames, drops)
        4. Handle join-specific key transformations
        5. Execute join with post-processing

        Args:
            settings: NodeJoin settings containing join configuration
            var_name: Name of the variable to store the result
            left_df: Variable name of the left DataFrame
            right_df: Variable name of the right DataFrame

        Returns:
            None: Modifies internal state by adding generated code
        """
        join_input_manager = transform_schema.JoinInputManager(settings.join_input)
        join_input_manager.auto_rename()
        left_on, right_on = self._get_join_keys(join_input_manager)

        left_df, right_df = self._apply_pre_join_transformations(join_input_manager, left_df, right_df)
        left_on, right_on, reverse_action, after_join_drop_cols = self._handle_join_key_transformations(
            join_input_manager, left_df, right_df, left_on, right_on
        )
        self._execute_join_with_post_processing(
            settings, var_name, left_df, right_df, left_on, right_on, after_join_drop_cols, reverse_action
        )

    @staticmethod
    def _get_join_keys(settings: transform_schema.JoinInputManager) -> tuple[list[str], list[str]]:
        """Extract join keys based on join type.

        Different join types require different handling of join keys:
        - For outer/right joins: Uses renamed column names for right DataFrame
        - For other joins: Uses original column names from join mapping

        Args:
            settings: NodeJoin settings containing join configuration

        Returns:
            Tuple[List[str], List[str]]: Lists of (left_on, right_on) column names
        """
        left_on = [jm.left_col for jm in settings.get_names_for_table_rename()]

        if settings.how in ("outer", "right"):
            right_on = [jm.right_col for jm in settings.get_names_for_table_rename()]
        else:
            right_on = [jm.right_col for jm in settings.join_mapping]

        return left_on, right_on

    def _apply_pre_join_transformations(
        self, settings: transform_schema.JoinInputManager, left_df: str, right_df: str
    ) -> tuple[str, str]:
        """Apply column renames and drops before the join operation.

        Pre-join transformations prepare DataFrames by:
        - Renaming columns according to user specifications
        - Dropping columns marked as not to keep (except join keys)
        - Special handling for right/outer joins where join keys may need preservation

        Args:
            settings: NodeJoin settings containing column rename/drop specifications
            left_df: Variable name of the left DataFrame
            right_df: Variable name of the right DataFrame

        Returns:
            Tuple[str, str]: The same DataFrame variable names (left_df, right_df)
                Note: DataFrames are modified via generated code, not new variables
        """
        right_renames = {
            column.old_name: column.new_name
            for column in settings.right_select.renames
            if column.old_name != column.new_name
            and (column.keep or column.join_key)
            and (not column.join_key or settings.how in ("outer", "right"))
        }

        left_renames = {
            column.old_name: column.new_name
            for column in settings.left_select.renames
            if column.old_name != column.new_name and (column.keep or column.join_key)
        }

        left_drop_columns = [
            column.old_name for column in settings.left_select.renames if not column.keep and not column.join_key
        ]

        right_drop_columns = [
            column.old_name for column in settings.right_select.renames if not column.keep and not column.join_key
        ]

        if right_renames:
            self._add_code(f"{right_df} = {right_df}.rename({right_renames})")
        if left_renames:
            self._add_code(f"{left_df} = {left_df}.rename({left_renames})")
        if left_drop_columns:
            self._add_code(f"{left_df} = {left_df}.drop({left_drop_columns})")
        if right_drop_columns:
            self._add_code(f"{right_df} = {right_df}.drop({right_drop_columns})")

        return left_df, right_df

    def _handle_join_key_transformations(
        self,
        settings: transform_schema.JoinInputManager,
        left_df: str,
        right_df: str,
        left_on: list[str],
        right_on: list[str],
    ) -> tuple[list[str], list[str], dict | None, list[str]]:
        """Route to appropriate join-specific key transformation handler.

        Different join types require different strategies for handling join keys
        to avoid conflicts and preserve necessary columns.

        Args:
            settings: NodeJoin settings containing join configuration
            left_df: Variable name of the left DataFrame
            right_df: Variable name of the right DataFrame
            left_on: List of left DataFrame column names to join on
            right_on: List of right DataFrame column names to join on

        Returns:
            Tuple containing:
                - left_on: Potentially modified list of left join columns
                - right_on: Potentially modified list of right join columns
                - reverse_action: Dictionary for renaming columns after join (or None)
                - after_join_drop_cols: List of columns to drop after join
        """
        join_type = settings.how

        if join_type in ("left", "inner"):
            return self._handle_left_inner_join_keys(settings, right_df, left_on, right_on)
        elif join_type == "right":
            return self._handle_right_join_keys(settings, left_df, left_on, right_on)
        elif join_type == "outer":
            return self._handle_outer_join_keys(settings, right_df, left_on, right_on)
        else:
            return left_on, right_on, None, []

    def _handle_left_inner_join_keys(
        self, settings: transform_schema.JoinInputManager, right_df: str, left_on: list[str], right_on: list[str]
    ) -> tuple[list[str], list[str], dict, list[str]]:
        """Handle key transformations for left and inner joins.

        For left/inner joins:
        - Join keys from left DataFrame are preserved
        - Right DataFrame join keys are temporarily renamed with __DROP__ prefix
        - After join, these temporary columns can be renamed back if needed

        Args:
            settings: NodeJoin settings containing join configuration
            right_df: Variable name of the right DataFrame
            left_on: List of left DataFrame column names to join on
            right_on: List of right DataFrame column names to join on

        Returns:
            Tuple containing:
                - left_on: Unchanged left join columns
                - right_on: Unchanged right join columns
                - reverse_action: Mapping to rename __DROP__ columns after join
                - after_join_drop_cols: Left join keys marked for dropping
        """
        [jk.new_name for jk in settings.left_select.join_key_selects if jk.keep]
        join_key_duplication_command = [
            f"{self.framework}.col({self._py_str(rjk.old_name)})"
            f".alias({self._py_str('__DROP__' + rjk.new_name + '__DROP__')})"
            for rjk in settings.right_select.join_key_selects
            if rjk.keep
        ]

        reverse_action = {
            f"__DROP__{rjk.new_name}__DROP__": rjk.new_name
            for rjk in settings.right_select.join_key_selects
            if rjk.keep
        }

        if join_key_duplication_command:
            self._add_code(f"{right_df} = {right_df}.with_columns([{', '.join(join_key_duplication_command)}])")

        after_join_drop_cols = [k.new_name for k in settings.left_select.join_key_selects if not k.keep]

        return left_on, right_on, reverse_action, after_join_drop_cols

    def _handle_right_join_keys(
        self, settings: transform_schema.JoinInputManager, left_df: str, left_on: list[str], right_on: list[str]
    ) -> tuple[list[str], list[str], None, list[str]]:
        """Handle key transformations for right joins.

        For right joins:
        - Join keys from right DataFrame are preserved
        - Left DataFrame join keys are prefixed with __jk_ to avoid conflicts
        - Polars appends "_right" suffix to conflicting column names

        Args:
            settings: NodeJoin settings containing join configuration
            left_df: Variable name of the left DataFrame
            left_on: List of left DataFrame column names to join on
            right_on: List of right DataFrame column names to join on

        Returns:
            Tuple containing:
                - left_on: Modified left join columns with __jk_ prefix where needed
                - right_on: Unchanged right join columns
                - reverse_action: None (no post-join renaming needed)
                - after_join_drop_cols: Right join keys marked for dropping
        """
        join_key_duplication_command = [
            f"{self.framework}.col({self._py_str(ljk.new_name)}).alias({self._py_str('__jk_' + ljk.new_name)})"
            for ljk in settings.left_select.join_key_selects
            if ljk.keep
        ]

        for position, left_on_key in enumerate(left_on):
            left_on_select = settings.left_select.get_select_input_on_new_name(left_on_key)
            if left_on_select and left_on_select.keep:
                left_on[position] = f"__jk_{left_on_select.new_name}"

        if join_key_duplication_command:
            self._add_code(f"{left_df} = {left_df}.with_columns([{', '.join(join_key_duplication_command)}])")

        left_join_keys_keep = {jk.new_name for jk in settings.left_select.join_key_selects if jk.keep}
        after_join_drop_cols_right = [
            jk.new_name if jk.new_name not in left_join_keys_keep else jk.new_name + "_right"
            for jk in settings.right_select.join_key_selects
            if not jk.keep
        ]
        after_join_drop_cols = list(dict.fromkeys(after_join_drop_cols_right))
        return left_on, right_on, None, after_join_drop_cols

    def _handle_outer_join_keys(
        self, settings: transform_schema.JoinInputManager, right_df: str, left_on: list[str], right_on: list[str]
    ) -> tuple[list[str], list[str], dict, list[str]]:
        """Handle key transformations for outer joins.

        For outer joins:
        - Both left and right join keys may need to be preserved
        - Right DataFrame join keys are prefixed with __jk_ when they conflict
        - Post-join renaming reverses the __jk_ prefix

        Args:
            settings: NodeJoin settings containing join configuration
            right_df: Variable name of the right DataFrame
            left_on: List of left DataFrame column names to join on
            right_on: List of right DataFrame column names to join on

        Returns:
            Tuple containing:
                - left_on: Unchanged left join columns
                - right_on: Modified right join columns with __jk_ prefix where needed
                - reverse_action: Mapping to remove __jk_ prefix after join
                - after_join_drop_cols: Combined list of columns to drop from both sides
        """
        left_join_keys = {jk.new_name for jk in settings.left_select.join_key_selects}

        join_keys_to_keep_and_rename = [
            rjk for rjk in settings.right_select.join_key_selects if rjk.keep and rjk.new_name in left_join_keys
        ]

        join_key_rename_command = {rjk.new_name: f"__jk_{rjk.new_name}" for rjk in join_keys_to_keep_and_rename}

        for position, right_on_key in enumerate(right_on):
            right_on_select = settings.right_select.get_select_input_on_new_name(right_on_key)
            if right_on_select and right_on_select.keep and right_on_select.new_name in left_join_keys:
                right_on[position] = f"__jk_{right_on_select.new_name}"

        if join_key_rename_command:
            self._add_code(f"{right_df} = {right_df}.rename({join_key_rename_command})")

        reverse_action = {f"__jk_{rjk.new_name}": rjk.new_name for rjk in join_keys_to_keep_and_rename}

        after_join_drop_cols_left = [jk.new_name for jk in settings.left_select.join_key_selects if not jk.keep]
        after_join_drop_cols_right = [
            jk.new_name if jk.new_name not in left_join_keys else jk.new_name + "_right"
            for jk in settings.right_select.join_key_selects
            if not jk.keep
        ]
        after_join_drop_cols = after_join_drop_cols_left + after_join_drop_cols_right

        return left_on, right_on, reverse_action, after_join_drop_cols

    def _execute_join_with_post_processing(
        self,
        settings: input_schema.NodeJoin,
        var_name: str,
        left_df: str,
        right_df: str,
        left_on: list[str],
        right_on: list[str],
        after_join_drop_cols: list[str],
        reverse_action: dict | None,
    ) -> None:
        """Execute the join operation and apply post-processing steps.

        Generates the actual join code with any necessary post-processing:
        1. Executes the join operation
        2. For right joins: Collects to eager mode (Polars requirement)
        3. Drops unnecessary columns
        4. Renames temporary columns back to final names
        5. For right joins: Converts back to lazy mode

        Args:
            settings: NodeJoin settings containing join configuration
            var_name: Name of the variable to store the result
            left_df: Variable name of the left DataFrame
            right_df: Variable name of the right DataFrame
            left_on: List of left DataFrame column names to join on
            right_on: List of right DataFrame column names to join on
            after_join_drop_cols: List of columns to drop after join
            reverse_action: Dictionary for renaming columns after join (or None)

        Returns:
            None: Modifies internal state by adding generated code
        """
        # Wrap in parens only when a post-processing method chains onto the join;
        # a clean join needs no outer parens.
        has_post = settings.join_input.how == "right" or bool(after_join_drop_cols) or bool(reverse_action)
        self._add_code(f"{var_name} = {'(' if has_post else ''}{left_df}.join(")
        self._add_code(f"        {right_df},")
        self._add_code(f"        left_on={left_on},")
        self._add_code(f"        right_on={right_on},")
        self._add_code(f'        how="{settings.join_input.how}"')
        self._add_code("    )")

        # TODO(FlowFrame): The .collect().lazy() pattern for right joins returns a
        # pl.LazyFrame, breaking the FlowFrame chain. The FlowFrame converter may
        # need to override join handling or use framework-aware collect/lazy.
        if settings.join_input.how == "right":
            self._add_code(".collect()")  # Right join needs to be collected first cause of issue with rename

        if after_join_drop_cols:
            self._add_code(f".drop({after_join_drop_cols})")

        if reverse_action:
            self._add_code(f".rename({reverse_action})")

        if settings.join_input.how == "right":
            self._add_code(".lazy()")

        if has_post:
            self._add_code(")")
