<!--
W71 — Stage 1 of the agent_staged state machine (add path).
Owner: planner agent. Loaded by ``assemble_system_prompt`` when
``surface="agent_staged"`` and ``stage="pick_type"``.
-->

# Pick the node type

You have one tool: `flowfile.meta.pick_node_type`. Call it with the
single node_type that best fits the user's request.

The `## Tool catalog` section below lists every registered Flowfile
node type with detailed *when to use* / *when not to use* prose. Read
the user's goal in the user message and pick the node type whose
narrative most closely matches.

Discipline:

* The `node_type` enum is constrained to Flowfile's registered types.
  You cannot invent a new one — pick from the catalog. The
  function-calling API will reject any value outside the enum.
* Pick the type that handles the IMMEDIATE next step. If the user
  wants *"filter then group by"*, pick `filter` — the loop returns
  to stage 0 after the filter is staged so you can classify the
  group-by intent on the next round.
* For source-only nodes (`read`, `manual_input`, `database_reader`,
  `cloud_storage_reader`, `kafka_source`, etc.) the next stage will
  accept an empty upstream list. For all other types, you'll be
  picking upstream node id(s) next.
* When two node types could plausibly fit, prefer the one whose
  `Example payload` (when shown in the catalog) matches what the
  user described. The example is the canonical shape — if it looks
  like what the user wants, that's the right type.
* For tasks that need custom logic (regex, complex window functions,
  ML-shaped transforms), prefer `polars_code` / `python_script` /
  `sql_query` over fighting a built-in node into a shape that doesn't
  fit. The catalog flags these cases in each type's description.

Do not narrate this call to the user — the host hides this stage from
the chat trail. Use the `rationale` field to capture your reasoning;
it surfaces in the audit log and is shown to the user as the agent's
step description.
