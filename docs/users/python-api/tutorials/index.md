# Python API Tutorials

Build one city sales report from supermarket invoices, then turn it into a reusable, parameterized flow. Each chapter extends the previous one, links to a complete runnable script, and shows what that script prints.

<div class="grid cards" markdown>

- **Build the report**

    - [1. First steps](first-steps.md): read, transform, collect, and print the graph
    - [2. Clean and shape](clean-and-shape.md): editable formulas and expression semantics
    - [3. Combine](combine.md): union customer segments and join city targets
    - [4. Aggregate](aggregate.md): summarize revenue without multiplying targets

- **Run the report repeatedly**

    - [5. Flow parameters](flow-parameters.md): change the basket threshold between runs
    - [6. Branching with Gate](branching.md): route the whole report when any city falls short
    - [7. Write results](write-results.md): files, catalog tables, and cloud storage
    - [8. Reusable flows](reusable-flows.md): extract a child flow and compare thresholds
    - [9. Custom logic](custom-logic.md): a percentage score with SQL, a custom node, or Python
    - [10. Ship it](ship-it.md): save, run headlessly, schedule, and open in the Designer

</div>

Use the [Quick Start](../quickstart.md) for installation. Download a chapter script and run it with Python, for example `python tutorial_01.py`. The scripts fetch the public `supermarket_sales.csv`; they do not need a repository checkout. Chapters 7 and 10 write a `city_report` folder into the directory you run them from and a `tutorial_sales` catalog table; chapters 8–10 register flows in your catalog. The output shown on each page is checked against the script by the docs test suite.

## Related walkthrough

[Building Flows with Code](flowfile_frame_api.md) is the shorter introduction to building and opening a graph. For individual operations, use the [API reference](../reference/index.md).

**Next:** [First steps](first-steps.md)
