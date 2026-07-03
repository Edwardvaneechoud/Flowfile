# Worked Examples

End-to-end walkthroughs that build a real flow from start to finish. Each one names the reader it serves, the nodes it uses, and the data it runs on, so you can pick the closest match to what you're building.

## Examples

| Example | For | Difficulty | Nodes | Data |
|---------|-----|-----------|-------|------|
| [Deduplicate and summarize sales data](sales-pipeline.md) | Analysts new to Flowfile | Beginner | 5 | `data/templates/supermarket_sales.csv` |
| [Export to Python](code-generator.md) | Python developers | Beginner | Any | Any flow |

## Connecting external data

Two more walkthroughs live under the **Connect Your Data** tab, since they start by wiring up a connection:

| Example | For | Difficulty | Nodes | Data |
|---------|-----|-----------|-------|------|
| [Connect to a PostgreSQL database](database-connectivity.md) | Analysts with a database | Intermediate | Read from Database → Formula → Group by → Write to Database | A PostgreSQL table |
| [Connect to AWS S3 and S3-compatible storage](cloud-connections.md) | Analysts with cloud data | Intermediate | Cloud Storage Reader / Writer | Files in S3 or MinIO |

Every example here is validated by the test suite on each commit: the flow files live in [`data/templates/flows/`](https://github.com/edwardvaneechoud/Flowfile/tree/main/data/templates/flows) and the Python twins in [`docs/examples/`](https://github.com/edwardvaneechoud/Flowfile/tree/main/docs/examples), and both are executed end-to-end against the committed sample data.
