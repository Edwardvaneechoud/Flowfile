# How to Connect and Work with PostgreSQL Databases in Flowfile

### ![Full overview](../../../assets/images/guides/database_connectivity/main_image.png) Full flow overview

Flowfile's latest release introduces powerful database connectivity features that allow you to seamlessly integrate with PostgreSQL databases like Supabase. In this guide, I'll walk you through the entire process of connecting to a database, reading data, transforming it, and writing it back.

## Prerequisites

Before diving in, make sure you have:

-   A Flowfile account (free tier works fine)
-   A Supabase account ([sign up here](https://supabase.com) if needed)
-   Sample data to work with (we're using the [Sales Forecasting Dataset](https://www.kaggle.com/datasets/rohitsahoo/sales-forecasting) from Kaggle **so you can easily follow along with the transformation examples**)

## Step 1: Set Up Your Supabase Database

1.  Create a new project in Supabase.
2.  Download the sample dataset from Kaggle.
3.  Create a new table in your Supabase project (e.g., `superstore_sales_data`).
4.  Import the dataset into your table (hint: use Supabase's built-in CSV import feature via the Table Editor).
5.  Note your database connection details (host, port, username, password).

## Step 2: Configure Your Database Connection in Flowfile

1.  Open Flowfile and navigate to the database connection manager (often found under a "Connections" icon or within the main "Settings" area).
2.  Click **"Create New Connection"**.
3.  Fill in your connection details:
    *   Connection Name: `supa_base_connection` (or any name you prefer)
    *   Database Type: PostgreSQL
    *   Host: Your Supabase host (e.g., `aws-0-eu-central-1.pooler.supabase.com`)
    *   Port: `5432`
    *   Database: `postgres`
    *   Username: Your Supabase username
    *   Password: Your Supabase password
    *   Enable SSL: Check if required by your database (Supabase typically requires it).
4.  Click **"Update Connection"** to save.

### ![db_connection](../../../assets/images/guides/database_connectivity/db_connection.png) Connection overview in Flowfile

## Step 3: Create a New Data Flow

1.  Click **"Create"** or **"New Flow"** to start a fresh workflow.
2.  Navigate to the "Data actions" panel on the left sidebar.
3.  Find the "Read from Database" node (look for the database icon).
4.  Drag and drop this node onto your canvas.

## Step 4: Configure Your Database Read Operation

1.  Click on the "Read from Database" node to open its settings panel.
2.  Select **"reference"** for Connection Mode (this tells Flowfile to use the connection you configured in Step 2).
3.  Choose your `supa_base_connection` from the Connection dropdown.
4.  Configure the table settings:
    *   Schema: `public` (or your specific schema)
    *   Table: `superstore_sales_data`
5.  Click **"Validate Settings"** to ensure everything is working.
6.  You should see a green confirmation message: "Query settings are valid".

### ![configure_read_db](../../../assets/images/guides/database_connectivity/configure_read_db.png) Node Settings panel showing database read configuration

## Step 5: Run Your Initial Flow

1.  Click the **"Run"** button in the top toolbar.
2.  Watch the flow execution in the log panel at the bottom.
3.  When completed, you'll see a success message and the number of records processed.
4.  You can now click on the node output dot to preview the data that was read from your database.

### ![initial_run_success](../../../assets/images/guides/database_connectivity/initial_run.png) Flow execution logs showing successful database read operation
*Ensure the image `initial_run.png` shows the successful run log/node status, not the configuration panel again.*

## Step 6: Add Data Transformations

Now that you've successfully read data from your database, you can add transformation steps:

1.  Add transformation nodes from the "Data actions" panel to your workflow.
    *   For example, creating time-to-ship metrics by category:
        *   Add formula nodes to transform the `shipping_date` and `delivery_date` columns to a proper date type if needed.
        *   Add a "Formula" node to calculate shipping time (e.g., `delivery_date - shipping_date`). Name the new column `shipping_time_days`.
        *   Add a "Group by" node to aggregate by product category.
        *   In the "Group by" node, calculate `min`, `max`, and `median` of the `shipping_time_days` column.
2.  Connect these nodes in sequence by dragging from the output dot of one node to the input dot of the next.
3.  Configure each node with the specific transformations you need (refer to the Flowfile documentation for details on specific node configurations if needed).

### ![transformations](../../../assets/images/guides/database_connectivity/transformations.png) Overview of connected transformation nodes (Read -> Formula -> Group By)

## Step 7: Add a Write to Database Node

1.  From the "Data actions" panel, find and drag the "Write to Database" node onto your canvas.
2.  Connect it to the output of the last transformation node (e.g., the "Group by" node).
3.  Configure the write operation in its settings panel:
    *   Connection Mode: Select **"reference"**.
    *   Connection: Choose your `supa_base_connection`.
    *   Schema: `public` (or your desired schema).
    *   Table: Enter a name for your new output table (e.g., `time_to_ship_per_category`).
    *   Write Mode: Select how to handle the table if it already exists:
        *   **"Append"**: Add new data to the table.
        *   **"Replace"**: Delete the existing table and create a new one with the output data.
        *   **"Fail"**: Abort the flow if the table already exists.

### ![configure_write_db](../../../assets/images/guides/database_connectivity/configure_write_db.png) Setup write to database node configuration panel
*Ensure the image `configure_write_db.png` actually shows the "Write to Database" node's configuration panel.*

## Step 8: Run Your Complete Workflow

1.  Click **"Run"** to execute the full workflow from start to finish.
2.  The system will:
    *   Read data from your source Supabase table (`superstore_sales_data`).
    *   Apply all the transformation steps (calculate shipping time, group by category).
    *   Write the aggregated results to your destination Supabase table (`time_to_ship_per_category`).
3.  Check the logs to confirm successful execution, including messages about records read and written.
4.  Navigate to your Supabase project, open the SQL Editor or Table Editor, and check the `public.time_to_ship_per_category` table to see your newly created data!

### ![supabase_result_table](../../../assets/images/guides/database_connectivity/result.png) Overview of the final result table in Supabase
*Ensure the image `result.png` shows the data in the newly created Supabase table.*

## Conclusion

Flowfile's database integration capabilities make it incredibly simple to build professional-grade data pipelines without writing code. By connecting to Supabase or other PostgreSQL databases, you can easily extract, transform, and load data in a visual, intuitive environment.

Whether you're creating business dashboards, data warehousing solutions, or just exploring your data, the combination of Flowfile's visual workflow and Supabase's powerful PostgreSQL hosting gives you a robust platform for all your data needs.

Feel free to experiment with different transformation nodes and workflow patterns to build increasingly sophisticated data pipelines!

---

*This guide is based on Flowfile v0.2.0, which introduced database connectivity features including PostgreSQL support, secure credential storage, and flexible connection management options.*
