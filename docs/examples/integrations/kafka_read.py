"""Read JSON messages from a Kafka topic through a stored Kafka connection."""

import uuid

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import KafkaConnection
from flowfile_core.kafka.connection_manager import store_kafka_connection
from flowfile_core.schemas.kafka_schemas import KafkaConnectionCreate
from test_utils.kafka.fixtures import BOOTSTRAP_SERVERS, create_topic, produce_json_messages

# A unique topic per run so the consumer reads exactly the messages produced here.
topic = f"docs_orders_{uuid.uuid4().hex[:8]}"
create_topic(topic, num_partitions=1)
produce_json_messages(
    topic,
    [
        {"order_id": 1, "product": "keyboard", "amount": 49.99},
        {"order_id": 2, "product": "mouse", "amount": 19.99},
        {"order_id": 3, "product": "monitor", "amount": 199.00},
    ],
)

# Kafka connections are stored server-side (there is no ff.* helper). Register one
# pointing at the broker; ff.read_kafka resolves it by name at read time.
connection_name = f"docs-kafka-{uuid.uuid4().hex[:8]}"
with get_db_context() as db:
    store_kafka_connection(
        db,
        KafkaConnectionCreate(
            connection_name=connection_name,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            security_protocol="PLAINTEXT",
        ),
        user_id=1,
    )

# --8<-- [start:example]
import flowfile as ff

orders = ff.read_kafka(
    connection_name,
    topic_name=topic,
    start_offset="earliest",
    poll_timeout_seconds=10.0,
).collect()
# --8<-- [end:example]

assert orders.height == 3
assert "product" in orders.columns
assert set(orders["product"].to_list()) == {"keyboard", "mouse", "monitor"}
# Kafka metadata columns are added alongside the message fields.
assert "_kafka_offset" in orders.columns
assert "_kafka_partition" in orders.columns

with get_db_context() as db:
    conn = (
        db.query(KafkaConnection)
        .filter(KafkaConnection.connection_name == connection_name, KafkaConnection.user_id == 1)
        .first()
    )
    if conn is not None:
        db.delete(conn)
        db.commit()
