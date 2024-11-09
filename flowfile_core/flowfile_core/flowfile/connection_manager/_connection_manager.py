from typing import Dict, List, Any
from flowfile_core.flowfile.connection_manager.models import Connection


class ConnectionManager:
    connections: Dict[str, Dict[str, Connection]]

    def add_connection(self, connection_group: str, connection_name: str, connection: Connection):
        existing_connections_in_group = self.connections.get(connection_group)
        if existing_connections_in_group is None:
            self.connections[connection_group] = {connection_name: connection}
        else:
            if connection_name in existing_connections_in_group:
                raise Exception(f"Connection {connection_name} already exists in group {connection_group}")
            else:
                self.connections[connection_group][connection_name] = connection

    def get_connection(self, connection_group: str, connection_name: str) -> Connection:
        self.raise_if_connection_does_not_exist(connection_group, connection_name)
        return self.connections[connection_group][connection_name]

    def check_if_connection_exists(self, connection_group: str, connection_name: str) -> bool:
        return connection_name in self.connections.get(connection_group, {})

    def raise_if_connection_exists(self, connection_group: str, connection_name: str):
        if self.check_if_connection_exists(connection_group, connection_name):
            raise Exception(f"Connection {connection_name} already exists in group {connection_group}")

    def raise_if_connection_does_not_exist(self, connection_group: str, connection_name: str):
        if not self.check_if_connection_exists(connection_group, connection_name):
            raise Exception(f"Connection {connection_name} does not exist in group {connection_group}")

    def update_connection(self, connection_group: str, connection_name: str, connection: Connection):
        self.raise_if_connection_does_not_exist(connection_group, connection_name)
        self.connections[connection_group][connection_name] = connection

    def insert_settings_raw(self, connection_group: str, connection_name: str, settings: Dict[str, Any]):
        connection = Connection(group=connection_group, name=connection_name, config_setting=settings)
        self.add_connection(connection_group, connection_name, connection)

    def connection_groups(self) -> List[str]:
        return list(self.connections.keys())

    def get_available_connections_in_group(self, group_name: str):
        connection_group = self.connections.get(group_name)
        if connection_group is None:
            return []
        return list(connection_group.keys())