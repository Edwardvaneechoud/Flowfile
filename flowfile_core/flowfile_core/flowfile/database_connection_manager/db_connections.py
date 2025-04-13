from flowfile_core.schemas.input_schema import FullDatabaseConnection, FullDatabaseConnectionInterface
from sqlalchemy.orm import Session
from flowfile_core.database.models import DatabaseConnection as DBConnectionModel, Secret
from flowfile_core.secrets.secrets import store_secret, SecretInput, decrypt_secret
from flowfile_core.database.connection import get_db_context


def store_database_connection(db: Session, connection: FullDatabaseConnection, user_id: int) -> DBConnectionModel:
    """
    Store a database connection in the database.
    """
    # Encrypt the password

    existing_database_connection = get_database_connection(db, connection.connection_name, user_id)
    if existing_database_connection:
        raise ValueError(
            f"Database connection with name '{connection.connection_name}' already exists for user {user_id}."
            f" Please use a unique connection name or delete the existing connection first."
        )

    password_id = store_secret(db, SecretInput(name=connection.connection_name, value=connection.password), user_id).id

    # Create a new database connection object
    db_connection = DBConnectionModel(
        connection_name=connection.connection_name,
        host=connection.host,
        port=connection.port,
        database=connection.database,
        database_type=connection.database_type,
        username=connection.username,
        password_id=password_id,
        ssl_enabled=connection.ssl_enabled,
        user_id=user_id
    )

    # Add and commit the new connection to the database
    db.add(db_connection)
    db.commit()
    db.refresh(db_connection)

    return db_connection


def get_database_connection(db: Session, connection_name: str, user_id: int) -> DBConnectionModel | None:
    """
    Get a database connection by its name and user ID.
    """
    db_connection = db.query(DBConnectionModel).filter(
        DBConnectionModel.connection_name == connection_name,
        DBConnectionModel.user_id == user_id
    ).first()

    return db_connection


def get_database_connection_schema(db: Session, connection_name: str, user_id: int) -> FullDatabaseConnection | None:
    """
    Get a database connection schema by its name and user ID.
    """
    db_connection = get_database_connection(db, connection_name, user_id)

    if db_connection:
        # Decrypt the password
        password_secret = db.query(Secret).filter(Secret.id == db_connection.password_id).first()
        if not password_secret:
            raise Exception("Password secret not found")

        return FullDatabaseConnection(
            connection_name=db_connection.connection_name,
            host=db_connection.host,
            port=db_connection.port,
            database=db_connection.database,
            database_type=db_connection.database_type,
            username=db_connection.username,
            password=password_secret.encrypted_value,
            ssl_enabled=db_connection.ssl_enabled
        )

    return None


def get_local_database_connection(connection_name: str, user_id: int) -> FullDatabaseConnection | None:
    with get_db_context() as db:
        return get_database_connection_schema(db, connection_name, user_id)


def delete_database_connection(db: Session, connection_name: str, user_id: int) -> None:
    """
    Delete a database connection by its name and user ID.
    """
    db_connection = db.query(DBConnectionModel).filter(
        DBConnectionModel.connection_name == connection_name,
        DBConnectionModel.user_id == user_id
    ).first()

    if db_connection:
        db.delete(db_connection)

        password_secret = db.query(Secret).filter(Secret.id == db_connection.password_id).first()
        if password_secret:
            db.delete(password_secret)
        db.commit()


def database_connection_interface_from_db_connection(db_connection: DBConnectionModel) -> FullDatabaseConnectionInterface:
    """
    Convert a database connection from the database model to the interface model.
    """
    return FullDatabaseConnectionInterface(
        connection_name=db_connection.connection_name,
        database_type=db_connection.database_type,
        username=db_connection.username,
        host=db_connection.host,
        port=db_connection.port,
        database=db_connection.database,
        ssl_enabled=db_connection.ssl_enabled
    )


def get_all_database_connections_interface(db: Session, user_id: int) -> list[FullDatabaseConnectionInterface]:
    """
    Get all database connections for a user.
    """
    # Get the raw query results
    query_results = db.query(DBConnectionModel).filter(
        DBConnectionModel.user_id == user_id
    ).all()

    # Convert with explicit type assertion
    result = []
    for db_connection in query_results:
        # Verify that we have an instance, not a type
        if isinstance(db_connection, DBConnectionModel):
            result.append(database_connection_interface_from_db_connection(db_connection))
        else:
            # Raise an error if we somehow get a type instead of an instance
            raise TypeError(f"Expected a DBConnectionModel instance, got {type(db_connection)}")

    return result
