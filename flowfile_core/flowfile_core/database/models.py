from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    full_name = Column(String)
    hashed_password = Column(String)
    disabled = Column(Boolean, default=False)


class Secret(Base):
    __tablename__ = "secrets"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    encrypted_value = Column(Text)
    iv = Column(String)
    user_id = Column(Integer, ForeignKey("users.id"))


class DatabaseConnection(Base):
    __tablename__ = "database_connections"

    id = Column(Integer, primary_key=True, index=True)
    connection_name = Column(String, index=True)
    database_type = Column(String)
    username = Column(String)
    host = Column(String)
    port = Column(Integer)
    database = Column(String, default=None)
    ssl_enabled = Column(Boolean, default=False)
    password_id = Column(Integer, ForeignKey("secrets.id"))
    user_id = Column(Integer, ForeignKey("users.id"))


class CloudStorageConnection(Base):
    __tablename__ = "cloud_storage_connections"

    id = Column(Integer, primary_key=True, index=True)
    connection_name = Column(String, index=True, nullable=False)
    storage_type = Column(String, nullable=False)  # 's3', 'adls', 'gcs'
    auth_method = Column(String, nullable=False)  # 'access_key', 'iam_role', etc.

    # AWS S3 fields
    aws_region = Column(String, nullable=True)
    aws_access_key_id = Column(String, nullable=True)
    aws_secret_access_key_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)
    aws_session_token_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)
    aws_role_arn = Column(String, nullable=True)
    aws_allow_unsafe_html = Column(Boolean, nullable=True)

    # Azure ADLS fields
    azure_account_name = Column(String, nullable=True)
    azure_account_key_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)
    azure_tenant_id = Column(String, nullable=True)
    azure_client_id = Column(String, nullable=True)
    azure_client_secret_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)
    azure_sas_token_id = Column(Integer, ForeignKey("secrets.id"), nullable=True)

    # Common fields
    endpoint_url = Column(String, nullable=True)
    extra_config = Column(Text, nullable=True)  # JSON field for additional config
    verify_ssl = Column(Boolean, default=True)

    # Metadata
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)


class CloudStoragePermission(Base):
    __tablename__ = "cloud_storage_permissions"

    id = Column(Integer, primary_key=True, index=True)
    connection_id = Column(Integer, ForeignKey("cloud_storage_connections.id"), nullable=False)
    resource_path = Column(String, nullable=False)  # e.g., "s3://bucket-name"
    can_read = Column(Boolean, default=True)
    can_write = Column(Boolean, default=False)
    can_delete = Column(Boolean, default=False)
    can_list = Column(Boolean, default=True)
