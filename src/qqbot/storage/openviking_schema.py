from sqlalchemy import Column, String, Table, Text

from qqbot.storage.schema import metadata

openviking_scopes = Table(
    "openviking_scopes", metadata,
    Column("id", String, primary_key=True),
    Column("binding", String, nullable=False),
    Column("generation", String, nullable=False),
    Column("state", String, nullable=False),
    Column("access_json", Text, nullable=False),
    Column("validation_json", Text, nullable=False),
)
