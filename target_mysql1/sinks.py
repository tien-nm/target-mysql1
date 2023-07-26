"""MySQL target sink class, which handles writing streams."""

from __future__ import annotations

import json
import logging
import re
import string
import typing as t
from typing import Any, Dict, Iterable, List, Optional, cast

import sqlalchemy
from singer_sdk.connectors import SQLConnector
from singer_sdk.helpers._conformers import replace_leading_digit
from singer_sdk.helpers._typing import get_datelike_property_type
from singer_sdk.sinks import SQLSink
from sqlalchemy import Column
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import Engine, URL
from sqlalchemy.schema import PrimaryKeyConstraint

if t.TYPE_CHECKING:
    from sqlalchemy.engine.reflection import Inspector


class MySQLConnector(SQLConnector):
    """The connector for MySQL.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.
    table_name_pattern: str = "${TABLE_NAME}"  # The pattern to use for temp table names.

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger.setLevel(logging.DEBUG)

        if "allow_column_alter" in super().config:
            self.allow_column_alter = super().config.get("allow_column_alter")

    def get_sqlalchemy_url(self, config: dict) -> URL:
        """Generates a SQLAlchemy URL for MySQL.

        Args:
            config: The configuration for the connector.
        """

        if config.get("sqlalchemy_url"):
            return config["sqlalchemy_url"]

        return sqlalchemy.engine.url.URL.create(
            drivername="mysql",
            username=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"],
        )

    def get_fully_qualified_name(
            self,
            table_name: str | None = None,
            schema_name: str | None = None,
            db_name: str | None = None,
            delimiter: str = ".",
    ) -> str:
        """Concatenates a fully qualified name from the parts.

        Args:
            table_name: The name of the table.
            schema_name: The name of the schema. Defaults to None.
            db_name: The name of the database. Defaults to None.
            delimiter: Generally: '.' for SQL names and '-' for Singer names.

        Raises:
            ValueError: If all 3 name parts not supplied.

        Returns:
            The fully qualified name as a string.
        """
        table_name_pattern = self.config.get("table_name_pattern")
        table_name_pattern = string.Template(table_name_pattern).substitute({"TABLE_NAME": table_name})
        if table_name_pattern == "" or table_name_pattern is None:
            table_name_pattern = table_name

        parts = []
        if db_name:
            parts.append(db_name)
        if schema_name:
            parts.append(schema_name)
        if table_name:
            parts.append(table_name_pattern)

        if not parts:
            raise ValueError(
                "Could not generate fully qualified name: "
                + ":".join(
                    [
                        db_name or "(unknown-db)",
                        schema_name or "(unknown-schema)",
                        table_name or "(unknown-table-name)",
                    ],
                ),
            )

        return delimiter.join(parts)

    def get_object_names(
            self,
            engine: Engine,  # noqa: ARG002
            inspected: Inspector,
            schema_name: str,
    ) -> list[tuple[str, bool]]:
        """Return a list of syncable objects.
        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect

        Returns:
            List of tuples (<table_or_view_name>, <is_view>)
        """
        # Get list of tables and views
        table_names = inspected.get_table_names(schema=schema_name)
        try:
            view_names = inspected.get_view_names(schema=schema_name)
        except NotImplementedError:
            # Some DB providers do not understand 'views'
            self._warn_no_view_detection()
            view_names = []

        objects = [(t, False) for t in table_names] + [(v, True) for v in view_names]
        if self.config.get("lower_case_table_names", True):
            objects = [x.lower() for x in objects]

        return objects

    def to_sql_type(self, jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:  # noqa
        """Convert JSON Schema type to a SQL type.
        Args:
            jsonschema_type: The JSON Schema object.
        Returns:
            The SQL type.
        """
        if self._jsonschema_type_check(jsonschema_type, ("string",)):
            datelike_type = get_datelike_property_type(jsonschema_type)
            if datelike_type:
                if datelike_type == "date-time":
                    return cast(
                        sqlalchemy.types.TypeEngine, mysql.DATETIME()
                    )
                elif datelike_type in "time":
                    return cast(sqlalchemy.types.TypeEngine, mysql.TIME())
                elif datelike_type == "date":
                    return cast(sqlalchemy.types.TypeEngine, mysql.DATE())
                elif datelike_type == "binary":
                    return cast(sqlalchemy.types.TypeEngine, mysql.BINARY())

            # The maximum row size for the used table type, not counting BLOBs, is 65535.
            maxlength = jsonschema_type.get("maxLength", 1000)
            data_type = mysql.VARCHAR(maxlength)
            if maxlength <= 1000:
                return cast(sqlalchemy.types.TypeEngine, mysql.VARCHAR(maxlength))
            elif maxlength <= 65535:
                return cast(sqlalchemy.types.TypeEngine, mysql.TEXT(maxlength))
            elif maxlength <= 16777215:
                return cast(sqlalchemy.types.TypeEngine, mysql.MEDIUMTEXT())
            elif maxlength <= 4294967295:
                return cast(sqlalchemy.types.TypeEngine, mysql.LONGTEXT())

            return cast(sqlalchemy.types.TypeEngine, data_type)

        if self._jsonschema_type_check(jsonschema_type, ("integer",)):
            minimum = jsonschema_type.get("minimum", -9223372036854775807)
            maximum = jsonschema_type.get("maximum", 9223372036854775807)

            if minimum >= -128 and maximum <= 127:
                return cast(sqlalchemy.types.TypeEngine, mysql.TINYINT(unsigned=False))
            elif minimum >= -32768 and maximum <= 32767:
                return cast(sqlalchemy.types.TypeEngine, mysql.SMALLINT(unsigned=False))
            elif minimum >= -8388608 and maximum <= 8388607:
                return cast(sqlalchemy.types.TypeEngine, mysql.MEDIUMINT(unsigned=False))
            elif minimum >= -2147483648 and maximum <= 2147483647:
                return cast(sqlalchemy.types.TypeEngine, mysql.INTEGER(unsigned=False))
            elif minimum >= -9223372036854775808 and maximum <= 9223372036854775807:
                return cast(sqlalchemy.types.TypeEngine, mysql.BIGINT(unsigned=False))
            elif minimum >= 0 and maximum <= 255:
                return cast(sqlalchemy.types.TypeEngine, mysql.TINYINT(unsigned=True))
            elif minimum >= 0 and maximum <= 65535:
                return cast(sqlalchemy.types.TypeEngine, mysql.SMALLINT(unsigned=True))
            elif minimum >= 0 and maximum <= 16777215:
                return cast(sqlalchemy.types.TypeEngine, mysql.MEDIUMINT(unsigned=True))
            elif minimum >= 0 and maximum <= 4294967295:
                return cast(sqlalchemy.types.TypeEngine, mysql.INTEGER(unsigned=True))
            elif minimum >= 0 and maximum <= 18446744073709551615:
                return cast(sqlalchemy.types.TypeEngine, mysql.BIGINT(unsigned=True))

        if self._jsonschema_type_check(jsonschema_type, ("number",)):
            if 'multipleOf' in jsonschema_type:
                return cast(sqlalchemy.types.TypeEngine, mysql.DECIMAL())
            else:
                return cast(sqlalchemy.types.TypeEngine, mysql.FLOAT())

        if self._jsonschema_type_check(jsonschema_type, ("boolean",)):
            return cast(sqlalchemy.types.TypeEngine, mysql.BOOLEAN())

        if self._jsonschema_type_check(jsonschema_type, ("object",)):
            # if 'format' in jsonschema_type and jsonschema_type.get("format") == "spatial":
            #     return cast(sqlalchemy.types.TypeEngine, mysql.MU)
            return cast(sqlalchemy.types.TypeEngine, mysql.JSON())

        if self._jsonschema_type_check(jsonschema_type, ("array",)):
            return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TEXT(4000))

        return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TEXT(4000))

    def _jsonschema_type_check(
            self, jsonschema_type: dict, type_check: tuple[str]
    ) -> bool:
        """Return True if the jsonschema_type supports the provided type.
        Args:
            jsonschema_type: The type dict.
            type_check: A tuple of type strings to look for.
        Returns:
            True if the schema suports the type.
        """
        if "type" in jsonschema_type:
            if isinstance(jsonschema_type["type"], (list, tuple)):
                for t in jsonschema_type["type"]:
                    if t in type_check:
                        return True
            else:
                if jsonschema_type.get("type") in type_check:
                    return True

        if any(t in type_check for t in jsonschema_type.get("anyOf", ())):
            return True

        return False

    def _create_empty_column(
            self,
            full_table_name: str,
            column_name: str,
            sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Create a new column.
        Args:
            full_table_name: The target table name.
            column_name: The name of the new column.
            sql_type: SQLAlchemy type engine to be used in creating the new column.
        Raises:
            NotImplementedError: if adding columns is not supported.
        """
        if not self.allow_column_add:
            raise NotImplementedError("Adding columns is not supported.")

        if column_name.startswith("_"):
            column_name = f"x{column_name}"

        create_column_clause = sqlalchemy.schema.CreateColumn(
            sqlalchemy.Column(
                column_name,
                sql_type,
            )
        )

        try:
            alter_sql = f"""ALTER TABLE {str(full_table_name)}
                ADD COLUMN {str(create_column_clause)} """
            self.logger.info("Altering with SQL: %s", alter_sql)
            self.connection.execute(alter_sql)
        except Exception as e:
            raise RuntimeError(
                f"Could not create column '{create_column_clause}' "
                f"on table '{full_table_name}'."
            ) from e

    def create_temp_table_from_table(self, from_table_name, temp_table_name):
        """Temp table from another table."""

        try:
            self.connection.execute(
                f"""DROP TABLE {temp_table_name}"""
            )
        except Exception as e:
            pass

        ddl = f"""
            SET SQL_REQUIRE_PRIMARY_KEY = 0;
            CREATE TABLE {temp_table_name} AS (
                SELECT * FROM {from_table_name}
                WHERE 1=0
            )
        """

        self.connection.execute(ddl)

    def create_empty_table(
            self,
            full_table_name: str,
            schema: dict,
            primary_keys: list[str] | None = None,
            partition_keys: list[str] | None = None,
            as_temp_table: bool = False,
    ) -> None:
        """Create an empty target table.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        if as_temp_table:
            raise NotImplementedError("Temporary tables are not supported.")

        _ = partition_keys  # Not supported in generic implementation.

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sqlalchemy.MetaData(schema=schema_name)
        columns: list[sqlalchemy.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError:
            raise RuntimeError(
                f"Schema for '{full_table_name}' does not define properties: {schema}"
            )

        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columns.append(
                sqlalchemy.Column(
                    property_name,
                    self.to_sql_type(property_jsonschema)
                )
            )

        if primary_keys:
            pk_constraint = PrimaryKeyConstraint(*primary_keys, name=f"{table_name}_PK")
            _ = sqlalchemy.Table(table_name, meta, *columns, pk_constraint)
        else:
            _ = sqlalchemy.Table(table_name, meta, *columns)

        meta.create_all(self._engine)

    def merge_sql_types(  # noqa
            self, sql_types: list[sqlalchemy.types.TypeEngine]
    ) -> sqlalchemy.types.TypeEngine:  # noqa
        """Return a compatible SQL type for the selected type list.
        Args:
            sql_types: List of SQL types.
        Returns:
            A SQL type that is compatible with the input types.
        Raises:
            ValueError: If sql_types argument has zero members.
        """
        if not sql_types:
            raise ValueError("Expected at least one member in `sql_types` argument.")

        if len(sql_types) == 1:
            return sql_types[0]

        # Gathering Type to match variables
        # sent in _adapt_column_type
        current_type = sql_types[0]
        # sql_type = sql_types[1]

        # Getting the length of each type
        # current_type_len: int = getattr(sql_types[0], "length", 0)
        sql_type_len: int = getattr(sql_types[1], "length", 0)
        if sql_type_len is None:
            sql_type_len = 0

        # Convert the two types given into a sorted list
        # containing the best conversion classes
        sql_types = self._sort_types(sql_types)

        # If greater than two evaluate the first pair then on down the line
        if len(sql_types) > 2:
            return self.merge_sql_types(
                [self.merge_sql_types([sql_types[0], sql_types[1]])] + sql_types[2:]
            )

        assert len(sql_types) == 2
        # Get the generic type class
        for opt in sql_types:
            # Get the length
            opt_len: int = getattr(opt, "length", 0)
            generic_type = type(opt.as_generic())

            current_type_length = 0
            if isinstance(current_type, sqlalchemy.types.TEXT) and current_type.length is None:
                current_type_length = 65535
            elif hasattr(current_type, 'length'):
                current_type_length = current_type.length

            if isinstance(generic_type, type):
                if issubclass(
                        generic_type,
                        (sqlalchemy.types.String, sqlalchemy.types.Unicode),
                ):
                    # If length None or 0 then is varchar max ?
                    if (
                            (opt_len is None)
                            or (opt_len == 0)
                            or (opt_len >= current_type_length)
                    ):
                        return opt
                elif isinstance(
                        generic_type,
                        (sqlalchemy.types.String, sqlalchemy.types.Unicode),
                ):
                    # If length None or 0 then is varchar max ?
                    if (
                            (opt_len is None)
                            or (current_type is None)
                            or (opt_len == 0)
                            or (opt_len >= current_type_length)
                    ):
                        return opt
                # If best conversion class is equal to current type
                # return the best conversion class
                elif str(opt) == str(current_type):
                    return opt

        raise ValueError(
            f"Unable to merge sql types: {', '.join([str(t) for t in sql_types])}"
        )

    def _adapt_column_type(
            self,
            full_table_name: str,
            column_name: str,
            sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.
        Args:
            full_table_name: The target table name.
            column_name: The target column name.
            sql_type: The new SQLAlchemy type.
        Raises:
            NotImplementedError: if altering columns is not supported.
        """
        current_type: sqlalchemy.types.TypeEngine = self._get_column_type(
            full_table_name, column_name
        )

        # Check if the existing column type and the sql type are the same
        if str(sql_type) == str(current_type):
            # The current column and sql type are the same
            # Nothing to do
            return

        # Not the same type, generic type or compatible types
        # calling merge_sql_types for assistnace
        compatible_sql_type = self.merge_sql_types([current_type, sql_type])

        if str(compatible_sql_type).split(" ")[0] == str(current_type).split(" ")[0]:
            # Nothing to do
            return

        if not self.allow_column_alter:
            raise NotImplementedError(
                "Altering columns is not supported. "
                f"Could not convert column '{full_table_name}.{column_name}' "
                f"from '{current_type}' to '{compatible_sql_type}'."
            )
        try:
            alter_sql = f"""ALTER TABLE {str(full_table_name)}
                MODIFY {str(column_name)} {str(compatible_sql_type)}"""
            self.logger.info("Altering with SQL: %s", alter_sql)
            self.connection.execute(alter_sql)
        except Exception as e:
            raise RuntimeError(
                f"Could not convert column '{full_table_name}.{column_name}' "
                f"from '{current_type}' to '{compatible_sql_type}'."
            ) from e


class MySQLSink(SQLSink):
    """MySQL target sink class."""

    connector_class = MySQLConnector

    soft_delete_column_name = "x_sdc_deleted_at"
    version_column_name = "x_sdc_table_version"

    # @property
    # def schema_name(self) -> Optional[str]:
    #     """Return the schema name or `None` if using names with no schema part.
    #     Returns:
    #         The target schema name.
    #     """
    #     # Look for a default_target_scheme in the configuraion fle
    #     default_target_schema: str = self.config.get("default_target_schema", None)
    #     parts = self.stream_name.split("-")

    #     # 1) When default_target_scheme is in the configuration use it
    #     # 2) if the streams are in <schema>-<table> format use the
    #     #    stream <schema>
    #     # 3) Return None if you don't find anything
    #     if default_target_schema:
    #         return default_target_schema

    #     # Schema name not detected.
    #     return None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger.setLevel(logging.DEBUG)

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.
        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.
        Args:
            context: Stream partition or context dictionary.
        """
        # First we need to be sure the main table is already created

        conformed_records = (
            [self.conform_record(record) for record in context["records"]]
            if isinstance(context["records"], list)
            else (self.conform_record(record) for record in context["records"])
        )

        join_keys = [self.conform_name(key, "column") for key in self.key_properties]
        schema = self.conform_schema(self.schema)

        if self.key_properties:
            self.logger.info(f"Preparing table {self.full_table_name}")
            self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=schema,
                primary_keys=self.key_properties,
                as_temp_table=False,
            )

            tmp_table_name = self.full_table_name + "_temp"

            # Create a temp table (Creates from the table above)
            self.logger.info(f"Creating temp table {self.full_table_name}")
            self._connector.create_temp_table_from_table(
                from_table_name=self.full_table_name,
                temp_table_name=tmp_table_name
            )

            # Insert into temp table
            self.bulk_insert_records(
                full_table_name=tmp_table_name,
                schema=schema,
                records=conformed_records,
            )
            # Merge data from Temp table to main table
            self.logger.info(f"Merging data from temp table to {self.full_table_name}")
            self.merge_upsert_from_table(
                from_table_name=tmp_table_name,
                to_table_name=self.full_table_name,
                join_keys=join_keys,
            )

        else:
            self.bulk_insert_records(
                full_table_name=self.full_table_name,
                schema=schema,
                records=conformed_records,
            )

    def merge_upsert_from_table(self,
                                from_table_name: str,
                                to_table_name: str,
                                join_keys: List[str],
                                ) -> Optional[int]:

        """Merge upsert data from one table to another.
        Args:
            from_table_name: The source table name.
            to_table_name: The destination table name.
            join_keys: The merge upsert keys, or `None` to append.
            schema: Singer Schema message.
        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.
        """
        # TODO think about sql injeciton,
        # issue here https://github.com/MeltanoLabs/target-postgres/issues/22

        join_keys = [self.conform_name(key, "column") for key in join_keys]
        schema = self.conform_schema(self.schema)

        join_condition = " and ".join(
            [f"temp.{key} = target.{key}" for key in join_keys]
        )

        upsert_on_condition = " and ".join(
            [f"{key} = VALUES({key})" for key in join_keys]
        )

        merge_sql = f"""
            INSERT INTO {to_table_name} ({", ".join(schema["properties"].keys())})
                SELECT {", ".join(schema["properties"].keys())}
                FROM 
                    {from_table_name} temp
            ON DUPLICATE KEY UPDATE 
                {upsert_on_condition}
        """

        self.logger.info("Merging with SQL: %s", merge_sql)

        self.connection.execute(merge_sql)

        self.connection.execute("COMMIT")

        self.connection.execute(f"DROP TABLE {from_table_name}")

    def bulk_insert_records(
            self,
            full_table_name: str,
            schema: dict,
            records: Iterable[Dict[str, Any]],
    ) -> Optional[int]:
        """Bulk insert records to an existing destination table.
        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.
        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.
        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        insert_sql = self.generate_insert_statement(
            full_table_name,
            schema,
        )
        if isinstance(insert_sql, str):
            insert_sql = sqlalchemy.text(insert_sql)

        self.logger.info("Inserting with SQL: %s", insert_sql)

        columns = self.column_representation(schema)

        # temporary fix to ensure missing properties are added
        insert_records = []

        for record in records:
            insert_record = {}
            conformed_record = self.conform_record(record)
            for column in columns:
                # insert_record[column.name] = conformed_record.get(column.name)

                val = conformed_record.get(column.name)
                if (isinstance(val, Dict) or isinstance(val, List)):
                    val = json.dumps(val)

                insert_record[column.name] = val
            insert_records.append(insert_record)

        self.connection.execute(insert_sql, insert_records)
        self.connection.execute("COMMIT")

        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.

    def column_representation(
            self,
            schema: dict,
    ) -> List[Column]:
        """Returns a sql alchemy table representation for the current schema."""
        columns: list[Column] = []
        conformed_properties = self.conform_schema(schema)["properties"]
        for property_name, property_jsonschema in conformed_properties.items():
            columns.append(
                Column(
                    property_name,
                    self.connector.to_sql_type(property_jsonschema),
                )
            )
        return columns

    def snakecase(self, name):
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
        return name.lower()

    def move_leading_underscores(self, text):
        match = re.match(r'^(_*)(.*)', text)
        if match:
            result = match.group(2) + match.group(1)
            return result
        return text

    def conform_name(self, name: str, object_type: Optional[str] = None) -> str:
        """Conform a stream property name to one suitable for the target system.
        Transforms names to snake case by default, applicable to most common DBMSs'.
        Developers may override this method to apply custom transformations
        to database/schema/table/column names.
        Args:
            name: Property name.
            object_type: One of ``database``, ``schema``, ``table`` or ``column``.
        Returns:
            The name transformed to snake case.
        """
        # strip non-alphanumeric characters except _.
        name = re.sub(r"[^a-zA-Z0-9_]+", "_", name)

        # Move leading underscores to the end of the name
        name = self.move_leading_underscores(name)

        # convert to snakecase
        name = self.snakecase(name)
        # replace leading digit
        return replace_leading_digit(name)
