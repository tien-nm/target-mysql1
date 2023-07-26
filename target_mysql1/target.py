"""MySQL target class."""

from __future__ import annotations

import io
import simplejson as json

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget
import typing as t

from target_mysql.sinks import (
    MySQLSink,
)


class TargetMySQL(SQLTarget):
    """Sample target for MySQL."""

    name = "target-mysql1"

    default_sink_class = MySQLSink

    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="SQLAlchemy connection string",
        ),
        th.Property(
            "driver_name",
            th.StringType,
            default="mysql",
            description="SQLAlchemy driver name",
        ),
        th.Property(
            "username",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="MySQL username",
        ),
        th.Property(
            "password",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="MySQL password",
        ),
        th.Property(
            "host",
            th.StringType,
            description="MySQL host",
        ),
        th.Property(
            "port",
            th.StringType,
            description="MySQL port",
        ),
        th.Property(
            "database",
            th.StringType,
            description="MySQL database",
        ),
        th.Property(
            "table_name_pattern",
            th.StringType,
            description="MySQL table name pattern",
            default="${TABLE_NAME}"
        ),
        th.Property(
            "lower_case_table_names",
            th.BooleanType,
            description="Lower case table names",
            default=True
        ),
        th.Property(
            "allow_column_alter",
            th.BooleanType,
            description="Allow column alter",
            default=False
        ),
        th.Property(
            "replace_null",
            th.BooleanType,
            description="Replace null to blank",
            default=False
        ),

    ).to_dict()

    schema_properties = {}

    def _process_lines(self, file_input: t.IO[str]) -> t.Counter[str]:
        if self.config.get("replace_null", False):
            processed_input = io.StringIO()
            for line in file_input:
                data = self.deserialize_json(line.strip())

                if data.get('type', '') == 'SCHEMA':
                    self.schema_properties = data['schema']['properties']
                elif data.get('type', '') == 'RECORD':
                    for key, value in data.get('record', {}).items():
                        if value is not None:
                            continue

                        # https://json-schema.org/understanding-json-schema/reference/type.html
                        _type = self.schema_properties[key]['type']
                        data_types = _type if isinstance(_type, list) else [_type]

                        if "null" in data_types:
                            continue
                        if "string" in data_types:
                            data['record'][key] = ""
                        elif "object" in data_types:
                            data['record'][key] = {}
                        elif "array" in data_types:
                            data['record'][key] = []
                        elif "boolean" in data_types:
                            data['record'][key] = False
                        else:
                            data['record'][key] = 0

                processed_input.write(json.dumps(data) + '\n')
            processed_input.seek(0)
            return super()._process_lines(processed_input)
        else:
            return super()._process_lines(file_input)


if __name__ == "__main__":
    TargetMySQL.cli()
