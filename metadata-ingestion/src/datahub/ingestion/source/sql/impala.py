import json
import logging
import re
from typing import Any, Dict, List, Optional

from pydantic.class_validators import validator
from pydantic.fields import Field

from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import create_engine, text, exc
# This import verifies that the dependencies are available.
from pyhive import hive  # noqa: F401
from pyhive.sqlalchemy_hive import HiveDate, HiveDecimal, HiveTimestamp

from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig, SQLAlchemyConfig
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    TimeTypeClass,
)
from datahub.utilities import config_clean
from datahub.utilities.hive_schema_to_avro import get_avro_schema_for_hive_column
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    SQLSourceReport,
    register_custom_type
)
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

logger = logging.getLogger(__name__)

register_custom_type(HiveDate, DateTypeClass)
register_custom_type(HiveTimestamp, TimeTypeClass)
register_custom_type(HiveDecimal, NumberTypeClass)

try:
    from databricks_dbapi.sqlalchemy_dialects.hive import DatabricksPyhiveDialect
    from pyhive.sqlalchemy_hive import _type_map
    from sqlalchemy import types, util
    from sqlalchemy.engine import reflection

    @reflection.cache  # type: ignore
    def dbapi_get_columns_patched(self, connection, table_name, schema=None, **kw):
        """Patches the get_columns method from dbapi (databricks_dbapi.sqlalchemy_dialects.base) to pass the native type through"""
        rows = self._get_table_columns(connection, table_name, schema)
        # Strip whitespace
        rows = [[col.strip() if col else None for col in row] for row in rows]
        # Filter out empty rows and comment
        rows = [row for row in rows if row[0] and row[0] != "# col_name"]
        result = []
        for col_name, col_type, _comment in rows:
            # Handle both oss hive and Databricks' hive partition header, respectively
            if col_name in ("# Partition Information", "# Partitioning"):
                break
            # Take out the more detailed type information
            # e.g. 'map<int,int>' -> 'map'
            #      'decimal(10,1)' -> decimal
            orig_col_type = col_type  # keep a copy
            col_type = re.search(r"^\w+", col_type).group(0)  # type: ignore
            try:
                coltype = _type_map[col_type]
            except KeyError:
                util.warn(
                    "Did not recognize type '%s' of column '%s'" % (col_type, col_name)
                )
                coltype = types.NullType  # type: ignore
            result.append(
                {
                    "name": col_name,
                    "type": coltype,
                    "nullable": True,
                    "default": None,
                    "full_type": orig_col_type,  # pass it through
                    "comment": _comment,
                }
            )
        return result

    DatabricksPyhiveDialect.get_columns = dbapi_get_columns_patched
except ModuleNotFoundError:
    pass
except Exception as e:
    logger.warning(f"Failed to patch method due to {e}")


class ImpalaConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = Field(default="impala", hidden_from_schema=True)

    # Hive SQLAlchemy connector returns views as tables.
    # See https://github.com/dropbox/PyHive/blob/b21c507a24ed2f2b0cf15b0b6abb1c43f31d3ee0/pyhive/sqlalchemy_hive.py#L270-L273.
    # Disabling views helps us prevent this duplication.
    include_views = Field(
        default=False,
        hidden_from_schema=True,
        description="Hive SQLAlchemy connector returns views as tables. See https://github.com/dropbox/PyHive/blob/b21c507a24ed2f2b0cf15b0b6abb1c43f31d3ee0/pyhive/sqlalchemy_hive.py#L270-L273. Disabling views helps us prevent this duplication.",
    )

    @validator("host_port")
    def clean_host_port(cls, v):
        return config_clean.remove_protocol(v)


@platform_name("Impala")
@config_class(ImpalaConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
class ImpalaSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, and tables
    - Column types associated with each table
    - Detailed table and storage information
    - Table, row, and column statistics via optional SQL profiling.

    """

    _COMPLEX_TYPE = re.compile("^(struct|map|array|uniontype)")

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "impala")
        self.report: SQLSourceReport = SQLSourceReport()
        self._alchemy_client = SQLAlchemyClient(config,self.report)

    @classmethod
    def create(cls, config_dict, ctx):
        config = ImpalaConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_schema_names(self, inspector):
        assert isinstance(self.config, ImpalaConfig)
        # This condition restricts the ingestion to the specified database.
        if self.config.database:
            return [self.config.database]
        else:
            return super().get_schema_names(inspector)

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: Dict[Any, Any],
        pk_constraints: Optional[Dict[Any, Any]] = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaField]:
        fields = super().get_schema_fields_for_column(
            dataset_name, column, pk_constraints
        )

        if self._COMPLEX_TYPE.match(fields[0].nativeDataType) and isinstance(
            fields[0].type.type, NullTypeClass
        ):
            assert len(fields) == 1
            field = fields[0]
            # Get avro schema for subfields along with parent complex field
            avro_schema = get_avro_schema_for_hive_column(
                column["name"], field.nativeDataType
            )

            new_fields = schema_util.avro_schema_to_mce_fields(
                json.dumps(avro_schema), default_nullable=True
            )

            # First field is the parent complex field
            new_fields[0].nullable = field.nullable
            new_fields[0].description = field.description
            new_fields[0].isPartOfKey = field.isPartOfKey
            return new_fields

        return fields

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            full_table = '`{}`.`{}`'.format(schema,table)
            rows = self._alchemy_client.execute_query('DESCRIBE FORMATTED {}'.format(full_table)).fetchall()
        
        except exc.OperationalError as e:
            # Does the table exist?
            regex_fmt = r'TExecuteStatementResp.*SemanticException.*Table not found {}'
            regex = regex_fmt.format(re.escape(full_table))
            if re.search(regex, e.args[0]):
                raise exc.NoSuchTableError(full_table)
            else:
                raise
        
        info_rows_delimiter = ('# Detailed Table Information', None, None)
        info_rows_delimiter_alternate = ('# Detailed Table Information', '', '')
        # Remove the column type specs.
        try:
            start_detailed_info_index = rows.index(info_rows_delimiter)
        except ValueError:
            start_detailed_info_index = rows.index(info_rows_delimiter_alternate)
        assert start_detailed_info_index >= 0
        rows = rows[start_detailed_info_index:]

        # Generate properties dictionary.
        properties = {}
        active_heading = None
        for col_name, data_type, value in rows:
            col_name: str = col_name.rstrip()
            if col_name.startswith('# '):
                continue
            elif col_name == "" and data_type is None:
                active_heading = None
                continue
            elif col_name != "" and data_type is None:
                active_heading = col_name
            elif col_name != "" and data_type is not None:
                properties[col_name] = data_type.strip()
            else:
                # col_name == "", data_type is not None
                prop_name = "{} {}".format(active_heading, data_type.rstrip())
                properties[prop_name] = value.rstrip()

        description = properties.get('Table Parameters: comment', None)
        self.report.report_warning(
            'properties',
                f"{properties}",
            )

        # return {'text': properties.get('Table Parameters: comment', None), 'properties': properties, location: None}
        return description, properties, location

    def _get_columns(
        self, dataset_name: str, inspector: Inspector, schema: str, table: str
    ) -> List[dict]:
        columns = []
        MISSING_COLUMN_INFO = "missing column information"
        try:
            table = '`{}`'.format(table)
            schema = '`{}`'.format(schema)

            columns = inspector.get_columns(table, schema)
            if len(columns) == 0:
                self.report.report_warning(MISSING_COLUMN_INFO, dataset_name)
        except Exception as e:
            self.report.report_warning(
                dataset_name,
                f"unable to get column information due to an error -> {e}",
            )
        return columns

class SQLAlchemyClient:
    def __init__(self, config: SQLAlchemyConfig, report):
        self.config = config
        self.report: SQLSourceReport = report
        self.connection = self._get_connection()

    def _get_connection(self) -> Any:
        url = self.config.get_sql_alchemy_url()
        self.report.report_warning(
            'url',
                f"{url}",
            )
        engine = create_engine(url, **self.config.options)
        conn = engine.connect()
        return conn

    def execute_query(self, query: str) -> Iterable:
        """
        Create an iterator to execute sql.
        """
        results = self.connection.execute(text(query))
        # return iter(results)
        return results