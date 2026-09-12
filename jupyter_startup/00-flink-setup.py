from pyflink.table import EnvironmentSettings, TableEnvironment
from IPython.core.magic import register_cell_magic

table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
table_env.get_config().set("execution.target", "remote")
table_env.get_config().set("jobmanager.rpc.address", "jobmanager")
table_env.get_config().set("jobmanager.rpc.port", "6123")
table_env.get_config().set("parallelism.default", "1")


@register_cell_magic
def sql(line, cell):
    table_env.execute_sql(cell).print()
