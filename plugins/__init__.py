from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin
import operators
import helpers

# Defining the plugin class
class GusPlugin(AirflowPlugin):
    name = "gus_plugin"
    operators = [
        operators.GetDataOperator,
        operators.GetMetaOperator,
        operators.GetFactsOperator,
        operators.CreateTablesOperator,
        operators.LoadToRedshiftOperator,
        operators.LoadTableOperator,
        operators.DataQualityOperator   
    ]
    helpers = [
        helpers.SqlQueries
    ]