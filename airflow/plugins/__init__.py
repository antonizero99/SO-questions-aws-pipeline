from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.S3ToRedshiftOperator
    ]
