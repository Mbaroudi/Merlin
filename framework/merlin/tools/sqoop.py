#
# Copyright (c) 2015 EPAM Systems, Inc. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
# Redistributions in binary form must reproduce the above copyright notice, this
# list of conditions and the following disclaimer in the documentation and/or
# other materials provided with the distribution.
# Neither the name of the EPAM Systems, Inc. nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# See the NOTICE file and the LICENSE file distributed with this work
# for additional information regarding copyright ownership and licensing.
#

"""
Apache Sqoop is a tool designed for efficiently transferring bulk data between Apache Hadoop and structured datastores
such as relational databases, enterprise data warehouses and NoSQL systems.

This client provides Python wrapper for Sqoop related tools:
    - sqoop-import - hadoopframework.tools.sqoop_client.SqoopImport  - imports an individual table from an RDBMS to HDFS.
    - sqoop-export - hadoopframework.tools.sqoop_client.SqoopExport  - exports a set of files from HDFS to an RDBMS.
                                                    The target table must already exist in the database.

SQOOP IMPORT EXAMPLES:
The following examples illustrate how to use the import tool in a variety of situations.

A basic import of a table named EMPLOYEES in the corp database
        Sqoop.import_data().from_rdbms(
            rdbms="mysql",
            host="db.foo.com",
            database="corp",
            username="mysql_user",
            password_file=".mysql.pwd"
        ).table("EMPLOYEES").to_hdfs(target_dir="/data/EMPLOYEES").run()

    Will be transformed to next Sqoop CLI command :
        sqoop-import \
        --connect jdbc:mysql://db.foo.com/corp \
        --username mysql_user \
        --password-file .mysql.pwd \
        --table EMPLOYEES \
        --target-dir /data/EMPLOYEES \
        --as-textfile


Selecting specific columns from the EMPLOYEES table:
        Sqoop.import_data().from_rdbms(
            rdbms="mysql",
            host="db.foo.com",
            database="corp",
            username="mysql_user",
            password_file=".mysql.pwd"
        ).table(table="EMPLOYEES",
                columns=['id','name', 'department']
        ).to_hdfs(target_dir="/data/EMPLOYEES").run()

    Will be transformed to next Sqoop CLI command :
        sqoop-import \
        --connect jdbc:mysql://db.foo.com/corp \
        --username mysql_user \
        --password-file .mysql.pwd \
        --table EMPLOYEES \
        --columns 'id,name,department' \
        --target-dir /data/EMPLOYEES \
        --as-textfile

Importing employees which where hired after 01/01/2010
    Sqoop.import_data().from_rdbms(
        rdbms="mysql",
        host="db.foo.com",
        database="corp",
        username="mysql_user",
        password_file=".mysql.pwd"
    ).table(table="EMPLOYEES",
            where="start_date > '2010-01-01'"
    ).to_hdfs(target_dir="/data/EMPLOYEES").

     Will be transformed to next Sqoop CLI command :
     sqoop-import \
     --connect jdbc:mysql://db.foo.com/corp \
     --username mysql_user \
     --password-file .mysql.pwd \
     --table EMPLOYEES \
     --where "start_date > '2010-01-01'" \
     --target-dir /data/EMPLOYEES \
     --as-textfile


Controlling the import parallelism (using 8 parallel tasks):
    Sqoop.import_data().from_rdbms(
        rdbms="mysql",
        host="db.foo.com",
        database="corp",
        username="mysql_user",
        password_file=".mysql.pwd"
    ).table('EMPLOYEES').use_num_mappers(8).to_hdfs(target_dir="/data/EMPLOYEES")

    Will be transformed to next Sqoop CLI command :
    sqoop-import \
    --connect jdbc:mysql://db.foo.com/corp \
    --username mysql_user \
    --password-file .mysql.pwd \
    --table EMPLOYEES \
    --target-dir /data/EMPLOYEES \
    --num-mappers 8 \
    --as-textfile

Staring data in SequenceFiles
    Sqoop.import_data().from_rdbms(
            rdbms="mysql",
            host="db.foo.com",
            database="corp",
            username="mysql_user",
            password_file=".mysql.pwd"
        ).table(table="EMPLOYEES",
                columns=['id','name', 'department']
        ).use_file_format("sequencefile").to_hdfs(target_dir="/data/EMPLOYEES").run()

    Will be transformed to next Sqoop CLI command :
        sqoop-import \
        --connect jdbc:mysql://db.foo.com/corp \
        --username mysql_user \
        --password-file .mysql.pwd \
        --table EMPLOYEES \
        --columns 'id',name,department' \
        --target-dir /data/EMPLOYEES \
        --as-sequencefile


Specifying the delimiters to use in a text-mode import:
        Sqoop.import_data().from_rdbms(
            rdbms="mysql",
            host="db.foo.com",
            database="corp",
            username="mysql_user",
            password_file=".mysql.pwd"
        ).table(table="EMPLOYEES").with_input_parsing(
            optionally_enclosed_by='\\"',
            fields_terminated_by='|',
            lines_terminated_by='\\n'
        ).to_hdfs(target_dir="/data/EMPLOYEES").run()

    Will be transformed to next Sqoop CLI command :
        sqoop-import \
        --connect jdbc:mysql://db.foo.com/corp \
        --username mysql_user \
        --password-file .mysql.pwd \
        --table EMPLOYEES \
        --target-dir /data/EMPLOYEES \
        --as-textfile \
        --input-fields-terminated-by '|' \
        --input-lines-terminated-by '\n' \
        --input-optionally-enclosed-by '\"'


Free-form query import
     Sqoop.import_data().from_rdbms(
        rdbms="mysql",
        host="db.foo.com",
        database="corp",
        username="mysql_user",
        password_file=".mysql.pwd"
    ).query(
        query='SELECT a.*, b.* FROM a JOIN b on (a.id == b.id)',
        split_by='a.id'
    ).to_hdfs(target_dir="/data/EMPLOYEES").run()

    Will be transformed to next Sqoop CLI command :
    sqoop-import \
    --connect jdbc:mysql://db.foo.com/corp \
    --username mysql_user \
    --password-file .mysql.pwd \
    --query 'SELECT a.*, b.* FROM a JOIN b on (a.id == b.id) WHERE $CONDITIONS' \
    --split-by a.id --target-dir /data/EMPLOYEES \
    --as-textfile

Incremental import. Load all records from Employees tables which were modified after 2014-11-20 15:27
    Sqoop.import_data().from_rdbms(
        rdbms="mysql",
        host="db.foo.com",
        database="corp",
        username="mysql_user",
        password_file=".mysql.pwd"
    ).table('EMPLOYEES').with_incremental(
        incremental='append',
        check_column='create_date',
        last_value='2014-11-20 15:27'
    ).to_hdfs(target_dir="/data/EMPLOYEES").run()

    Will be transformed to next Sqoop CLI command :
    sqoop-import \
    --connect jdbc:mysql://db.foo.com/corp \
    --username mysql_user \
    --password-file .mysql.pwd \
    --table EMPLOYEES \
    --target-dir /data/EMPLOYEES \
    --as-textfile \
    --incremental append \
    --check-column create_date \
    --last-value '2014-11-20 15:27'

Importing the data to Hive
    Sqoop.import_data().from_rdbms(
        rdbms="mysql",
        host="db.foo.com",
        database="corp",
        username="mysql_user",
        password_file=".mysql.pwd"
    ).table('EMPLOYEES',
            where="start_date > '2010-01-01'"
    ).use_num_mappers(8).to_hive().run()

     Will be transformed to next Sqoop CLI command :
     sqoop-import \
     --connect jdbc:mysql://db.foo.com/corp \
     --username mysql_user \
     --password-file .mysql.pwd \
     --table EMPLOYEES \
     --where "start_date > '2010-01-01'" \
     --num-mappers 8 \
     --as-textfile \
     --hive-import

Importing the data to HBase
    Sqoop.import_data().from_rdbms(
        rdbms="mysql",
        host="db.foo.com",
        database="corp",
        username="mysql_user",
        password_file=".mysql.pwd"
    ).table('EMPLOYEES',
            where="start_date > '2010-01-01'"
    ).to_hbase(
        hbase_table='htable',
        column_family='h_family').run()

    Will be transformed to next Sqoop CLI command :
    sqoop-import \
    --connect jdbc:mysql://db.foo.com/corp \
    --username mysql_user \
    --password-file .mysql.pwd \
    --table EMPLOYEES \
    --where "start_date > '2010-01-01'" \
    --as-textfile \
    --hbase-table htable \
    --column-family h_family


SQOOP EXPORT EXAMPLES:
    Sqoop.export_data().from_hdfs(
        export_dir='/user/data'
    ).to_rdbms(
        rdbms='mysql',
        database='CompanyDB',
        username='mysqluser').table(
        table="Employees"
    ).run()


KNOWN BUGS AND LIMITATION:

"""
from merlin.common.configurations import Configuration
from merlin.common.metastores import IniFileMetaStore

from string import Template
import os
import uuid

from merlin.common.logger import get_logger
from merlin.common.shell_command_executor import execute_shell_command
from merlin.common.exceptions import SqoopCommandError, ConfigurationError
from merlin.common.utils import ListUtility


class Sqoop(object):
    """          
    Base class for SQOOP related tools.
    """

    LOG = get_logger("Sqoop")

    @staticmethod
    def import_data(executor=execute_shell_command):
        """
        Creates wrapper for sqoop-import command line utility
        :param executor: The interface used by the client to run command.
        :rtype: SqoopImport

        """
        path_to_config = os.path.join(os.path.dirname(__file__),
                                      'resources', 'sqoop-default.ini')
        metastore = IniFileMetaStore(file=path_to_config)
        config = Configuration.load(metastore, readonly=False, accepts_nulls=True)
        return SqoopImport(config=config, executor=executor)

    @staticmethod
    def export_data(executor=execute_shell_command):
        """
        Creates wrapper for sqoop-export command line utility
        :param executor: The interface used by the client to run command.
        :rtype: SqoopExport

        """
        path_to_config = os.path.join(os.path.dirname(__file__),
                                      'resources', 'sqoop-default.ini')
        metastore = IniFileMetaStore(file=path_to_config)
        config = Configuration.load(metastore, readonly=False, accepts_nulls=True)
        return SqoopExport(config=config, executor=executor)

    def __require_attr__(self, key):
        """
        Loads required attribute from job configuration and converts it to command specific argument
        ConfigurationError will be thrown in case option was not found
        :param key: config option name
        :type key: str
        :rtype: list

        """

        return ["--{0} {1}".format(self.__format_attr__(key),
                                   self.get(key=key, required=True))]

    def __optional_attr__(self, key):
        """
        Loads attribute from job configuration and converts it to command specific argument
        Command argument won't be added in case config option was not found
        :param key: config option name
        :type key: str
        :rtype: list

        """
        list_command = []
        if self.has_option(key):
            list_command.append("--{0} {1}".format(self.__format_attr__(key), self.get(key)))
        return list_command

    def __config_marker__(self, key):
        """
        Looks for specific configuration option and adds argument to command in case option was found
        :type key: str
        :rtype: list

        """
        list_command = []
        if self.has_option(key) and (self.get(key) == 'enabled'):
            list_command.append("--{0}".format(self.__format_attr__(key)))
        return list_command

    def __format_attr__(self, command=None):
        """
        Format name attribute
        :type command: str
        :rtype: str

        """
        command = command.replace("_", "-")
        return command

    def __format_prop__(self, command=None):
        """
        Format name property
        :type command: str
        :rtype: str

        """
        command = command.replace("_", ".")
        return command

    def __set_attr__(self, key=None, value=None):
        """
        Configuration method for set attribute
        :type key: str

        """
        if value:
            self._config.set(self.name, key, value)

    def __set_marker_enabled__(self, key=None, value=False):
        """
        Configuration method for set marker to enable mod
        :type key: str

        """
        if value:
            self._config.set(self.name, key, 'enabled')

    def __config_hadoop_properties__(self):
        """
        Adds Hadoop generic arguments to command
        :rtype: list

        """
        list_command = []
        if self.has_option(TaskOptions.CONFIG_KEY_SQOOP_HADOOP_PROPERTIES):
            _list = self.get_list(TaskOptions.CONFIG_KEY_SQOOP_HADOOP_PROPERTIES)
            for value in _list:
                list_command.append("-D{0}".format(value))

        return list_command

    def __config_jar__(self):
        """
        Configuration method for jar's path
        :rtype: list

        """
        list_command = []
        if self.has_option(TaskOptions.CONFIG_KEY_SQOOP_LIBJARS):
            list_command.append("-libjars={0}".format(self.get(TaskOptions.CONFIG_KEY_SQOOP_LIBJARS)))

        return list_command

    def __config_direct_mode__(self):
        """
        Configuration method for direct mode
        :rtype: list

        """
        list_command = []
        if self.has_option(TaskOptions.CONFIG_KEY_SQOOP_DIRECT_MODE_PROPERTIES):
            list_command.append('--')
            _list = self.get_list(TaskOptions.CONFIG_KEY_SQOOP_DIRECT_MODE_PROPERTIES)
            for value in _list:
                list_command.append("--{0}".format(value))

        return list_command

    def __config_rdbms__(self):
        """
        Configuration method for RDBMS
        :rtype: list

        """
        host = self.get(key=TaskOptions.CONFIG_KEY_SQOOP_HOST, required=True)
        if 'jdbc' in host:
            return ["--connect {0}".format(host)]
        else:
            return ["--connect jdbc:{0}://{1}/{2}".format(
                self.get(key=TaskOptions.CONFIG_KEY_SQOOP_RDBMS, required=True),
                host, self.get(key=TaskOptions.CONFIG_KEY_SQOOP_DATABASE, required=True))]

    def __config_credentials__(self):
        """
        Configuration method for credentials
        :rtype: list

        """
        list_command = []
        list_command.extend(self.__require_attr__(TaskOptions.CONFIG_KEY_SQOOP_USERNAME))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_PASSWORD_FILE))

        return list_command

    def __config_parse__(self):
        """
        Configuration method for parsing
        :rtype: list

        """
        list_command = []
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_ENCLOSED_BY))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_ESCAPED_BY))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_FIELDS_TERMINATED_BY))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_LINES_TERMINATED_BY))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_OPTIONALLY_ENCLOSED_BY))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_ENCLOSED_BY))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_ESCAPED_BY))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_FIELDS_TERMINATED_BY))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_LINES_TERMINATED_BY))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_OPTIONALLY_ENCLOSED_BY))
        list_command.extend(self.__config_marker__(TaskOptions.CONFIG_KEY_SQOOP_MYSQL_DELIMITERS))

        return list_command

    def __config_specific__(self):
        """
        Configuration method for specific attributes
        :rtype: list

        """
        list_command = []
        for key in self.specific_attributes:
            list_command.append("--{0} {1}".format(self.__format_attr__(key),
                                                   self.specific_attributes[key]))

        return list_command

    @staticmethod
    def __quotes_wrapper__(attr=None):
        """
        :type attr: str
        :rtype: str

        """
        if attr:
            if attr[0] != "'":
                return "'{0}'".format(attr)
        return attr

    @staticmethod
    def __double_quotes_wrapper__(attr=None):
        """
        :type attr: str
        :rtype: str

        """
        if attr:
            if attr[0] != "\"":
                return "\"{0}\"".format(attr)
        return attr

    def build(self):
        """
        Build any Sqoop's command
        :rtype: str

        """
        list_command = []
        list_command.extend(self.__config_hadoop_properties__())
        list_command.extend(self.__config_jar__())
        list_command.extend(self.__config_rdbms__())
        list_command.extend(self.__config_credentials__())
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_DRIVER))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_CONNECTION_MANAGER))

        if self.command == "sqoop.import":
            list_command.extend(self.__config_import__())

        if self.command == "sqoop.export":
            list_command.extend(self.__config_export__())

        list_command.extend(self.__config_parse__())
        list_command.extend(self.__config_specific__())
        list_command.extend(self.__config_marker__(TaskOptions.CONFIG_KEY_SQOOP_DIRECT))
        list_command.extend(self.__config_direct_mode__())

        return " ".join(list_command)

    def with_hadoop_properties(self, **properties):
        """
        Specify hadoop properties
        :type properties: dict

        """
        if self.has_option(TaskOptions.CONFIG_KEY_SQOOP_HADOOP_PROPERTIES):
            command = self.get(TaskOptions.CONFIG_KEY_SQOOP_HADOOP_PROPERTIES)
        else:
            command = ""
        for key, value in properties.iteritems():
            if command == "":
                command = "{0}={1}".format(self.__format_prop__(key), value)
            else:
                command = "{0}\n{1}={2}".format(command, self.__format_prop__(key), value)

        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_HADOOP_PROPERTIES, command)

        return self

    def with_direct_mode(self, direct_split_size=None, **direct_arguments):
        """
        Set Sqoop to use direct import channel.
        :param direct_split_size: Split size.
            Sqoop will split the input stream every 'n' bytes when importing in direct mode.
        :param direct_arguments:
        :type direct_split_size: int
        :type direct_arguments: dict

        """
        self.__set_marker_enabled__(TaskOptions.CONFIG_KEY_SQOOP_DIRECT, True)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_DIRECT_SPLIT_SIZE, direct_split_size)
        if self.has_option(TaskOptions.CONFIG_KEY_SQOOP_DIRECT_MODE_PROPERTIES):
            command = self.get(TaskOptions.CONFIG_KEY_SQOOP_DIRECT_MODE_PROPERTIES)
        else:
            command = ""
        for key, value in direct_arguments.iteritems():
            if command == "":
                command = "{0}={1}".format(self.__format_attr__(key), value)
            else:
                command = "{0}\n{1}={2}".format(command, self.__format_attr__(key), value)

        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_DIRECT_MODE_PROPERTIES, command)

        return self

    def with_input_parsing(self, enclosed_by=None, escaped_by=None, fields_terminated_by=None,
                           lines_terminated_by=None, optionally_enclosed_by=None):
        """
        Configure input data formatting arguments
        :param enclosed_by:  required field enclosing character
        :param escaped_by:  escape character
        :param fields_terminated_by: field separator character
        :param lines_terminated_by: end-of-line character
        :param optionally_enclosed_by:  field enclosing character
        :type enclosed_by: str
        :type escaped_by: str
        :type fields_terminated_by: str
        :type lines_terminated_by: str
        :type optionally_enclosed_by: str

        """
        enclosed_by = Sqoop.__quotes_wrapper__(enclosed_by)
        escaped_by = Sqoop.__quotes_wrapper__(escaped_by)
        fields_terminated_by = Sqoop.__quotes_wrapper__(fields_terminated_by)
        lines_terminated_by = Sqoop.__quotes_wrapper__(lines_terminated_by)
        optionally_enclosed_by = Sqoop.__quotes_wrapper__(optionally_enclosed_by)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_ENCLOSED_BY, enclosed_by)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_ESCAPED_BY, escaped_by)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_FIELDS_TERMINATED_BY, fields_terminated_by)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_LINES_TERMINATED_BY, lines_terminated_by)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_OPTIONALLY_ENCLOSED_BY, optionally_enclosed_by)

        return self

    def with_output_parsing(self, enclosed_by=None, escaped_by=None, fields_terminated_by=None,
                            lines_terminated_by=None, optionally_enclosed_by=None,
                            mysql_delimiters=False):
        """
        Configure output line formatting arguments
        :param enclosed_by: required field enclosing character
        :param escaped_by: escape character
        :param fields_terminated_by:  field separator character
        :param lines_terminated_by: end-of-line character
        :param optionally_enclosed_by: enclosing character
        :param mysql_delimiters:
        :type enclosed_by: str
        :type escaped_by: str
        :type fields_terminated_by: str
        :type lines_terminated_by: str
        :type optionally_enclosed_by: str
        :type mysql_delimiters: bool
        
        """
        enclosed_by = Sqoop.__quotes_wrapper__(enclosed_by)
        escaped_by = Sqoop.__quotes_wrapper__(escaped_by)
        fields_terminated_by = Sqoop.__quotes_wrapper__(fields_terminated_by)
        lines_terminated_by = Sqoop.__quotes_wrapper__(lines_terminated_by)
        optionally_enclosed_by = Sqoop.__quotes_wrapper__(optionally_enclosed_by)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_ENCLOSED_BY, enclosed_by)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_ESCAPED_BY, escaped_by)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_FIELDS_TERMINATED_BY, fields_terminated_by)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_LINES_TERMINATED_BY, lines_terminated_by)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_OPTIONALLY_ENCLOSED_BY, optionally_enclosed_by)
        self.__set_marker_enabled__(TaskOptions.CONFIG_KEY_SQOOP_MYSQL_DELIMITERS, mysql_delimiters)

        return self

    def with_lib_jar(self, libjars=None):
        """
        Add libs to application's classpath
        :param libjars: comma-separated list of jar files to be added to application classpath
        :type libjars: str
        
        """
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_LIBJARS, libjars)

        return self

    def with_specific_attr(self, **attributes):
        """
        Specify specific attributes
        :type attributes: dict

        """
        for key, value in attributes.iteritems():
            self.specific_attributes[key] = value
        return self

    def with_attr(self, **attributes):
        """
        Specify common attributes

        """

        for key, value in attributes.iteritems():
            self.__set_attr__(key, value)
        return self

    def use_num_mappers(self, num_mappers):
        """
        Specify number of mappers
        :param num_mappers:  number of map tasks (parallel processes) to use
        :type num_mappers: int

        """

        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_NUM_MAPPERS, str(num_mappers))
        return self

    def has_option(self, key):
        """
        Check if attribute at the given key exists in  job specific or sqoop.common section of the Configuration.

        :param key: attribute name
        :return:  True in case attribute was found otherwise False
        """
        if not self._config.has(section=self.name, key=key):
            if not self._config.has(section=self.command, key=key):
                return self._config.has(section="sqoop.common", key=key)
            else:
                return True
        else:
            return True

    def get(self, key, required=False):
        """
        Get value at the given key from Configuration.
        Throws an ConfigurationError when a required property is not found.
        :param key: config option name
        :param required:
        :type key: str
        :type required: bool
        :rtype str

        """
        # try to load config option from job speciofic section
        _value = self._config.get(self.name, key)
        if not _value:
            # try to load option from command specific section
            _value = self._config.get(self.command, key)
        if not _value:
            # try to load option from sqoop common section
            _value = self._config.get("sqoop.common", key)

        if not _value and required:
            raise ConfigurationError("Configuration option '{0}' is required".format(key))
        else:
            return _value

    def get_list(self, key, required=False):
        """
        Get value at the given key from Configuration.
        Throws an ConfigurationError when a required property is not found.
        :param key: config option name
        :param required:
        :type key: str
        :type required: bool
        :rtype str

        """
        # try to load config option from job speciofic section
        _value = self._config.get_list(self.name, key)
        if not _value:
            # try to load option from command specific section
            _value = self._config.get_list(self.command, key)
        if not _value:
            # try to load option from sqoop common section
            _value = self._config.get_list("sqoop.common", key)

        if not _value and required:
            raise ConfigurationError("Configuration option '{0}' is required".format(key))
        else:
            return _value


class SqoopImport(Sqoop):
    """
    Sqoop's import command.
    Provides logic to load  an individual table from an RDBMS to HDFS.
    Each row from a table is represented as a separate record in HDFS.
    Records can be stored as text files (one record per line), or in binary representation as Avro or SequenceFiles.

    Sqoop Import job configuration can be loaded from external Ini file.
    Job name is used as a name of the configuration section containing job specific options.
    Allowed configuration keys are listed in hadoopframework.tools.sqoop.TaskOptions

    Alternatively, you can also use provided API to configure and run import job.

    """

    def __init__(self, name=None, config=None, executor=execute_shell_command):
        """

        :param name: job name. used to store/load job specific settings from configurations
        :param config: job configurations
        :param executor: The interface used by the client to launch Sqoop import job.
        """
        self.name = name if name else "SQOOP_TASK_{0}".format(uuid.uuid4())
        self.__executor = executor
        self.specific_attributes = {}
        self.command = "sqoop.import"
        self._config = config if config else Configuration.create(
            readonly=False,
            accepts_nulls=True
        )
        self._process = None

    @staticmethod
    def load_preconfigured_job(name=None, config=None, executor=execute_shell_command):
        """
        Creates instance of SqoopImport. Configure it with options
        :param config: sqoop job configurations
        :param name: sqoop job identifier.
             Will be used as a name of the section with job-specific configurations.
        :param executor:
        """
        if name:
            Sqoop.LOG.info("Load Sqoop Import attributes from section's in configuration "
                           "[sqoop], [sqoop.import] and [{0}]".format(name))
        else:
            Sqoop.LOG.info("Load Sqoop Import attributes from section's in configuration "
                           "[sqoop] and [sqoop.import]")
        return SqoopImport(name, config, executor)

    def __config_import__(self):
        """
        Prepare command attributes to launch sqoop-import to HDFS
        :rtype: list

        """
        list_command = []

        if not self.has_option(TaskOptions.CONFIG_KEY_SQOOP_QUERY):
            list_command.extend(self.__require_attr__(TaskOptions.CONFIG_KEY_SQOOP_TABLE))
            list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_COLUMNS))
            list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_WHERE))
            list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_TARGET_DIR))
            list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_SPLIT_BY))
            list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_NUM_MAPPERS))
        elif not self.has_option(TaskOptions.CONFIG_KEY_SQOOP_TABLE):
            list_command.extend(self.__require_attr__(TaskOptions.CONFIG_KEY_SQOOP_QUERY))
            list_command.extend(self.__require_attr__(TaskOptions.CONFIG_KEY_SQOOP_SPLIT_BY))
            list_command.extend(self.__require_attr__(TaskOptions.CONFIG_KEY_SQOOP_TARGET_DIR))
            list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_NUM_MAPPERS))
            list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_BOUNDARY_QUERY))
        else:
            raise SqoopCommandError("You can't use table and query together")

        if self.has_option(TaskOptions.CONFIG_KEY_SQOOP_AS):
            list_command.append(self.get(TaskOptions.CONFIG_KEY_SQOOP_AS))

        list_command.extend(self.__config_compress__())
        list_command.extend(self.__config_marker__(TaskOptions.CONFIG_KEY_SQOOP_APPEND))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_INLINE_LOB_LIMIT))

        if self.has_option(TaskOptions.CONFIG_KEY_SQOOP_INCREMENTAL):
            list_command.extend(self.__config_incremental__())

        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_NULL_STRING))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_NULL_NON_STRING))

        if self.has_option(TaskOptions.CONFIG_KEY_SQOOP_HIVE_IMPORT):
            list_command.extend(self.__config_hive__())
        else:
            list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_MAP_COLUMN_JAVA))

        if self.has_option('hbase_import'):
            list_command.extend(self.__config_hbase__())

        list_command.extend(self.__config_marker__(TaskOptions.CONFIG_KEY_SQOOP_DIRECT_SPLIT_SIZE))

        return list_command

    def __config_hive__(self):
        """
        Prepare command attributes to launch sqoop-import to Hive
        :rtype: list

        """
        list_command = []

        list_command.extend(self.__config_marker__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_IMPORT))
        list_command.extend(self.__config_marker__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_OVERWRITE))
        list_command.extend(self.__config_marker__(TaskOptions.CONFIG_KEY_SQOOP_CREATE_HIVE_TABLE))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_TABLE))
        list_command.extend(self.__config_marker__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_DROP_IMPORT_DELIMS))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_DELIMS_REPLACEMENT))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_PARTITION_KEY))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_PARTITION_VALUE))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_MAP_COLUMN_HIVE))

        return list_command

    def __config_hbase__(self):
        """
        Prepare command attributes to launch sqoop-import to HBase
        :rtype: list

        """

        list_command = []
        list_command.extend(self.__require_attr__(TaskOptions.CONFIG_KEY_SQOOP_HBASE_TABLE))
        list_command.extend(self.__config_marker__(TaskOptions.CONFIG_KEY_SQOOP_HBASE_CREATE_TABLE))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_HBASE_ROW_KEY))
        list_command.extend(self.__require_attr__(TaskOptions.CONFIG_KEY_SQOOP_COLUMN_FAMILY))

        return list_command

    def __config_compress__(self):
        """
        Configuration method for compress data
        :rtype: list

        """
        list_command = []
        list_command.extend(self.__config_marker__(TaskOptions.CONFIG_KEY_SQOOP_COMPRESS))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_COMPRESSION_CODEC))

        return list_command

    def __config_incremental__(self):
        """
        Prepare command arguments required for incremental import
        :rtype: list

        """
        list_command = []
        if self.get(TaskOptions.CONFIG_KEY_SQOOP_INCREMENTAL) == 'append' \
                or self.get(TaskOptions.CONFIG_KEY_SQOOP_INCREMENTAL) == 'lastmodified':
            list_command.extend(self.__require_attr__(TaskOptions.CONFIG_KEY_SQOOP_INCREMENTAL))
        else:
            raise SqoopCommandError("You must specify one incremental mode from list: "
                                    "'append', 'lastmodified'")
        list_command.extend(self.__require_attr__(TaskOptions.CONFIG_KEY_SQOOP_CHECK_COLUMN))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_LAST_VALUE))

        return list_command

    def run(self):
        """
        Runs Sqoop Import command
        :rtype:

        """
        Sqoop.LOG.info("Running Sqoop Import Job")
        self._process = self.__executor('sqoop-import', self.build())
        self._process.if_failed_raise(SqoopCommandError("Sqoop Job failed"))
        return self._process

    def from_rdbms(self, rdbms=None, host=None, database=None, username=None, password_file=None):
        """
        Configures JDBC connection to datasource.

        :param rdbms: the name of the datasource driver which will be used to accessed to database.
            E.g.: mysql, microsoft:sqlserver, oracle:thin, etc.
            Parameter is required in case host param doesn't contain complete jdbc connection string
        :param host: host name and port number of the computer hosting your database.
            Jdbc connection string can be passed as host param.
        :param database: the name of the database or service to connect to.
        :param username: the database user on whose behalf the connection is being made
        :param password_file: path for a file containing the authentication password

        :type rdbms: str
        :type host: str
        :type database: str
        :type username: str
        :type password_file: str

        """

        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_RDBMS, rdbms)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_HOST, host)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_DATABASE, database)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_USERNAME, username)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_PASSWORD_FILE, password_file)

        return self

    def table(self, table=None, columns=None, where=None):
        """
        Configures select query which will be used to import data from database

        :param table: Table to read. This argument can also identify a VIEW or other table-like entity in a database.
        :param columns: Columns to import from table. By default, all columns within a table are selected for import.
        :param where: WHERE clause to use during import

        :type table: str
        :type columns: str
        :type where: str
        :rtype: SqoopImport

        """
        where = Sqoop.__double_quotes_wrapper__(where)
        if isinstance(columns, list):
            columns = ListUtility.to_string(columns)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_TABLE, table)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_COLUMNS, columns)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_WHERE, where)

        return self

    def query(self, query=None, split_by=None, boundary_query=None, **attr):
        """
        Configures free-form import query

        :param query: sql query which will be used to import data from database.
            Can contain placeholders : a variable consists of a leading "$" character followed by variable name.
            Query placeholders will be replaced with specific values provided as function arguments
        :param split_by: column of the table used to split work.
            If split column is not specified, Sqoop will try to identify the primary key column,
            if any, of the source table.
        :param boundary_query: boundary query to use for creating splits
        :param attr: properties which can be used to substitute placeholders in query.

        :type query: str
        :type split_by: str
        :type target_dir: str
        :rtype: SqoopImport

        """
        query = Template(query).safe_substitute(attr)
        if query[0] == "'" or query[0] == "\"":
            query = query[1:query.__len__() - 1]
        if "\$CONDITIONS" not in query:
            query = query.replace("$CONDITIONS", "\$CONDITIONS")
        if "\$CONDITIONS" not in query:
            if "where" in query.lower():
                query = "{0} AND \$CONDITIONS".format(query)
            else:
                query = "{0} WHERE \$CONDITIONS".format(query)

        query = Sqoop.__double_quotes_wrapper__(query)
        boundary_query = Sqoop.__quotes_wrapper__(boundary_query)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_QUERY, query)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_SPLIT_BY, split_by)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_BOUNDARY_QUERY, boundary_query)

        return self

    def to_hdfs(self, target_dir=None):
        """
        Specifies the directory on HDFS into which the data should be imported.
        Exception will be raised in case the destination directory is already exists in HDFS,

        :param target_dir: destination directory
        :type target_dir: str
        :rtype: SqoopImport

        """

        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_TARGET_DIR, target_dir)

        return self

    def with_incremental(self, incremental=None, check_column=None, last_value=None):
        """
        Configures incremental import.

        :param incremental: incremental import mode.
            Legal values for mode include 'append' and 'lastmodified'.
            Use 'append' mode for numerical data that is incrementing over time, such as auto-increment keys,
            "lastmodified" works on time-stamped data
        :param check_column: Specifies the column to be examined when determining which rows to import.
            The column should not be of type CHAR/NCHAR/VARCHAR/VARNCHAR/ LONGVARCHAR/LONGNVARCHAR
        :param last_value: the maximum value of the check column from the previous import.

        :type incremental: str
        :type check_column: str
        :type last_value: str
        :rtype: SqoopImport

        """

        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_INCREMENTAL, incremental)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_CHECK_COLUMN, check_column)
        last_value = Sqoop.__quotes_wrapper__(last_value)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_LAST_VALUE, last_value)

        return self

    def with_compress(self, compression_codec=None):
        """
        Enables compression
        By default, data is not compressed.
        :param compression_codec: compression codec that can be used for data compression/decompression.
        :type compression_codec: str
        :rtype: SqoopImport

        """

        self.__set_marker_enabled__(TaskOptions.CONFIG_KEY_SQOOP_COMPRESS, True)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_COMPRESSION_CODEC, compression_codec)

        return self

    def with_encoding(self, null_string=None, null_non_string=None):
        """
        Configures NULL values encoding

        :param null_string: The string to be written for a null value for string columns
            If not specified, then the string "null" will be used.
        :param null_non_string: The string to be written for a null value for non-string columns
            If not specified, then the string "null" will be used.
        :type null_string: str
        :type null_non_string: str
        :rtype: SqoopImport

        """
        null_string = Sqoop.__quotes_wrapper__(null_string)
        null_non_string = Sqoop.__quotes_wrapper__(null_non_string)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_NULL_STRING, null_string)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_NULL_NON_STRING, null_non_string)

        return self

    def to_hbase(self, hbase_table=None, hbase_create_table=False, hbase_row_key=None,
                 column_family=None):
        """
        Imports data into HBase

        :param hbase_table: HBase table to use as the target
        :param hbase_create_table: If specified, create missing HBase tables
        :param hbase_row_key: Specifies which input column to use as the row key
            In case, if input table contains composite  key, then <col> must be in the form of a
            comma-separated list of composite key attributes
        :param column_family: the target column family for the import

        :type hbase_table: str
        :type hbase_create_table: bool
        :type hbase_row_key: str
        :type column_family: str
        :rtype: SqoopImport

        """
        self.__set_attr__('hbase_import', 'hbase_import')
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_HBASE_TABLE, hbase_table)
        self.__set_marker_enabled__(TaskOptions.CONFIG_KEY_SQOOP_HBASE_CREATE_TABLE, hbase_create_table)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_HBASE_ROW_KEY, hbase_row_key)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_COLUMN_FAMILY, column_family)

        return self

    def to_hive(self, hive_overwrite=False, create_hive_table=None, hive_table=None,
                hive_drop_import_delims=None, hive_delims_replacement=None,
                hive_partition_key=None, hive_partition_value=None, map_column_hive=None):
        """
        Imports data into Hive

        :param hive_overwrite: overwrite existing data in the Hive table.
        :param create_hive_table: create Hive table.
            If set, then the job will fail if the target hive table exits.
            By default this property is false.
        :param hive_table: destination table name
        :param hive_drop_import_delims: Drops \n, \r, and \01 from string fields when importing to Hive.
        :param hive_delims_replacement: Replace \n, \r, and \01 from string fields with user defined string
            when importing to Hive.
        :param hive_partition_key: name of the hive partition
        :param hive_partition_value: partition value
        :param map_column_hive: SQL type to Hive type mapping.
            Mapping format should be the next "id=STRING,price=DECIMAL"

        :type hive_overwrite: bool
        :type create_hive_table: bool
        :type hive_table: str
        :type hive_drop_import_delims: bool
        :type hive_delims_replacement: str
        :type hive_partition_key: str
        :type hive_partition_value: str
        :type map_column_hive: str

        :rtype: SqoopImport

        """
        hive_partition_value = Sqoop.__quotes_wrapper__(hive_partition_value)
        hive_delims_replacement = Sqoop.__quotes_wrapper__(hive_delims_replacement)
        self.__set_marker_enabled__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_IMPORT, True)
        self.__set_marker_enabled__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_OVERWRITE, hive_overwrite)
        self.__set_marker_enabled__(TaskOptions.CONFIG_KEY_SQOOP_CREATE_HIVE_TABLE, create_hive_table)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_TABLE, hive_table)
        self.__set_marker_enabled__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_DROP_IMPORT_DELIMS, hive_drop_import_delims)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_DELIMS_REPLACEMENT, hive_delims_replacement)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_PARTITION_KEY, hive_partition_key)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_HIVE_PARTITION_VALUE, hive_partition_value)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_MAP_COLUMN_HIVE, map_column_hive)

        return self

    def use_file_format(self, file_format=None):
        """
        Specifies file format
        :type file_format: str
        :rtype: SqoopImport

        """
        if file_format not in ["--as-avrodatafile", "--as-sequencefile", "--as-textfile"]:
            Sqoop.LOG.warning("You use your custom format {0}. You must be sure that "
                              "sqoop support this file format. "
                              "Framework knows next formats: "
                              "'--as-avrodatafile', '--as-sequencefile' or '--as-textfile'"
                              .format(file_format))
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_AS, file_format)

        return self

    def as_textfile(self):
        """
        Imports data as plain text (default)
        :rtype: SqoopImport
        """
        self.use_file_format('--as-textfile')

        return self

    def as_sequencefile(self):
        """
        Imports data to SequenceFiles
        :rtype: SqoopImport
        """
        self.use_file_format('--as-sequencefile')

        return self

    def as_avrofile(self):
        """
        Imports data to Avro Data Files
        :rtype: SqoopImport
        """
        self.use_file_format('--as-avrodatafile')

        return self


class SqoopExport(Sqoop):
    """
    Sqoop export command

    Tool to export a set of files from HDFS back to an RDBMS.
    The target table must already exist in the database.
    The input files are read and parsed into a set of records according to the user-specified delimiters.

    Sqoop export job configuration can be loaded from external Ini file.
    Job name is used as a name of the configuration section containing job specific options.
    Allowed configuration keys are listed in hadoopframework.tools.sqoop.TaskOptions

    Alternatively, you can also use provided API to configure and run export job.

    """

    def __init__(self, name=None, config=None, executor=execute_shell_command):
        """

        :param name: Job name is used as a name of the configuration section containing job specific options.
        :param config: Job configuration
        :param executor: The interface used by the client to launch Sqoop export job.
        """
        self.name = name if name else "SQOOP_TASK_{0}".format(uuid.uuid4())
        self.__executor = executor
        self.specific_attributes = {}
        self.command = "sqoop.export"
        self._config = config if config else Configuration.create(
            readonly=False,
            accepts_nulls=True
        )
        self._process = None

    @staticmethod
    def load_preconfigured_job(name=None, config=None, executor=execute_shell_command):
        """
        Creates instance of SqoopExport. Configure it with options
        :param config: sqoop job configurations
        :param name: sqoop job identifier.
             Will be used as a name of the section with job-specific configurations.
        :param executor:
        """
        if name:
            Sqoop.LOG.info("Load Sqoop Export attributes from section's in configuration "
                           "[sqoop], [sqoop.export] and [{0}]".format(name))
        else:
            Sqoop.LOG.info("Load Sqoop Export attributes from section's in configuration "
                           "[sqoop] and [sqoop.export]")
        return SqoopExport(name, config, executor)

    def __config_export__(self):
        """
        Configuration method for export from HDFS
        :rtype: list

        """
        list_command = []

        list_command.extend(self.__require_attr__(TaskOptions.CONFIG_KEY_SQOOP_EXPORT_DIR))

        if not self.has_option(TaskOptions.CONFIG_KEY_SQOOP_CALL):
            list_command.extend(self.__require_attr__(TaskOptions.CONFIG_KEY_SQOOP_TABLE))
            list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_COLUMNS))
        elif not self.has_option(TaskOptions.CONFIG_KEY_SQOOP_TABLE):
            list_command.extend(self.__require_attr__(TaskOptions.CONFIG_KEY_SQOOP_CALL))
        else:
            raise SqoopCommandError("You must specify table or call.\
                            You can't use table and call together")

        list_command.extend(self.__config_marker__(TaskOptions.CONFIG_KEY_SQOOP_BATCH))
        list_command.extend(self.__config_staging_table__())
        list_command.extend(self.__config_update())
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_NULL_STRING))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_NULL_NON_STRING))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_NUM_MAPPERS))

        return list_command

    def __config_staging_table__(self):
        """
        Configuration method for staging table
        :rtype: list

        """

        list_command = []
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_STAGING_TABLE))
        list_command.extend(self.__config_marker__(TaskOptions.CONFIG_KEY_SQOOP_CLEAR_STAGING_TABLE))
        return list_command

    def __config_update(self):
        """
        Configuration method for export update
        :rtype: list

        """

        list_command = []
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_UPDATE_KEY))
        list_command.extend(self.__optional_attr__(TaskOptions.CONFIG_KEY_SQOOP_UPDATE_MODE))
        return list_command

    def run(self):
        """
        Launches Sqoop Export job
        :rtype:

        """
        Sqoop.LOG.info("Running Sqoop Export Job")
        self._process = self.__executor('sqoop-export', self.build())
        self._process.if_failed_raise(SqoopCommandError("Sqoop Job failed"))
        return self._process

    def from_hdfs(self, export_dir=None):
        """
        Specifies export directory
        :param export_dir: HDFS source path to be exported to RDBMS
        :type export_dir: str
        :type: str
        :rtype: SqoopExport

        """
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_EXPORT_DIR, export_dir)

        return self

    def to_rdbms(self, rdbms=None, host=None, database=None, username=None, password_file=None):
        """
        Configures JDBC connection to target database.

        :param rdbms: the name of the datasource driver which will be used to access to the database.
            E.g.: mysql, microsoft:sqlserver, oracle:thin, etc.
            Parameter is required in case host param doesn't contain complete jdbc connection string
        :param host: host name and port number of the computer hosting your database.
            Jdbc connection string can be passed as host param.
        :param database: the name of the database or service to connect to.
        :param username: the database user on whose behalf the connection is being made
        :param password_file: path for a file containing the authentication password

        :type rdbms: str
        :type host: str
        :type database: str
        :type username: str
        :type password_file: str
        :rtype: SqoopExport

        """

        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_RDBMS, rdbms)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_HOST, host)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_DATABASE, database)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_USERNAME, username)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_PASSWORD_FILE, password_file)

        return self

    def with_batch(self):
        """
        Sets sqoop-export to use batch mode for underlying statement execution.
        By default, Sqoop uses a separate insert statement for each row.
        Batch mode can be used to batch multiple insert statements together.

        Alternatively you can override value of the sqoop.export.records.per.statement
        to specify multiple rows inside one single insert statement.
        :rtype: SqoopExport

        """

        self.__set_marker_enabled__(TaskOptions.CONFIG_KEY_SQOOP_BATCH, True)

        return self

    def with_staging_table(self, staging_table=None, clear_staging_table=False):
        """
        Specifies staging table's attributes.
        Sqoop will load all data to a temporary(staging) table before making changes to the real table.
        Sqoop requires that the structure of the staging table be the same as that of the target table.
        The number of columns and their types must be the same; otherwise, the export operation will fail

        Such approach can be used to guarantee all-or-nothing semantics for the export operation:
        Sqoop opens a new transaction to move data from the staging table to the final destination,
        if and only if all parallel tasks successfully transfer data.

        :param staging_table: staging table name
        :param clear_staging_table: If True, indicates that any data present in the staging table can be deleted.
        :type staging_table: str
        :type clear_staging_table: bool
        :rtype: SqoopExport

        """

        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_STAGING_TABLE, staging_table)
        self.__set_marker_enabled__(TaskOptions.CONFIG_KEY_SQOOP_CLEAR_STAGING_TABLE, clear_staging_table)

        return self

    def with_update(self, update_key=None, update_mode=None):
        """
        Configures sqoop-export command to conditionally insert a new row or update an existing one.

        N.B.! : This feature is not available on all database systems nor supported by all Sqoop connectors.
        Currently it's available only for Oracle and nondirect MySQL exports.

        :param update_key: Anchor column to use for updates.
            Use a comma separated list of columns if there are more than one column.
        :param update_mode: Specify how updates are performed when
            new rows are found with non-matching keys in database.
            Legal values for mode include updateonly (default) and allowinsert.
        :type update_key: str
        :type update_mode: str
        :rtype: SqoopExport

        """

        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_UPDATE_KEY, update_key)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_UPDATE_MODE, update_mode)

        return self

    def table(self, table=None, columns=None):
        """
        Configures sqoop-export destination table.

        In case subset of columns is exported, Sqoop assumes that HDFS data contains the same number
        and ordering of columns as the destination table.

        :param table: table to populate
        :param columns: comma-separated list of column names which should be exported to destination database
        :type table: str
        :type columns: list, str
        :rtype: SqoopExport

        """
        if isinstance(columns, list):
            columns = ListUtility.to_string(columns)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_TABLE, table)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_COLUMNS, columns)

        return self

    def with_encoding(self, input_null_string=None, input_null_non_string=None):
        """
        Overrides the default string constant used for encoding missing values in the database.

        :param input_null_string: constant used for encoding missing values for text-based columns
        :param input_null_non_string: constant used for encoding missing values for not text-based columns
        :type input_null_string: str
        :type input_null_non_string: str
        :rtype: SqoopExport

        """
        input_null_string = Sqoop.__quotes_wrapper__(input_null_string)
        input_null_non_string = Sqoop.__quotes_wrapper__(input_null_non_string)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_NULL_STRING, input_null_string)
        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_INPUT_NULL_NON_STRING, input_null_non_string)

        return self

    def call(self, stored_procedure=None):
        """
        Configures sqoop-export job to use store procedure to populate target table.

        :param stored_procedure: Stored Procedure to call
        :type stored_procedure: str
        :rtype: SqoopExport

        """

        self.__set_attr__(TaskOptions.CONFIG_KEY_SQOOP_CALL, stored_procedure)

        return self


class TaskOptions(Sqoop):
    """
    Option names used to configure Sqoop job
    """
    #specify rdbms
    CONFIG_KEY_SQOOP_RDBMS = "rdbms"
    #specify host where rdbms exist
    CONFIG_KEY_SQOOP_HOST = "host"
    #specify database
    CONFIG_KEY_SQOOP_DATABASE = "database"
    #specify connection manager class name
    CONFIG_KEY_SQOOP_CONNECTION_MANAGER = "connection_manager"
    #manually specify JDBC driver class to use
    CONFIG_KEY_SQOOP_DRIVER = "driver"
    #set authentication password file path
    CONFIG_KEY_SQOOP_PASSWORD_FILE = "password_file"
    #set authentication username
    CONFIG_KEY_SQOOP_USERNAME = "username"
    #use value for given property
    CONFIG_KEY_SQOOP_HADOOP_PROPERTIES = "hadoop_properties"
    #specify comma separated jar files to include in the classpath.
    CONFIG_KEY_SQOOP_LIBJARS = "libjars"
    #columns to import from table
    CONFIG_KEY_SQOOP_COLUMNS = "columns"
    #table to read
    CONFIG_KEY_SQOOP_TABLE = "table"
    #Use 'n' map tasks to import in parallel
    CONFIG_KEY_SQOOP_NUM_MAPPERS = "num_mappers"

    #output line formatting arguments:
    CONFIG_KEY_SQOOP_ENCLOSED_BY = "enclosed_by"
    CONFIG_KEY_SQOOP_ESCAPED_BY = "escaped_by"
    CONFIG_KEY_SQOOP_FIELDS_TERMINATED_BY = "fields_terminated_by"
    CONFIG_KEY_SQOOP_LINES_TERMINATED_BY = "lines_terminated_by"
    CONFIG_KEY_SQOOP_MYSQL_DELIMITERS = "mysql_delimiters"
    CONFIG_KEY_SQOOP_OPTIONALLY_ENCLOSED_BY = "optionally_enclosed_by"

    #input parsing arguments:
    CONFIG_KEY_SQOOP_INPUT_ENCLOSED_BY = "input_enclosed_by"
    CONFIG_KEY_SQOOP_INPUT_ESCAPED_BY = "input_escaped_by"
    CONFIG_KEY_SQOOP_INPUT_FIELDS_TERMINATED_BY = "input_fields_terminated_by"
    CONFIG_KEY_SQOOP_INPUT_LINES_TERMINATED_BY = "input_lines_terminated_by"
    CONFIG_KEY_SQOOP_INPUT_OPTIONALLY_ENCLOSED_BY = "input_optionally_enclosed_by"

    #use direct import fast path
    CONFIG_KEY_SQOOP_DIRECT = "direct"
    #split the input stream every 'n' bytes when importing in direct mode
    CONFIG_KEY_SQOOP_DIRECT_SPLIT_SIZE = "direct_split_size"
    CONFIG_KEY_SQOOP_DIRECT_MODE_PROPERTIES = "direct_mode_properties"

    #imports data in append mode
    CONFIG_KEY_SQOOP_APPEND = "append"
    #imports data to specific format data file
    CONFIG_KEY_SQOOP_AS = "as"

    #set the maximum size for an inline LOB
    CONFIG_KEY_SQOOP_INLINE_LOB_LIMIT = "inline_lob_limit"

    #WHERE clause to use during import
    CONFIG_KEY_SQOOP_WHERE = "where"

    #import results of SQL 'statement'
    CONFIG_KEY_SQOOP_QUERY = "query"
    #HDFS plain table destination
    CONFIG_KEY_SQOOP_TARGET_DIR = "target_dir"
    #column of the table used to split work units
    CONFIG_KEY_SQOOP_SPLIT_BY = "split_by"
    #set boundary query for retrieving max and min value of the primary key
    CONFIG_KEY_SQOOP_BOUNDARY_QUERY = "boundary_query"

    #enable compression
    CONFIG_KEY_SQOOP_COMPRESS = "compress"
    #compression codec to use for import
    CONFIG_KEY_SQOOP_COMPRESSION_CODEC = "compression_codec"

    #null string representation
    CONFIG_KEY_SQOOP_NULL_STRING = "null_string"
    #null non-string representation
    CONFIG_KEY_SQOOP_NULL_NON_STRING = "null_non_string"

    #override mapping for specific columns to java types
    CONFIG_KEY_SQOOP_MAP_COLUMN_JAVA = "map_column_java"

    #incremental import arguments
    CONFIG_KEY_SQOOP_INCREMENTAL = "incremental"
    CONFIG_KEY_SQOOP_CHECK_COLUMN = "check_column"
    CONFIG_KEY_SQOOP_LAST_VALUE = "last_value"

    #Hive arguments:
    CONFIG_KEY_SQOOP_HIVE_IMPORT = "hive_import"
    CONFIG_KEY_SQOOP_CREATE_HIVE_TABLE = "create_hive_table"
    CONFIG_KEY_SQOOP_HIVE_TABLE = "hive_table"
    CONFIG_KEY_SQOOP_MAP_COLUMN_HIVE = "map_column_hive"
    CONFIG_KEY_SQOOP_HIVE_OVERWRITE = "hive_overwrite"
    CONFIG_KEY_SQOOP_HIVE_PARTITION_KEY = "hive_partition_key"
    CONFIG_KEY_SQOOP_HIVE_PARTITION_VALUE = "hive_partition_value"
    CONFIG_KEY_SQOOP_HIVE_DROP_IMPORT_DELIMS = "hive_drop_import_delims"
    CONFIG_KEY_SQOOP_HIVE_DELIMS_REPLACEMENT = "hive_delims_replacement"

    #HBase arguments
    CONFIG_KEY_SQOOP_HBASE_TABLE = "hbase_table"
    CONFIG_KEY_SQOOP_COLUMN_FAMILY = "column_family"
    CONFIG_KEY_SQOOP_HBASE_ROW_KEY = "hbase_row_key"
    CONFIG_KEY_SQOOP_HBASE_CREATE_TABLE = "hbase_create_table"

    #HDFS source path for the export
    CONFIG_KEY_SQOOP_EXPORT_DIR = "export_dir"

    #indicates underlying statements to be executed in batch mode
    CONFIG_KEY_SQOOP_BATCH = "batch"

    #intermediate staging table
    CONFIG_KEY_SQOOP_STAGING_TABLE = "staging_table"
    #indicates that any data in staging table can be deleted
    CONFIG_KEY_SQOOP_CLEAR_STAGING_TABLE = "clear_staging_table"

    #update records by specified key column
    CONFIG_KEY_SQOOP_UPDATE_KEY = "update_key"
    #specifies how updates are performed
    # when new rows are found with non-matching keys in database
    CONFIG_KEY_SQOOP_UPDATE_MODE = "update_mode"

    #populate the table using this stored procedure (one call per row)
    CONFIG_KEY_SQOOP_CALL = "call"

    #input null string representation
    CONFIG_KEY_SQOOP_INPUT_NULL_STRING = "input_null_string"
    #input null non-string representation
    CONFIG_KEY_SQOOP_INPUT_NULL_NON_STRING = "input_null_non_string"
