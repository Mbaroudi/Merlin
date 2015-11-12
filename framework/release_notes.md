[Features]
**********
Class merlin.common.configuration.Configuration is based on metastore.
Added module metastores that contain interface for metastore and
two implementations: based on file and based on WebHCat properties.

[Fixed]
*******


[Compatibility with previous version]
*************************************
In version 0.3 or higher, class merlin.common.configuration.Configuration doesn't support old API

    //config_file is path to configuration file
    Configuration.load(config_file)

Instead of this method please use the next piece of code:

    //config_file is path to configuration file
    metastore = merlin.common.metastores.IniFileMetaStore(file=config_file)
    Configuration.load(metastore)