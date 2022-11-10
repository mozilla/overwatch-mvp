import cattrs
import toml
import os
from .configs import Config
from analysis.logging import logger


class Loader:
    @classmethod
    def load_config(cls, filename: str) -> Config:
        """
        Loads the Config from the specified toml file.  All configured validation is applied at time
         of loading.
        @param filename:
        @return: Config object representing the configuration in the file.
        """
        toml_config = toml.load(open(filename))
        config = cattrs.structure(toml_config, Config)
        return config

    @classmethod
    def load_all_config_files(cls, directory: str) -> [Config]:
        """
        Loads the Config from all toml files found in the specified directory
        @param directory:
        @return: Returns an array of Config objects, one for each toml file in the specified
         directory.
        """
        configs = []
        for file in os.listdir(directory):
            full_filename = os.path.join(directory, file)
            if os.path.isfile(full_filename):
                logger.info(f"Validating file: {full_filename}")
                config = Loader.load_config(full_filename)
                configs.append(config)
        return configs
