import config
if config.CONFIG is None:
    config.load_config()

import hydra_logging
hydra_logging.init()

