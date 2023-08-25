try:
    from syslogng import Logger

    logger = Logger(__name__)
except ImportError:
    from logging import StreamHandler, getLogger

    logger = getLogger(__name__)
    from pythonjsonlogger import jsonlogger

    logHandler = StreamHandler()
    formatter = jsonlogger.JsonFormatter()
    logHandler.setFormatter(formatter)
    logger.addHandler(logHandler)

try:
    from syslogng import register_config_generator
except ImportError:
    pass

from importlib import resources as impresources

from segateway_source_mimecast import conf


def _plugin_config_generator(args):
    inp_file = impresources.files(conf) / "source.conf"
    logger.error(f"Source {inp_file}")
    with inp_file.open("rt") as f:
        return f.read() + "\n"


def register_plugin_config_generator():
    register_config_generator(
        context="root",
        name="segateway_source_mimecast_source",
        config_generator=_plugin_config_generator,
    )


def main():
    print(_plugin_config_generator({}))


if __name__ == "__main__":
    main()
