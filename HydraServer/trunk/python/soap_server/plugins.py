
from spyne.service import ServiceBase
from hydra_complexmodels import Plugin
from spyne.decorator import rpc

class PluginService(ServiceBase):
    """
        Plugin SOAP service
    """

    @rpc(Plugin, _returns=Plugin)
    def get_plugins(ctx):
        """
            Get all available plugins
        """

    @rpc(Plugin, _returns=Plugin)
    def run_plugin(ctx, plugin):
        """
            Run a plugin
        """

    @rpc(Plugin, _returns=Plugin)
    def register_plugin(ctx, plugin):
        """
            Add a plugin to the list of available plugins.
        """
