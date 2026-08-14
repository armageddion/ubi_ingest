import logging

_registry = []


class BasePlugin:
    """Base class for customer-specific transformations."""

    @staticmethod
    def applies_to(customer):
        raise NotImplementedError

    def transform_articles(self, customer, articles):
        raise NotImplementedError


def register(plugin):
    _registry.append(plugin)
    name = plugin.__name__ if isinstance(plugin, type) else plugin.__class__.__name__
    logging.info(f"Registered plugin: {name}")


def get_plugins_for_customer(customer):
    plugins = []
    for plugin in _registry:
        plugin_obj = plugin() if isinstance(plugin, type) else plugin
        if plugin_obj.applies_to(customer):
            plugins.append(plugin_obj)
    return plugins
