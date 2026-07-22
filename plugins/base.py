import logging

_registry = []


def register(plugin_class):
    _registry.append(plugin_class)
    logging.info(f"Registered plugin: {plugin_class.__name__}")


def get_plugins_for_customer(customer):
    return [p() for p in _registry if p.applies_to(customer)]
