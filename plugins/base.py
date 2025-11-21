import logging
from typing import List, Any, Dict

_PLUGINS: List["BasePlugin"] = []


class BasePlugin:
    """Base class for plugins.

    Subclass and implement `applies_to(customer)` and `transform_articles(customer, articles)`.
    """

    def applies_to(self, customer: Dict[str, Any]) -> bool:
        """Return True if this plugin should be applied to the given customer."""
        return False

    def transform_articles(self, customer: Dict[str, Any], articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Return transformed articles. Default implementation returns articles unchanged."""
        return articles


def register(plugin: BasePlugin) -> None:
    """Register a plugin instance."""
    _PLUGINS.append(plugin)
    logging.info(f"Registered plugin: {plugin}")


def get_plugins_for_customer(customer: Dict[str, Any]) -> List[BasePlugin]:
    """Return registered plugins that apply to the given customer."""
    logging.debug(f"Getting applicable plugins for customer: {customer.get('name')}")
    return [p for p in _PLUGINS if p.applies_to(customer)]
