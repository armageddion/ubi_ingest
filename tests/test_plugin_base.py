from plugins.base import BasePlugin, get_plugins_for_customer, register


class ExamplePlugin(BasePlugin):
    @staticmethod
    def applies_to(customer):
        return customer.get("name") == "example"

    def transform_articles(self, customer, articles):
        return articles


def test_base_plugin_registry_accepts_classes():
    register(ExamplePlugin)
    plugins = get_plugins_for_customer({"name": "example"})
    assert len(plugins) >= 1
    assert isinstance(plugins[0], ExamplePlugin)
