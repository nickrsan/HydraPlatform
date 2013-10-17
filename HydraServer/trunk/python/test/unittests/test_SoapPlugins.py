
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer

class PluginsTest(test_SoapServer.SoapServerTest):

    def test_get_plugins(self):
        plugins = self.client.service.get_plugins()

        assert len(plugins) > 0, "Plugins not retrieved correctly."

if __name__ == '__main__':
    test_SoapServer.run()
