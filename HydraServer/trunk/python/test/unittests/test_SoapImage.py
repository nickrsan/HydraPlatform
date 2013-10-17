#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import base64

class ImageTest(test_SoapServer.SoapServerTest):

    def test_upload(self):
        imageFile = open('hydra.jpg','rb')
        imageData = imageFile.read()
        encodedData = base64.b64encode(imageData)

        add_result = self.client.service.add_image("hydra.jpg", encodedData)

        assert add_result is True, "Image was not added correctly!"
        
        img = self.client.service.get_image("hydra.jpg")
        
        assert img is not None, "Image was not saved or retrieved correctly!"

        result = self.client.service.remove_image("hydra.jpg")
        
        assert result is True, "Image was not deletd correctly!"

if __name__ == '__main__':
    test_SoapServer.run()
