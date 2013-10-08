#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import base64

class ImageTest(test_SoapServer.SoapServerTest):

    def test_upload(self):
        imageFile = open('/home/stephen/Pictures/test.png','rb')
        imageData = imageFile.read()
        encodedData = base64.b64encode(imageData)

        add_result = self.client.service.add_image("test1.png", encodedData)

        assert add_result is True, "Image was not added correctly!"
        
        img = self.client.service.get_image("test1.png")
        
        assert img is not None, "Image was not saved or retrieved correctly!"

        result = self.client.service.remove_image("test1.png")
        
        assert result is True, "Image was not deletd correctly!"

if __name__ == '__main__':
    test_SoapServer.run()
