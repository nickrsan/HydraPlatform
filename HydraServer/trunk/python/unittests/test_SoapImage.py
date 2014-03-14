# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
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

    def test_download(self):
        imageFile = open('hydra.jpg','rb')
        imageData = imageFile.read()
        encodedData = base64.b64encode(imageData)

        self.client.service.add_image("hydra.jpg", encodedData)

        img = self.client.service.get_image("hydra.jpg")

        assert base64.b64decode(img) == encodedData, "Image was not retrieved correctly!"

        result = self.client.service.remove_image("hydra.jpg")

        assert result is True, "Image was not deletd correctly!"

class FileTest(test_SoapServer.SoapServerTest):

    def test_upload(self):
        file_to_upload = open('test.xlsx','rb')
        fileData = file_to_upload.read()
        encodedData = base64.b64encode(fileData)

        add_result = self.client.service.add_file("NETWORK", 1, "test.xlsx", encodedData)

        assert add_result is True, "File was not added correctly!"

        img = self.client.service.get_file("NETWORK", 1, "test.xlsx")

        assert img is not None, "File was not saved or retrieved correctly!"

        result = self.client.service.remove_file("NETWORK", 1, "test.xlsx")

        assert result is True, "File was not deletd correctly!"

    def test_download(self):
        file_to_upload = open('test.xlsx','rb')
        fileData = file_to_upload.read()
        encodedData = base64.b64encode(fileData)

        self.client.service.add_file("NETWORK", 1, "test.xlsx", encodedData)

        img = self.client.service.get_file("NETWORK", 1, "test.xlsx")

        assert base64.b64decode(img) == encodedData, "File was not retrieved correctly!"

        result = self.client.service.remove_file("NETWORK", 1, "test.xlsx")

        assert result is True, "File was not deletd correctly!"



if __name__ == '__main__':
    test_SoapServer.run()
