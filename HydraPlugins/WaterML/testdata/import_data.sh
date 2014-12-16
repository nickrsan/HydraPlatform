#!/bin/bash
python ../ImportWML/ImportWML.py -t GetVaulesforSite_Mendon.xml -n 'Mendon Timeseries'

python ../ImportWML/ImportWML.py -t QC\ Data

