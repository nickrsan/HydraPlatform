#!/bin/bash
python -m cProfile -o import_profile ../../ImportCSV.py -t EBSD_network.csv -n WRZ_nodes.csv -l WRZ_links.csv -x -m ebsd_template_v5.xml