#!/bin/bash
python ../../../../CSVplugin/trunk/ImportCSV.py -t hydro-econ_network.csv -n hydro-econ_nodes_ag.csv hydro-econ_nodes_desal.csv hydro-econ_nodes_gw.csv hydro-econ_nodes_hp.csv hydro-econ_nodes_jn.csv hydro-econ_nodes_sr.csv hydro-econ_nodes_ur.csv hydro-econ_nodes_WWTP.csv -l hydro-econ_links_costLink.csv hydro-econ_links_defLink.csv -x

python assign_templates.py

