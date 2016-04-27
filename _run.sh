#!/usr/bin/env bash

#cat /home/mi/tmpdata/metok_trajectory/missing_cells/missing_cells.json|python missingcell_key_parser.py |python opencellid_crawer.py |cat >> missingcell_with_opencellid.json.txt
# cat missing_cells_key.json.txt |python cellocation_crawer.py |cat >> missingcell_position_results.json.txt
cat missing_cells_key.json.txt |python cellocation_crawer.py
