#!/usr/bin/env python
"""Wrapper script that sets the necessary environment variables for GAMSimport.
"""

import os
import sys
import subprocess

cmd_args = sys.argv

gams_path = None

for i, arg in enumerate(cmd_args):
    if arg in ['-G', '--gams-path']:
        gams_path = cmd_args[i + 1]

if gams_path:
    gams_path = os.path.abspath(gams_path)

    os.environ['LD_LIBRARY_PATH'] = gams_path

cmd_args[0] = 'GAMSimport.py'

stdout = sys.stdout
stderr = sys.stderr

cmd = subprocess.call(['python'] + cmd_args, stdout=stdout, stderr=stderr)
