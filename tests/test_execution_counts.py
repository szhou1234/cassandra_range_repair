#! /usr/bin/env python

from __future__ import print_function
import os, sys, unittest, pkg_resources, mock, logging, subprocess
sys.path.insert(0, '..')
sys.path.insert(0, '.')
    
class execution_count_tests(unittest.TestCase):
    def test_ten_commands(self):
        thisdir = os.path.dirname(__file__)
        cmd = [os.path.join(thisdir, '..', 'range_repair.py'), '--nodetool', os.path.join(thisdir, 'mock_nodetool_script'), '-s', '4', '-w', '2']
        logging.debug(str(cmd))
        subprocess.check_output(cmd)
        results = open('logfile.count').readlines()
        # So 40 is the magic number because:
        # 10 tokens * 4 steps
        self.assertEquals(len(results), 40)
        return

    test_ten_commands.slow=1
