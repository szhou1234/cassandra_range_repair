#! /usr/bin/env python
import sys
import os

sys.path.insert(0, '..')
sys.path.insert(0, '.')

sys.path.insert(0,os.path.abspath(__file__+"/../../src"))

import range_repair


if __name__ == '__main__':
    suite1 = unittest.TestLoader().loadTestsFromTestCase(execution_count_tests)
    unittest.TextTestRunner(verbosity=2).run(suite1)
    suite2 = unittest.TestLoader().loadTestsFromTestCase(range_tests)
    unittest.TextTestRunner(verbosity=2).run(suite2)
