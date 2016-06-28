#! /usr/bin/env python

from __future__ import print_function
import os, sys, unittest, pkg_resources, mock, logging
sys.path.insert(0, '..')
sys.path.insert(0, '.')

sys.path.insert(0,os.path.abspath(__file__+"/../../src"))

import range_repair


class FailingExecutor:
    def __init__(self, nfails):
        self._outcomes = [False] * nfails

    def __call__(self):
        if self._outcomes:
            return self._outcomes.pop()
        else:
            return True


def build_fake_retryer(nfails, maxtries):
    executor = FailingExecutor(nfails)

    sleeps = []
    sleeper = lambda seconds: sleeps.append(seconds)

    config = range_repair.ExponentialBackoffRetryerConfig(maxtries, 1, 2)
    retryer = range_repair.ExponentialBackoffRetryer(config, lambda ok: ok, executor, sleeper)

    return retryer, sleeps


class RetryTests(unittest.TestCase):
    def test_first_execution_success(self):
        retryer, sleeps = build_fake_retryer(0, 5)
        self.assertEquals(retryer(), True)
        self.assertEquals(sleeps, [])

    def test_seconds_execution_success(self):
        retryer, sleeps = build_fake_retryer(1, 5)
        self.assertEquals(retryer(), True)
        self.assertEquals(sleeps, [1])

    def test_third_execution_success(self):
        retryer, sleeps = build_fake_retryer(2, 5)
        self.assertEquals(retryer(), True)
        self.assertEquals(sleeps, [1, 2])

    def test_too_many_retries(self):
        retryer, sleeps = build_fake_retryer(10, 5)
        self.assertEquals(retryer(), False)
        self.assertEquals(sleeps, [1, 2, 4, 8])