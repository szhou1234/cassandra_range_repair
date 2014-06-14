#!/usr/bin/env python
"""
This script will allow for smaller repairs of Cassandra ranges.

#################################################
# success, ring_tokens, error = get_ring_tokens()
# success, host_token, error = get_host_token()
# range_termination = get_range_termination(host_token, ring_tokens)
# steps = 100

# print repr(get_ring_tokens())
# print repr(get_host_token())
# print repr(get_range_termination(host_token, ring_tokens))
# print repr(sub_range_generator(host_token, range_termination, steps).next())
#################################################
"""
from optparse import OptionParser

import logging
import subprocess
import sys
import multiprocessing
import platform

MURMUR3_MIN = -(2**63)
MURMUR3_MAX = 2**63
MD5_MAX = (2**127) -1

class Token_Container:
    RANGE_MIN = -(2**63)
    RANGE_MAX = 2**63
    FORMAT_TEMPLATE = "{0:020d}"
    def __init__(self, options):
        '''Initialize the Token Container by getting the host and ring tokens and
        then confirming the values used for formatting and range
        management.
        :param options: OptionParser result
        :returns: None
        '''
        self.options = options
        self.host_tokens = []
        self.ring_tokens = []
        self.host_token_count = -1
        self.get_host_tokens()
        self.get_ring_tokens()
        self.check_for_MD5_tokens()
        return
        
    def check_for_MD5_tokens(self):
        """By default, the Token_Container assumes that the Murmur3 partitioner is
        in use.  If that's true, then the first token in the ring should
        have a negative value as long as the cluster has at least 3
        (v)nodes.  If the first token is not negative, switch the class
        constants for the values associated with Random paritioner.
        :returns: None
        """
        if not self.ring_tokens[0] < 0:
            self.FORMAT_TEMPLATE = "{0:039d}"
            self.RANGE_MIN = 0
            self.RANGE_MAX = (2**127) - 1
        return
        
    def get_ring_tokens(self):
        """Gets the token information for the ring
        :returns: None
        """
        logging.info("running nodetool ring, this will take a little bit of time")
        cmd = [self.options.nodetool, "-h", self.options.host, "ring"]
        success, _, stdout, stderr = run_command(" ".join(cmd))

        if not success:
            raise Exception("Died in get_ring_tokens because: " + stderr)

        logging.debug("ring tokens found, creating ring token list...")
        for line in stdout.split("\n")[6:]:
            segments = line.split()
            # Filter tokens from joining nodes
            if (len(segments) == 8) and (segments[3] != "Joining"):
                self.ring_tokens.append(long(segments[-1]))
        self.ring_tokens.sort()
        return

    def get_host_tokens(self):
        """Gets the tokens ranges for the target host
        :returns: None
        """
        cmd = [self.options.nodetool, "-h", self.options.host, "info", "-T"]
        success, _, stdout, stderr = run_command(" ".join(cmd))
        if not success or stdout.find("Token") == -1:
            logging.error(stdout)
            raise Exception("Died in get_host_tokens because: " + stderr)

        logging.debug("host tokens found, creating host token list...")
        for line in stdout.split("\n"):
            if not line.startswith("Token"): continue
            parts = line.split()
            self.host_tokens.append(long(parts[-1]))
        self.host_tokens.sort()
        self.host_token_count = len(self.host_tokens)
        return

    
    def format(self, value):
        '''Return the correctly zero-padded string for the token.
        :returns: the properly-formatted token.
        '''
        return self.FORMAT_TEMPLATE.format(value)
    def get_range_termination(self, token):
        """get the start token for the next range
        :param token: Token to start from
        :returns: The token that falls immediately after the argument token
        """
        for i in self.ring_tokens:
            if token < i:
                return i
        # token is the largest value in the ring.  Since the rings wrap around,
        # return the first value.
        return self.ring_tokens[0]
        
    def sub_range_generator(self, start, stop, steps=100):
        """Generate $step subranges between $start and $stop
        :param start: beginning token in the range
        :param stop: first token of the next range
        :param steps: number of sub-ranges to create

        There is special-case handling for when there are more steps than there
        are keys in the range: just return the start and stop values.
        """
        # This first case works for all but the highest-valued token.
        if stop > start:
            if start+steps+1 < stop:
                step_increment = (stop - start) / steps

                for i in xrange(start, stop, step_increment):
                    local_end = i + step_increment
                    if local_end > stop:
                        local_end = stop
                    if i == local_end:
                        break
                    yield self.format(i), self.format(local_end)
            else:
                yield self.format(start), self.format(stop)
        else:                     # This is the wrap-around case
            distance = (self.RANGE_MAX - start) + (stop - self.RANGE_MIN) 
            if distance > steps:
                step_increment = distance / steps
                for i in xrange(start, self.RANGE_MAX, step_increment):
                    local_end = i + step_increment
                    if local_end > self.RANGE_MAX:
                        local_end = self.RANGE_MAX
                    if i == local_end:
                        break
                    yield self.format(i), self.format(local_end)
                for i in xrange(self.RANGE_MIN, stop, step_increment):
                    local_end = i + step_increment
                    if local_end > stop:
                        local_end = stop
                    if i == local_end:
                        break
                    yield self.format(i), self.format(local_end)
            else:
                yield self.format(start), self.format(stop)

def run_command(command, *args):
    """Execute a shell command and return the output
    """
    cmd = " ".join([command] + list(args))
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    return proc.returncode == 0, cmd, stdout, stderr

def repair_range(options, start, end):
    """Repair a keyspace/columnfamily between a given token range with nodetool
    :param options: OptionParser result
    :param start: Beginning token in the range to repair (formatted string)
    :param end: Ending token in the range to repair (formatted string)
    """
    cmd = [options.nodetool, "-h", options.host,
           "repair", options.keyspace, options.columnfamily,
           options.local, options.snapshot,
           "-pr", "-st", start, "-et", end
    ]
    success, cmd, stdout, stderr = run_command(" ".join(cmd))
    return success, cmd, stdout, stderr

def setup_logging(option_group):
    """Sets up logging in a syslog format by log level
    :param option_group: options as returned by the OptionParser
    """
    log_format = "%(levelname) -10s %(asctime)s %(funcName) -20s line:%(lineno) -5d: %(message)s"
    if option_group.debug:
        logging.basicConfig(level=logging.DEBUG, format=log_format)
    elif option_group.verbose:
        logging.basicConfig(level=logging.INFO, format=log_format)
    else:
        logging.basicConfig(level=logging.WARNING, format=log_format)

def repair(options):
    """Repair a keyspace/columnfamily by breaking each token range into $start_steps ranges
    :param options.keyspace: Cassandra keyspace to repair
    :param options.columnfamily: Cassandra columnfamily to repair
    :param options.host: (optional) Hostname to pass to nodetool 
    :param options.steps: Number of sub-ranges to split primary range in to
    :param options.workers: Number of workers to use
    """
    tokens = Token_Container(options)

    worker_pool = multiprocessing.Pool(options.workers)
    
    for token_num, host_token in enumerate(tokens.host_tokens):
        steps = options.steps
        range_termination = tokens.get_range_termination(host_token)
        
        logging.info(
            "[{count}/{total}] repairing range ({token}, {termination}) in {steps} steps for keyspace {keyspace}".format(
                count=token_num + 1,
                total=tokens.host_token_count,
                token=tokens.format(host_token), 
                termination=tokens.format(range_termination), 
                steps=steps, 
                keyspace=options.keyspace))

        for start, end in tokens.sub_range_generator(host_token, range_termination, steps):

            logging.debug(
                "step {steps:04d} repairing range ({start}, {end}) for keyspace {keyspace}".format(
                    steps=steps,
                    start=start,
                    end=end,
                    keyspace=options.keyspace))

            success, cmd, _, stderr = repair_range(options,
                                                   start=start,
                                                   end=end)

            if not success:
                logging.error("FAILED: {0}".format(cmd))
                logging.error(stderr)
                return False
            logging.debug("step {steps:04d} complete".format(steps=steps))
            steps -= 1

    return True

def main():
    """Validate arguments and initiate repair
    """
    parser = OptionParser()
    parser.add_option("-k", "--keyspace", dest="keyspace", metavar="KEYSPACE",
                      help="Keyspace to repair (REQUIRED)")

    parser.add_option("-c", "--columnfamily", dest="columnfamily", default=[],
                      action="append", metavar="COLUMNFAMILY",
                      help="ColumnFamily to repair, can appear multiple times")

    parser.add_option("-H", "--host", dest="host", default=platform.node(),
                      metavar="HOST", help="Hostname to repair")

    parser.add_option("-s", "--steps", dest="steps", type="int", default=100,
                      metavar="STEPS", help="Number of discrete ranges")

    parser.add_option("-n", "--nodetool", dest="nodetool", default="nodetool",
                      metavar="NODETOOL", help="Path to nodetool")

    parser.add_option("-w", "--workers", dest="workers", type="int", default=1,
                      metavar="WORKERS", help="Number of workers to use for parallelism (DANGEROUS)")

    parser.add_option("-l", "--local", dest="local", default="",
                      action="store_const", const="-local",
                      metavar="LOCAL", help="Restrict repair to the local DC")

    parser.add_option("-S", "--snapshot", dest="snapshot", default="",
                      action="store_const", const="-snapshot",
                      metavar="LOCAL", help="Use snapshots (pre-2.x only)")

    parser.add_option("-v", "--verbose", dest="verbose", action='store_true',
                      default=False, help="Verbose output")

    parser.add_option("-d", "--debug", dest="debug", action='store_true',
                      default=False, help="Debugging output")

    (options, args) = parser.parse_args()

    if not options.keyspace:    # keyspace is a *required* parameter
        parser.print_help()
        sys.exit(1)

    if args:                    # There are no positional parameters
        parser.print_help()
        sys.exit(1)

    setup_logging(options)

    if repair(options):
        sys.exit(0)

    sys.exit(2)

if __name__ == "__main__":
    main()
