#!/usr/bin/env python
"""
This script will allow for smaller repairs of Cassandra ranges.

See the tests subdirectory for example code.
"""
from optparse import OptionParser

import logging
import subprocess
import sys
import multiprocessing
import platform

class Token_Container:
    RANGE_MIN = -(2**63)
    RANGE_MAX = (2**63)-1
    FORMAT_TEMPLATE = "{0:+021d}"
    def __init__(self, options):
        '''Initialize the Token Container by getting the host and ring tokens and
        then confirming the values used for formatting and range
        management.
        :param options: OptionParser result
        :returns: None
        '''
        self.options = options
        self.local_nodes = []
        self.host_tokens = []
        self.ring_tokens = []
        self.host_token_count = -1
        self.get_local_nodes()
        self.get_host_tokens()
        self.get_ring_tokens()
        self.check_for_MD5_tokens()
        return

    def get_local_nodes(self):
        '''In a multi-DC environment, it is important to *only* consider tokens on
        members of the local ring.

        '''
        if not self.options.datacenter:
            logging.debug("No datacenter specified, all ring members' tokens will be considered")
            return
        logging.debug("Determining local ring members")
        cmd = [self.options.nodetool, "-h", self.options.host, "gossipinfo"]
        success, _, stdout, stderr = run_command(*cmd)

        if not success:
            raise Exception("Died in get_ring_tokens because: " + stderr)

        # This is a really well-specified value.  If the format of the
        # output of 'nodetool gossipinfo' changes, this will have to be
        # revisited.
        search_value = "\n  DC:{datacenter}\n".format(datacenter=self.options.datacenter)
        for paragraph in stdout.split("/"):
            if not search_value in paragraph: continue
            self.local_nodes.append(paragraph.split()[0])
        logging.info("Local nodes: " + " ".join(self.local_nodes))
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
        success, _, stdout, stderr = run_command(*cmd)

        if not success:
            raise Exception("Died in get_ring_tokens because: " + stderr)

        logging.debug("ring tokens found, creating ring token list...")
        for line in stdout.split("\n")[6:]:
            segments = line.split()
            # Filter tokens from joining nodes
            if (len(segments) != 8) or (segments[3] == "Joining"):
                continue
            # If a datacenter has been specified, filter nodes that are in
            # different datacenters.
            if self.options.datacenter and not segments[0] in self.local_nodes:
                logging.debug("Discarding node/token %s/%s", segments[0], segments[-1])
                continue
            self.ring_tokens.append(long(segments[-1]))
        self.ring_tokens.sort()
        logging.info("Found {0} tokens".format(len(self.ring_tokens)))
        return

    def get_host_tokens(self):
        """Gets the tokens ranges for the target host
        :returns: None
        """
        cmd = [self.options.nodetool, "-h", self.options.host, "info", "-T"]
        success, _, stdout, stderr = run_command(*cmd)
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
        :returns: string-formatted start value, string-formatted end value, current step number
        There is special-case handling for when there are more steps than there
        are keys in the range: just return the start and stop values.
        """
        step = 0
        # This first case works for all but the highest-valued token.
        if stop > start:
            if start+steps+1 < stop:
                step_increment = (stop - start) / steps

                for i in range(start, stop, step_increment):
                    local_end = i + step_increment
                    if local_end > stop:
                        local_end = stop
                    if i == local_end:
                        break
                    step += 1
                    yield self.format(i), self.format(local_end), step
            else:
                step += 1
                yield self.format(start), self.format(stop), step
        else:                     # This is the wrap-around case
            steps -= 1            # Because of the wraparound, the odds are there will be an extra step.
            distance = (self.RANGE_MAX - start) + (stop - self.RANGE_MIN) 
            if distance > steps:
                step_increment = distance / steps
                # Can't use xrange here because the numbers are too large!
                for i in range(start, self.RANGE_MAX, step_increment):
                    local_end = i + step_increment
                    if local_end > self.RANGE_MAX:
                        local_end = self.RANGE_MAX
                    if i == local_end:
                        break
                    step += 1
                    yield self.format(i), self.format(local_end), step
                for i in range(self.RANGE_MIN, stop, step_increment):
                    local_end = i + step_increment
                    if local_end > stop:
                        local_end = stop
                    if i == local_end:
                        break
                    step += 1
                    yield self.format(i), self.format(local_end), step
            else:
                step += 1
                yield self.format(start), self.format(stop), step

def run_command(*command):
    """Execute a shell command and return the output
    :param command: the command to be run and all of the arguments
    :returns: success_boolean, command_string, stdout, stderr
    """
    cmd = " ".join(command)
    logging.debug("run_command: " + cmd)
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    return proc.returncode == 0, cmd, stdout, stderr

def repair_range(options, start, end, step, nodeposition):
    """Repair a keyspace/columnfamily between a given token range with nodetool
    :param options: OptionParser result
    :param start: Beginning token in the range to repair (formatted string)
    :param end: Ending token in the range to repair (formatted string)
    :param step: The step we're executing (for logging purposes)
    :param nodeposition: string to indicate which node this particular step is for.
    :returns: None
    """
    setup_logging(options)
    logging.debug(
        "{nodeposition} step {step:04d} repairing range ({start}, {end}) for keyspace {keyspace}".format(
            step=step,
            start=start,
            end=end,
            nodeposition=nodeposition,
            keyspace=options.keyspace or "<all>"))

    cmd = [options.nodetool, "-h", options.host,
           "repair", options.keyspace]
    cmd.extend(options.columnfamily)
    cmd.extend([options.local, options.snapshot,
                "-pr", "-st", start, "-et", end])

    if not options.dry_run:
        success, cmd, _, stderr = run_command(*cmd)
    else:
        success = True

    if not success:
        logging.error("FAILED: {nodeposition} step {step:04d} {cmd}".format(nodeposition=nodeposition, step=step, cmd=cmd))
        logging.error(stderr)
        return
    logging.debug("{nodeposition} step {step:04d} complete".format(nodeposition=nodeposition,step=step))
    return

def setup_logging(option_group):
    """Sets up logging in a syslog format by log level
    :param option_group: options as returned by the OptionParser
    """
    stderr_log_format = "%(levelname) -10s %(asctime)s %(funcName) -20s line:%(lineno) -5d: %(message)s"
    file_log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logger = logging.getLogger()
    if option_group.debug:
        logger.setLevel(level=logging.DEBUG)
    elif option_group.verbose:
        logger.setLevel(level=logging.INFO)
    elif option_group.verbose:
        logger.setLevel(level=logging.WARNING)
        
    handlers = []
    if option_group.syslog:
        handlers.append(logging.SyslogHandler(facility=option_group.syslog))
        # Use standard format here because timestamp and level will be added by syslogd.
    if option_group.logfile:
        handlers.append(logging.FileHandler(option_group.logfile))
        handlers[0].setFormatter(logging.Formatter(file_log_format))
    if not handlers:
        handlers.append(logging.StreamHandler())
        handlers[0].setFormatter(logging.Formatter(stderr_log_format))
    for handler in handlers:
        logger.addHandler(handler)
    return

def repair(options):
    """Repair a keyspace/columnfamily by breaking each token range into $start_steps ranges
    :param options.keyspace: Cassandra keyspace to repair
    :param options.host: (optional) Hostname to pass to nodetool 
    :param options.steps: Number of sub-ranges to split primary range in to
    :param options.workers: Number of workers to use
    """
    tokens = Token_Container(options)

    worker_pool = multiprocessing.Pool(options.workers)
    
    for token_num, host_token in enumerate(tokens.host_tokens):
        range_termination = tokens.get_range_termination(host_token)
        
        logging.info(
            "[{count}/{total}] repairing range ({token}, {termination}) in {steps} steps for keyspace {keyspace}".format(
                count=token_num + 1,
                total=tokens.host_token_count,
                token=tokens.format(host_token), 
                termination=tokens.format(range_termination), 
                steps=options.steps, 
                keyspace=options.keyspace))

        results = [worker_pool.apply_async(repair_range,
                                           (options,
                                            start,
                                            end,
                                            step,
                                            "{count}/{total}".format(count=token_num + 1,
                                                                     total=tokens.host_token_count)))
                   for start, end, step in tokens.sub_range_generator(host_token, range_termination, options.steps)]
        for r in results:
            r.get()
    return

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
                      metavar="HOST", help="Hostname to repair [default: %default]")

    parser.add_option("-s", "--steps", dest="steps", type="int", default=100,
                      metavar="STEPS", help="Number of discrete ranges [default: %default]")

    parser.add_option("-n", "--nodetool", dest="nodetool", default="nodetool",
                      metavar="NODETOOL", help="Path to nodetool [default: %default]")

    # The module default for workers is actually the CPU count, but we're
    # going to override it to 1, which matches the old behavior of serial
    # repairs.
    parser.add_option("-w", "--workers", dest="workers", type="int", default=1,
                      metavar="WORKERS", help="Number of workers to use for parallelism [default: %default]")

    parser.add_option("-D", "--datacenter", dest="datacenter", default=None,
                      metavar="DATACENTER", help="Identify local datacenter [default: %default]")

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

    parser.add_option("--dry-run", dest="dry_run", action='store_true',
                      default=False, help="Do not execute repairs.")

    parser.add_option("--syslog", dest="syslog", metavar="FACILITY",
                      help="Send log messages to the syslog")

    parser.add_option("--logfile", dest="logfile", metavar="FILENAME",
                      help="Send log messages to a file")

    (options, args) = parser.parse_args()

    if options.columnfamily and not options.keyspace: # keyspace is a *required* for columfamilies
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
