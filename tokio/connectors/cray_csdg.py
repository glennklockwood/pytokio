"""Processes the output of Cray ClusterStor's Data Gather (CSDG) tool
"""

import re
import json
import datetime
from .. import common
from tokio.connectors.nersc_isdct import _normalize_key

HEADER_LINES = {
    "cache_info": {
        "start": r"^Vendor \(Seagate\) cache information",
        "end": r"^\S",
    },
    "factory_info": {
        "start": r"^Vendor \(Seagate/Hitachi\) factory information",
        "end": r"^\S",
    },
    "errors": {
        "start": r"^Error counter log:",
        "end": r"^[A-Z]",
    }
}
"""HEADER_LINES' keys are regexes that match to indicate the start of a
subsection and values are regexes to indicate the end of that section
"""

REMAP_KEYS = {
    "user_capacity": {
        "k": lambda x: x + "_bytes",
        "v": lambda x: int(x.split()[0].replace(',', '')),
    },
    "logical_block_size": {
        "k": lambda x: x + "_bytes",
        "v": lambda x: int(x.split()[0]),
    },
    "local_time_is": {
        "k": lambda x: "timestamp",
        "v": lambda x: datetime.datetime.strptime(x, "%a %b %d %H:%M:%S %Y PST"),
    },
    "accumulated_startstop_cycles": {
        "k": lambda x: "power_cycles",
    },
    "accumulated_loadunload_cycles": {
        "k": lambda x: "loadunload_cycles",
    },
    "logical_unit_id": {
        "v": lambda x: x, # otherwise default will convert from hex to int
    },
    "current_drive_temperature": {
        "k": lambda x: x + "_c",
        "v": lambda x: int(x.split()[0])
    },
    "drive_trip_temperature": {
        "k": lambda x: x + "_c",
        "v": lambda x: int(x.split()[0])
    },
    "long_(extended)_self_test_duration": {
        "k": lambda x: "extended_self_test_duration_secs",
        "v": lambda x: int(x.split()[0]),
    }
}

for HEADER in HEADER_LINES:
    HEADER_LINES[HEADER]['start_rex'] = re.compile(HEADER_LINES[HEADER]['start'])
    HEADER_LINES[HEADER]['end_rex'] = re.compile(HEADER_LINES[HEADER]['end'])

class Smartctl(dict):
    """Loads the output of the smartctl_a component of CSDG

    Each CSDG dump contains a directory called ``smart_a`` which contains a text
    file for each storage server in the ClusterStor system.  Each such text file
    contains a vaguely CSV-like output that encodes the SMART data of every disk
    that is visible that Lustre server.  The data for a single disk on a single
    server looks like::

        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Vendor:               SEAGATE 
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Product:              ST4000NM0034    
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Revision:             E0G5
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,User Capacity:        4,000,787,030,016 bytes [4.00 TB]
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Logical block size:   512 bytes
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Logical Unit id:      0x5000c50083a771ff
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Serial number:        Z4F05WQP0000R541K7F2
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Device type:          disk
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Transport protocol:   SAS
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Local Time is:        Thu Feb  7 12:27:03 2019 PST
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Device supports SMART and is Enabled
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Temperature Warning Enabled
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,SMART Health Status: OK
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Current Drive Temperature:     31 C
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Drive Trip Temperature:        60 C
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Manufactured in week 20 of year 2015
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Specified cycle count over device lifetime:  10000
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Accumulated start-stop cycles:  53
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Specified load-unload count over device lifetime:  300000
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Accumulated load-unload cycles:  1423
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Elements in grown defect list: 0
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Vendor (Seagate) cache information
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,  Blocks sent to initiator = 831498112
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,  Blocks received from initiator = 2097970568
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,  Blocks read from cache and sent to initiator = 1339922072
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,  Number of read and write commands whose size <= segment size = 196565994
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,  Number of read and write commands whose size > segment size = 6671
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Vendor (Seagate/Hitachi) factory information
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,  number of hours powered up = 30665.57
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,  number of minutes until next internal SMART test = 0
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Error counter log:
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,           Errors Corrected by           Total   Correction     Gigabytes    Total
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,               ECC          rereads/    errors   algorithm      processed    uncorrected
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,           fast | delayed   rewrites  corrected  invocations   [10^9 bytes]  errors
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,read:   149632647        0         0  149632647          0      78600.784           0
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,write:         0        0         0         0          0      18962.364           0
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Non-medium error count:        3
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,[GLTSD (Global Logging Target Save Disable) set. Enable Save with '-S on']
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,No self-tests have been logged
        snx11168n127: ,slot_0 ,/dev/sdej ,SN_Z4F05WQP0000R0000000 ,Long (extended) Self Test duration: 26755 seconds [445.9 minutes]

    It is important to note that since ClusterStor uses twin-tailed disks for
    HA, every physical drive should appear in two adjacent Lustre servers' text
    files.
    """

    def __init__(self, *args, **kwargs):
        """
        """
        super(Smartctl, self).__init__(*args, **kwargs)
        self.parse_state = {}
        self.raw_values = {}
        # Not yet implemented
        pass

    @classmethod
    def from_file(cls, filename):
        new_obj = cls()
        new_obj.load_file(filename)
        return new_obj

    def load_file(self, filename):
        with open(filename, 'r') as in_f:
            for line in in_f:
                self.load_line(line)

        # convert raw_values into self
        for nodename in self.raw_values:
            for slot in self.raw_values[nodename]:
                for dev in self.raw_values[nodename][slot]:
                    for serial_no in self.raw_values[nodename][slot][dev]:
                        if serial_no not in self:
                            self[serial_no] = {
                                "slot": set([]),
                                "dev": set([]),
                            }
                        self[serial_no]['slot'].add(slot)
                        self[serial_no]['dev'].add(dev)
                        for key, val in self.raw_values[nodename][slot][dev][serial_no].items():
                            new_key, new_val = self._scrub_kv(key, val)
                            self[serial_no][new_key] = new_val

                        # self.raw_values[nodename][slot][dev][serial_no]

    def to_json(self, **kwargs):
        return json.dumps(self, cls=common.JSONEncoder, **kwargs)

    def _scrub_kv(self, key, val):
        """Returns a semantically relevant representation of a raw key, value

        Args:
            key (str): A raw key returned from _normalize_keys
            val: A value corresponding to key

        Returns:
            tuple: A new (key, value) where key may be a "better" name for the
            inputted key and value is an appropriately cast representation of
            the inputted value.
        """
        def recast_number(number):
            new_value = number
            for cast in (int, float, lambda x: int(x, 16)):
                try:
                    new_value = cast(number)
                    break
                except ValueError:
                    pass
            return new_value

        cfg = REMAP_KEYS.get(key, {})
        remap_key = cfg.get('k', lambda x: x)
        remap_val = cfg.get('v', recast_number)
        return remap_key(key), remap_val(val)

    def load_line(self, line):
        """Parses a single line of a node's SMART dump
        
        Breaks a single line from a node's SMART dump into all of its relevant
        consistuent components, updates the state of the parser for the
        identified device, and inserts the appropriate keys into self.

        Args:
            line (str): A single line from a single node's SMART dump file
        """
        csv_cols = line.split(',', 4)
        nodename = csv_cols[0].strip().rstrip(':')
        slot = int(csv_cols[1].split('_', 1)[-1])
        dev = csv_cols[2].strip()
        serial_no = csv_cols[3].strip()

        state_key = ''.join(csv_cols[:-1])
        smart_line = csv_cols[-1]

        # are we in a section?  if so, is this the indicator that the section has ended?
        parse_state = self.parse_state.get(state_key)
        found = None
        if parse_state:
            if HEADER_LINES[parse_state]['end_rex'].match(smart_line):
                self.parse_state[state_key] = None
            else:
                found = self._parse_section_line(self.parse_state[state_key], smart_line)

        # are we entering a new section?
        if found is None:
            for header_name, header_cfg in HEADER_LINES.items():
                header_rex = header_cfg['start_rex']
                if header_rex.match(smart_line):
                    found = {}
                    self.parse_state[state_key] = header_name

        # found is None indicates that the line has not yet been identified
        if found is None:
            found = self._parse_toplevel_line(smart_line)

        if found:
            if nodename not in self.raw_values:
                self.raw_values[nodename] = {}
            if slot not in self.raw_values[nodename]:
                self.raw_values[nodename][slot] = {}
            if dev not in self.raw_values[nodename][slot]:
                self.raw_values[nodename][slot][dev] = {}
            if serial_no not in self.raw_values[nodename][slot][dev]:
                self.raw_values[nodename][slot][dev][serial_no] = {}

        if found:
            self.raw_values[nodename][slot][dev][serial_no].update(found)

    def _parse_section_line(self, section, line):
        found = {}

        # parse stuff
        if section == 'cache_info' or section == 'factory_info':
            keyvalue = line.rsplit('=', 1)
            if len(keyvalue) > 1:
                key = _normalize_key(keyvalue[0])
                value = keyvalue[-1].strip()
                found[key] = value
        elif section == 'errors': 
            # The table in this section is not even human-readable, so we just
            # hard-code the columns here.  For reference, they are
            #
            #  1. Errors corrected by ECC
            #  2. Errors corrected by rereads/rewrites
            #  3. Total errors corrected
            #  4. Correction algorithm invocations
            #  5. Gigabytes processed
            #  6. Total uncorrected errors
            #
            #Errors Corrected by           Total   Correction     Gigabytes    Total
            #    ECC          rereads/    errors   algorithm      processed    uncorrected
            #fast | delayed   rewrites  corrected  invocations   [10^9 bytes]  errors
            #read:   149632647        0         0  149632647          0      78600.784           0
            #write:         0        0         0         0          0      18962.364           0
            if line[0] == 'r' or line[0] == 'w':
                values = line.split()
                mode = values[0].rstrip(':')
                found['errors_corrected_by_ecc_%s' % mode] = int(values[1])
                found['errors_corrected_by_retry_%s' % mode] = int(values[2])
                found['total_errors_corrected_%s' % mode] = int(values[3])
                found['correction_algorithm_invocations_%s' % mode] = int(values[5])
                found['gigabytes_%s_base10' % mode] = float(values[6])
                found['total_uncorrected_errors_%s' % mode] = int(values[7])

        # TODO: dict comprehension to prepend section
        return found

    def _parse_toplevel_line(self, line):
        found = {}
        keyvalue = line.split(':', 1)
        if len(keyvalue) > 1:
            found[_normalize_key(keyvalue[0])] = keyvalue[-1].strip()
        return found
