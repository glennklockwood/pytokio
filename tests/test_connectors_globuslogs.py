#!/usr/bin/env python

import json
import tokiotest
import tokio.connectors.globuslogs

def test_connector_globuslogs():
    """connectors.globuslogs.GlobusLog
    """
    blah = tokio.connectors.globuslogs.GlobusLog.from_file(tokiotest.SAMPLE_GLOBUSLOGS_TXT)
    print(json.dumps(blah, indent=4, sort_keys=True))
    assert blah
    assert blah[0]

    for logrec in blah:
        print(logrec)
        # catch regressions in time resolution
        assert logrec['DATE'] != logrec['START']
