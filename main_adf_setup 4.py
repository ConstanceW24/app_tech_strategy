import sys
from sys import argv
from batch_audit import appl_audit_utils
from orchestrator_utils import dataflow_submit

print("****** CYBER E2 TABLE LOADING ADF ORCHESTRATION STARTED ******")
dp_service = argv[1]
if dp_service == "create_batch":
    print(f"{dp_service} Invoked")
    sys.argv = ['./appl_audit_utils.py', '--appl_short_name', argv[2], '--source', argv[3], '--schema', argv[4]]
    appl_audit_utils.insert_batch()
elif dp_service == "stage_load":
    print(f"{dp_service} Invoked")
    sys.argv = ['./dataflow_submit.py', '--appl_short_name', argv[2], '--source', argv[3], '--schema', argv[4], '--table', argv[5], '--layer', argv[6], '--environment', argv[7]]
    dataflow_submit.init_process()
elif dp_service == "refined_load":
    print(f"{dp_service} Invoked")
    sys.argv = ['./dataflow_submit.py', '--appl_short_name', argv[2], '--source', argv[3], '--schema', argv[4], '--table', argv[5], '--environment', argv[6], '--sequence', argv[7]]
    dataflow_submit.init_process()
elif dp_service == "E2E":
    print(f"{dp_service} Invoked")
    sys.argv = ['./dataflow_submit.py', '--appl_short_name', argv[2], '--source', argv[3], '--schema', argv[4], '--table', argv[5], '--environment', argv[6]]
    dataflow_submit.init_process()
elif dp_service == "close_batch":
    print(f"{dp_service} Invoked")
    sys.argv = ['./appl_audit_utils.py', '--appl_short_name', argv[2], '--source', argv[3], '--status', argv[4], '--schema', argv[5]]
    appl_audit_utils.update_batch()

    
print("****** DATA PIPELINE ADF ORCHESTRATION COMPLETED ******")




