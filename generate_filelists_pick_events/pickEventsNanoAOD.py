#!/usr/bin/env python3
#
# Getting only the events from a NanoAOD file that are in a list of events
# 
from argparse import ArgumentParser

import ROOT
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import (
    Collection,
    Object,
)
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import (
    PostProcessor,
)

ROOT.PyConfig.IgnoreCommandLineOptions = True

run_lumis_events = {}

class PickEventsNanoAOD(Module):

    def analyze(self, event):
        event_nr = event["event"]# Object(event, "event")
        run = event["run"] # Object(event, "run")
        lumi = event["luminosityBlock"]# Object(event, "luminosityBlock")
        # print(event_nr, run, lumi)
        # print(event_nr[0], type(event_nr[0]))
        # exit()
        if event_nr in run_lumis_events.get(run, {}).get(lumi, set()):
            # print(f"found event {run}:{lumi}:{event_nr}")
            run_lumis_events[run][lumi].remove(event_nr)
            return True
        else:
            return False

if __name__ == "__main__":
    parser = ArgumentParser()
    
    parser.add_argument("event_run_lumi_file", help="File with events to pick", type=str)
    parser.add_argument("input_files_file", help="File with input files", type=str)
    args = parser.parse_args()
    
    # read input files
    with open(args.input_files_file) as f:
        input_files = [line.strip() for line in f if line.strip()]
        
    with open(args.event_run_lumi_file) as f:
        for line in f:
            line = line.strip()
            if not line: # if line is empty
                continue
            run, lumi, event = line.split(":")
            run_lumis_events.setdefault(int(run), {}).setdefault(int(lumi), set()).add(int(event))
    p = PostProcessor(".",
                    input_files,
                    modules=[PickEventsNanoAOD()],
                    )
    p.run()
    
    print("leftover events:")
    for run, lumis in run_lumis_events.items():
        for lumi, events in lumis.items():
            for event in events:
                print(f"{run}:{lumi}:{event}")