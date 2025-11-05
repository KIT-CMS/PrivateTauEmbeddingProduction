import law
from tasks.EmpeddingTasks import EmbeddingTask
from tasks.EmpeddingTasks.era2024.select_and_clean import (
    SelectionTask2024,
    condor_2024_param,
    cmssw_2024_param_HLT,
    cmssw_2024_param_15
)
from tasks.htcondor.htcondor import default_param

logger = law.logger.get_logger(__name__)

@default_param(
    htcondor_walltime="2000",
    htcondor_request_cpus="2",
    htcondor_request_memory="4GB",
    htcondor_request_disk="500MB",
    emb_files_per_job=20,
    **condor_2024_param,
    **cmssw_2024_param_HLT,
)
class MiniAODwoEmbeddingTaskMuMu2024(EmbeddingTask):

    RequiredTask = SelectionTask2024
    
    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuMu_wo_embedding/miniaod/{self.branch}_miniaod.root")

    def run(self):
        """Run the cleaning cmsdriver command"""
        self.run_cms_driver(
            step="PAT",
            data=True,
            scenario="pp",
            conditions="140X_dataRun3_v20",
            era="Run3_2024",
            eventcontent="MINIAOD",
            datatier="MINIAOD",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


@default_param(
    htcondor_walltime="900",
    htcondor_request_cpus="4",
    htcondor_request_memory="3GB",
    htcondor_request_disk="1GB",
    emb_files_per_job=12,
    **condor_2024_param,
    **cmssw_2024_param_15,
)
class NanoAODwoEmbeddingTaskMuMu2024(EmbeddingTask):
    
    RequiredTask = MiniAODwoEmbeddingTaskMuMu2024

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuMu_wo_embedding/nanoaod/{self.branch}_nanoaod.root")

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            step="NANO",
            data=True,
            conditions="140X_dataRun3_v20",
            era="Run3_2024",
            eventcontent="NANOAOD",
            datatier="NANOAOD",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )
