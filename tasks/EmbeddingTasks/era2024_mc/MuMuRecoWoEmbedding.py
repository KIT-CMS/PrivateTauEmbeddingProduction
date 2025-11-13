import law
from tasks.EmbeddingTasks import EmbeddingTask
from tasks.htcondor.cmssw import ETP_CMSSW_HTCondorWorkflow
from tasks.EmbeddingTasks.era2024_mc.select_and_clean import (
    SelectionTask2024MC,
    default_2024_mc_param,
)
from tasks.htcondor.htcondor import default_param

logger = law.logger.get_logger(__name__)

@default_param(
    htcondor_walltime="2000",
    htcondor_request_cpus="2",
    htcondor_request_memory="4GB",
    htcondor_request_disk="500MB",
    emb_files_per_job=20,
    **default_2024_mc_param,
)
class MiniAODwoEmbeddingTaskMuMu2024MC(EmbeddingTask):

    RequiredTask = SelectionTask2024MC
    
    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024_mc/MuMu_wo_embedding/miniaod/{self.branch}_miniaod.root")

    def run(self):
        """Run the cleaning cmsdriver command"""
        self.run_cms_driver(
            step="NONE",
            mc=True,
            beamspot="DBrealistic",
            geometry="DB:Extended",
            conditions="auto:phase1_2024_realistic",
            era="Run3_2024",
            eventcontent="MINIAODSIM",
            datatier="MINIAODSIM",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


@default_param(
    htcondor_walltime="900",
    htcondor_request_cpus="4",
    htcondor_request_memory="3GB",
    htcondor_request_disk="1GB",
    emb_files_per_job=12,
    **default_2024_mc_param,
)
class NanoAODwoEmbeddingTaskMuMu2024MC(EmbeddingTask):
    
    RequiredTask = MiniAODwoEmbeddingTaskMuMu2024MC

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024_mc/MuMu_wo_embedding/nanoaod/{self.branch}_nanoaod.root")

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            step="NANO",
            mc=True,
            scenario="pp",
            conditions="auto:phase1_2024_realistic",
            era="Run3_2024",
            eventcontent="NANOAODSIM",
            datatier="NANOAODSIM",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )
