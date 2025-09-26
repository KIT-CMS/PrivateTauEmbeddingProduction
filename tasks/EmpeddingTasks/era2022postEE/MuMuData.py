
import law
import luigi
from tasks.EmpeddingTasks import EmbeddingTask
from tasks.EmpeddingTasks.era2022postEE.select_and_clean import (
    SelectionTask2022postEE,
)

logger = law.logger.get_logger(__name__)

class MiniAODTaskDataMuMu2022postEE(EmbeddingTask):

    RequiredTask = SelectionTask2022postEE
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_23",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""
    
    cmssw_branch = luigi.Parameter(
        default="embedding_backport_CMSSW_13_0_X_wo_InitialRecoCorrection",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMuData/miniaod/{self.branch}_miniaod.root")

    def run(self):
        """Run the cleaning cmsdriver command"""
        self.run_cms_driver(
            step="PAT",
            processName="PAT",
            data=True,
            scenario="pp",
            conditions="auto:run3_data",
            era="Run3",
            eventcontent="MINIAOD",
            datatier="MINIAOD",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )

class NanoAODTaskDataMuMu2022postEE(EmbeddingTask):
    
    emb_files_per_job = luigi.IntParameter(
        default=40,
        description="Number of files to process per job.",
    )
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_23",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    cmssw_branch = luigi.Parameter(
        default="embedding_backport_CMSSW_13_0_X_wo_InitialRecoCorrection",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    RequiredTask = MiniAODTaskDataMuMu2022postEE

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMuData/nanoaod/{self.branch}_nanoaod.root")

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            step="NANO",
            data=True,
            conditions="auto:run3_data",
            era="Run3",
            eventcontent="NANOAOD",
            datatier="NANOAOD",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )
