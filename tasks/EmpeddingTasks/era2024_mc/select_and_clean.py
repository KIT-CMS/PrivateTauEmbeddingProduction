import law
import luigi
from tasks.EmpeddingTasks import EmbeddingTask
from tasks.htcondor.cmssw import ETP_CMSSW_HTCondorWorkflow

logger = law.logger.get_logger(__name__)

#
# CMSSW versions:
#  - CMSSW_13_0_17: Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50
#  - CMSSW_12_4_11_patch3: The CMSSW version used in MC production for 2022 DY samples  Taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 from this chain https://cms-pdmv-prod.web.cern.ch/mcm/chained_requests?prepid=EGM-chain_Run3Summer22EEwmLHEGS_flowRun3Summer22EEDRPremix_flowRun3Summer22EEMiniAODv4_flowRun3Summer22EENanoAODv12-00001&page=0&shown=15



class SelectionTask2024MC(ETP_CMSSW_HTCondorWorkflow, law.LocalWorkflow):
    """This class is the first step in the embedding workflow. Therfore can't inherit from EmbeddingTask"""

    emb_number_of_events = luigi.Parameter(
        default="-1",
        description="Number of events to process. Default is -1, which means all events.",
    )

    emb_filelist = luigi.Parameter(
        default= "artur_2024_mc_prod.filelist",
        description="List of input files.",
    )

    cmssw_version = luigi.Parameter(
        default="CMSSW_15_1_0_pre1",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""
    
    cmssw_branch = luigi.Parameter(
        default="embedding_dev_CMSSW_15_1_0_pre1",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc12",
        description="The CMSSW scram arch.",
    )
    def create_branch_map(self):
        """This branch map maps one file from the filelist in the filelists folder to one job (branch)"""
        filelist_path = law.util.rel_path(__file__, "filelists", self.emb_filelist)
        with open(filelist_path, "r") as f:
            files = [i.strip() for i in f.readlines() if i.strip()]
        return {i: file for i, file in enumerate(files)}

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(
            f"2024_mc/selection/{self.branch}_selection.root"
        )

    def run(self):
        """Run the selection cmsdriver command"""
        logger.warning(self.branch_data)
        self.run_cms_driver(
            "RECO",
            mc=True,
            step="RAW2DIGI,L1Reco,RECO,PAT",
            geometry="DB:Extended",
            conditions="auto:phase1_2024_realistic",
            era="Run3_2024",
            eventcontent="RAWRECO",
            datatier="RAWRECO",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseSelecting",
            filein=f"root://cmsdcache-kit-disk.gridka.de:1094/{self.branch_data}",
            number=self.emb_number_of_events,
        )

class CleaningTaskMuMu2024MC(EmbeddingTask):

    RequiredTask = SelectionTask2024MC
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc12",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_15_1_0_pre1",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""
    
    cmssw_branch = luigi.Parameter(
        default="embedding_dev_CMSSW_15_1_0_pre1",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024_mc/MuMu/cleaning/{self.branch}_cleaning.root")

    def run(self):
        """Run the cleaning cmsdriver command"""
        self.run_cms_driver(
            "LHEprodandCLEAN",
            mc=True,
            step="RAW2DIGI,RECO,PAT",
            geometry="DB:Extended",
            conditions="auto:phase1_2024_realistic",
            era="Run3_2024",
            eventcontent="RAWRECO",
            datatier="RAWRECO",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseLHEandCleaning",
            customise_commands=(  # configs for Mu->Mu embedding
                "'process.externalLHEProducer.particleToEmbed = cms.int32(13)'"
            ),
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class CleaningTaskTauTau2024MC(EmbeddingTask):

    RequiredTask = SelectionTask2024MC
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc12",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_15_1_0_pre1",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""
    
    cmssw_branch = luigi.Parameter(
        default="embedding_dev_CMSSW_15_1_0_pre1",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024_mc/cleaning/{self.branch}_cleaning.root")

    def run(self):
        """Run the cleaning cmsdriver command"""
        self.run_cms_driver(
            "LHEprodandCLEAN",
            mc=True,
            step="RAW2DIGI,RECO,PAT",
            geometry="DB:Extended",
            conditions="auto:phase1_2024_realistic",
            era="Run3_2024",
            eventcontent="RAWRECO",
            datatier="RAWRECO",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseLHEandCleaning",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )
