
import law
import luigi
from tasks.EmpeddingTasks import EmbeddingTask
from tasks.EmpeddingTasks.era2024.select_and_clean import (
    CleaningTaskTauTau2024,
)

logger = law.logger.get_logger(__name__)

class GenSimTaskMuTau2024(EmbeddingTask):
    
    emb_files_per_job = luigi.IntParameter(
        default=3,
        description="Number of files to process per job.",
    )
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc12",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_15_0_4_patch3",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    cmssw_branch = luigi.Parameter(
        default="embedding_dev_CMSSW_15_0_4_p3_improveCode",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    RequiredTask = CleaningTaskTauTau2024

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuTau/gensim/{self.branch}_gensim.root")

    def run(self):
        """Run the gen cmsdriver command"""
        self.run_cms_driver(
            "TauAnalysis/MCEmbeddingTools/python/Simulation_GEN_cfi.py",
            step="GEN,SIM,DIGI,L1,DIGI2RAW",
            processName="SIMembeddingpreHLT",
            mc=True,
            beamspot="DBrealistic",
            geometry="DB:Extended",
            era="Run3_2024",
            conditions="auto:phase1_2024_realistic", # same Global Tag as in HLTSimTask!
            eventcontent="TauEmbeddingSimGen",
            datatier="RAWSIM",
            procModifiers="tau_embedding_mutauh",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class HLTSimTaskMuTau2024(EmbeddingTask):

    cmssw_version = luigi.Parameter(
        default="CMSSW_15_0_4_patch3",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """
    The needed CMSSW version for this task.
    Taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 
    from this chain https://cms-pdmv-prod.web.cern.ch/mcm/chained_requests?prepid=EGM-chain_Run3Summer22EEwmLHEGS_flowRun3Summer22EEDRPremix_flowRun3Summer22EEMiniAODv4_flowRun3Summer22EENanoAODv12-00001&page=0&shown=15
    """
    
    cmssw_branch = luigi.Parameter(
        default="embedding_dev_CMSSW_15_0_4_p3_improveCode",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc12",
        description="The CMSSW scram arch.",
    )
    RequiredTask = GenSimTaskMuTau2024

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuTau/hltsim/{self.branch}_hltsim.root")

    def run(self):
        """Run the hlt cmsdriver command"""
        # step and conditions taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 (see recomended sample: https://twiki.cern.ch/twiki/bin/viewauth/CMS/MuonRun32022#MC)
        self.run_cms_driver(
            step="HLT:TauAnalysis/MCEmbeddingTools/Simulation_HLT_customiser_cff.embeddingHLTCustomiser.2024v14",
            processName="SIMembeddingHLT",
            mc=True,
            beamspot="DBrealistic",
            geometry="DB:Extended",
            era="Run3_2024",
            conditions="auto:phase1_2024_realistic",
            eventcontent="TauEmbeddingSimHLT",
            datatier="RAWSIM",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class RecoSimTaskMuTau2024(EmbeddingTask):
    
    cmssw_version = luigi.Parameter(
        default="CMSSW_15_0_4_patch3",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """
    The needed CMSSW version for this task.
    Taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 
    from this chain https://cms-pdmv-prod.web.cern.ch/mcm/chained_requests?prepid=EGM-chain_Run3Summer22EEwmLHEGS_flowRun3Summer22EEDRPremix_flowRun3Summer22EEMiniAODv4_flowRun3Summer22EENanoAODv12-00001&page=0&shown=15
    """
    
    cmssw_branch = luigi.Parameter(
        default="embedding_dev_CMSSW_15_0_4_p3_improveCode",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc12",
        description="The CMSSW scram arch.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    RequiredTask = HLTSimTaskMuTau2024

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuTau/recosim/{self.branch}_recosim.root")

    def run(self):
        """Run the reco cmsdriver command"""
        self.run_cms_driver(
            step="RAW2DIGI,L1Reco,RECO:TauAnalysis/MCEmbeddingTools/Simulation_RECO_cff.reconstruction,RECOSIM",
            processName="SIMembedding",
            mc=True,
            beamspot="DBrealistic",
            geometry="DB:Extended",
            era="Run3_2024",
            conditions="auto:phase1_2024_realistic",
            eventcontent="TauEmbeddingSimReco",
            datatier="RAW-RECO-SIM",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class MergingTaskMuTau2024(EmbeddingTask):
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc12",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_15_0_4_patch3",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    cmssw_branch = luigi.Parameter(
        default="embedding_dev_CMSSW_15_0_4_p3_improveCode",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    RequiredTask = RecoSimTaskMuTau2024

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuTau/merging/{self.branch}_merging.root")

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            step="USER:TauAnalysis/MCEmbeddingTools/Merging_USER_cff.merge_step,PAT",
            processName="MERGE",
            data=True,
            conditions="auto:run3_data",
            era="Run3_2024",
            eventcontent="TauEmbeddingMerge",
            datatier="USER",
            inputCommands="'keep *_*_*_*'",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )

class NanoAODTaskMuTau2024(EmbeddingTask):
    
    emb_files_per_job = luigi.IntParameter(
        default=20,
        description="Number of files to process per job.",
    )
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc12",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_15_0_4_patch3",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    cmssw_branch = luigi.Parameter(
        default="embedding_dev_CMSSW_15_0_4_p3_improveCode",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    RequiredTask = MergingTaskMuTau2024

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuTau/nanoaod/{self.branch}_nanoaod.root")

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            step="NANO:TauAnalysis/MCEmbeddingTools/Nano_cff.embedding_nanoAOD_seq",
            data=True,
            conditions="auto:run3_data",
            era="Run3_2024",
            eventcontent="TauEmbeddingNANOAOD",
            datatier="NANOAODSIM",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )
