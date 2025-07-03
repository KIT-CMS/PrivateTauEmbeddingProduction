
import law
import luigi
from tasks.EmpeddingTasks import EmbeddingTask
from tasks.EmpeddingTasks.era2024_mc.select_and_clean import (
    SelectionTask2024MC,
)

logger = law.logger.get_logger(__name__)

class CleaningTaskMuMu2024MCWOInitialRecoCorrection(EmbeddingTask):

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
        return law.wlcg.WLCGFileTarget(f"2024_mc/MuMu_wo_InitialRecoCorrection/cleaning/{self.branch}_cleaning.root")

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

class GenSimTaskMuMu2024MCWOInitialRecoCorrection(EmbeddingTask):
    
    emb_files_per_job = luigi.IntParameter(
        default=1,
        description="Number of files to process per job.",
    )
    
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
    
    RequiredTask = CleaningTaskMuMu2024MCWOInitialRecoCorrection

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024_mc/MuMu_wo_InitialRecoCorrection/gensim/{self.branch}_gensim.root")

    def run(self):
        """Run the gen cmsdriver command"""
        self.run_cms_driver(
            "TauAnalysis/MCEmbeddingTools/python/EmbeddingPythia8Hadronizer_cfi.py",
            step="GEN,SIM,DIGI,L1,DIGI2RAW",
            mc=True,
            beamspot="DBrealistic",
            geometry="DB:Extended",
            era="Run3_2024",
            conditions="auto:phase1_2024_realistic", # same Global Tag as in HLTSimTask!
            eventcontent="RAWSIM",
            datatier="RAWSIM",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseGenerator_preHLT",
            customise_commands=(  # configs for Mu->Mu embedding
                "'"
                """process.generator.HepMCFilter.filterParameters.MuMuCut = cms.string("(Mu1.Pt > 17 && Mu2.Pt > 8 && Mu1.Eta < 2.5 && Mu2.Eta < 2.5)");"""
                """process.generator.HepMCFilter.filterParameters.Final_States = cms.vstring("MuMu");"""
                """process.generator.nAttempts = cms.uint32(1);"""
                "'"
            ),
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class HLTSimTaskMuMu2024MCWOInitialRecoCorrection(EmbeddingTask):

    cmssw_version = luigi.Parameter(
        default="CMSSW_15_1_0_pre1",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """
    The needed CMSSW version for this task.
    Taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 
    from this chain https://cms-pdmv-prod.web.cern.ch/mcm/chained_requests?prepid=EGM-chain_Run3Summer22EEwmLHEGS_flowRun3Summer22EEDRPremix_flowRun3Summer22EEMiniAODv4_flowRun3Summer22EENanoAODv12-00001&page=0&shown=15
    """
    
    cmssw_branch = luigi.Parameter(
        default="embedding_dev_CMSSW_15_1_0_pre1",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc12",
        description="The CMSSW scram arch.",
    )
    RequiredTask = GenSimTaskMuMu2024MCWOInitialRecoCorrection

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024_mc/MuMu_wo_InitialRecoCorrection/hltsim/{self.branch}_hltsim.root")

    def run(self):
        """Run the hlt cmsdriver command"""
        # step and conditions taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 (see recomended sample: https://twiki.cern.ch/twiki/bin/viewauth/CMS/MuonRun32022#MC)
        self.run_cms_driver(
            "TauAnalysis/MCEmbeddingTools/python/EmbeddingPythia8Hadronizer_cfi.py",
            step="HLT:2024v14",
            mc=True,
            beamspot="DBrealistic",
            geometry="DB:Extended",
            era="Run3_2024",
            conditions="auto:phase1_2024_realistic",
            eventcontent="RAWSIM",
            datatier="RAWSIM",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseGenerator_HLT",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class RecoSimTaskMuMu2024MCWOInitialRecoCorrection(EmbeddingTask):
    
    cmssw_version = luigi.Parameter(
        default="CMSSW_15_1_0_pre1",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """
    The needed CMSSW version for this task.
    Taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 
    from this chain https://cms-pdmv-prod.web.cern.ch/mcm/chained_requests?prepid=EGM-chain_Run3Summer22EEwmLHEGS_flowRun3Summer22EEDRPremix_flowRun3Summer22EEMiniAODv4_flowRun3Summer22EENanoAODv12-00001&page=0&shown=15
    """
    
    cmssw_branch = luigi.Parameter(
        default="embedding_dev_CMSSW_15_1_0_pre1",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc12",
        description="The CMSSW scram arch.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    RequiredTask = HLTSimTaskMuMu2024MCWOInitialRecoCorrection

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024_mc/MuMu_wo_InitialRecoCorrection/recosim/{self.branch}_recosim.root")

    def run(self):
        """Run the reco cmsdriver command"""
        self.run_cms_driver(
            "TauAnalysis/MCEmbeddingTools/python/EmbeddingPythia8Hadronizer_cfi.py",
            step="RAW2DIGI,L1Reco,RECO,RECOSIM",
            mc=True,
            beamspot="DBrealistic",
            geometry="DB:Extended",
            era="Run3_2024",
            conditions="auto:phase1_2024_realistic",
            eventcontent="RAWRECOSIMHLT",
            datatier="RAW-RECO-SIM",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseGenerator_postHLT",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class MergingTaskMuMu2024MCWOInitialRecoCorrection(EmbeddingTask):
    
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
    
    RequiredTask = RecoSimTaskMuMu2024MCWOInitialRecoCorrection

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024_mc/MuMu_wo_InitialRecoCorrection/merging/{self.branch}_merging.root")

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            "PAT",
            step="PAT",
            mc=True,
            conditions="auto:phase1_2024_realistic",
            era="Run3_2024",
            eventcontent="MINIAODSIM",
            datatier="USER",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseMerging",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )

class NanoAODTaskMuMu2024MCWOInitialRecoCorrection(EmbeddingTask):
    
    emb_files_per_job = luigi.IntParameter(
        default=1,
        description="Number of files to process per job.",
    )
    
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
    
    RequiredTask = MergingTaskMuMu2024MCWOInitialRecoCorrection

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024_mc/MuMu_wo_InitialRecoCorrection/nanoaod/{self.branch}_nanoaod.root")

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            "",
            step="NANO",
            mc=True,
            conditions="auto:phase1_2024_realistic",
            era="Run3_2024",
            eventcontent="NANOAODSIM",
            datatier="NANOAODSIM",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseNanoAOD",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )
