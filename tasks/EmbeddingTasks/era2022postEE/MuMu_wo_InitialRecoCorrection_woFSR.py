
import law
import luigi
from tasks.EmbeddingTasks import EmbeddingTask
from tasks.EmbeddingTasks.era2022postEE.select_and_clean import (
    SelectionTask2022postEE,
)

logger = law.logger.get_logger(__name__)

class CleaningTaskMuMu2022postEEWOInitialRecoCorrection(EmbeddingTask):

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
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMu_wo_InitialRecoCorrection/cleaning/{self.branch}_cleaning.root")

    def run(self):
        """Run the cleaning cmsdriver command"""
        self.run_cms_driver(
            "LHEprodandCLEAN",
            data=True,
            step="RAW2DIGI,RECO,PAT",
            scenario="pp",
            conditions="auto:run3_data",
            era="Run3",
            eventcontent="RAWRECO",
            datatier="RAWRECO",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseLHEandCleaning",
            customise_commands=(  # configs for Mu->Mu embedding
                "'process.externalLHEProducer.particleToEmbed = cms.int32(13)'"
            ),
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )

class GenSimTaskMuMu2022postEEWOInitialRecoCorrectionWOFSR(EmbeddingTask):
    
    emb_files_per_job = luigi.IntParameter(
        default=3,
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
    
    RequiredTask = CleaningTaskMuMu2022postEEWOInitialRecoCorrection

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMu_wo_InitialRecoCorrectionWOFSR/gensim/{self.branch}_gensim.root")

    def run(self):
        """Run the gen cmsdriver command"""
        self.run_cms_driver(
            "TauAnalysis/MCEmbeddingTools/python/EmbeddingPythia8Hadronizer_cfi.py",
            step="GEN,SIM,DIGI,L1,DIGI2RAW",
            mc=True,
            beamspot="Realistic25ns13p6TeVEarly2022Collision",
            geometry="DB:Extended",
            era="Run3",
            conditions="124X_mcRun3_2022_realistic_postEE_v1", # same Global Tag as in HLTSimTask!
            eventcontent="RAWSIM",
            datatier="RAWSIM",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseGenerator_preHLT",
            customise_commands=(  # configs for Mu->Mu embedding
                "'"
                """process.generator.HepMCFilter.filterParameters.MuMuCut = cms.string("(Mu1.Pt > 17 && Mu2.Pt > 8 && Mu1.Eta < 2.5 && Mu2.Eta < 2.5)");"""
                """process.generator.HepMCFilter.filterParameters.Final_States = cms.vstring("MuMu");"""
                """process.generator.nAttempts = cms.uint32(1);"""
                """process.generator.PythiaParameters.processParameters=cms.vstring("JetMatching:merge = off","Init:showChangedSettings = off","Init:showChangedParticleData = off","ProcessLevel:all = off","PartonLevel:FSR = off");"""
                "'"
            ),
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class HLTSimTaskMuMu2022postEEWOInitialRecoCorrectionWOFSR(EmbeddingTask):

    cmssw_version = luigi.Parameter(
        default="CMSSW_12_4_23",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """
    The needed CMSSW version for this task.
    Taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 
    from this chain https://cms-pdmv-prod.web.cern.ch/mcm/chained_requests?prepid=EGM-chain_Run3Summer22EEwmLHEGS_flowRun3Summer22EEDRPremix_flowRun3Summer22EEMiniAODv4_flowRun3Summer22EENanoAODv12-00001&page=0&shown=15
    """
    
    cmssw_branch = luigi.Parameter(
        default="embedding_backport_CMSSW_12_4_X",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc10",
        description="The CMSSW scram arch.",
    )
    RequiredTask = GenSimTaskMuMu2022postEEWOInitialRecoCorrectionWOFSR

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMu_wo_InitialRecoCorrectionWOFSR/hltsim/{self.branch}_hltsim.root")

    def run(self):
        """Run the hlt cmsdriver command"""
        # step and conditions taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 (see recomended sample: https://twiki.cern.ch/twiki/bin/viewauth/CMS/MuonRun32022#MC)
        self.run_cms_driver(
            "TauAnalysis/MCEmbeddingTools/python/EmbeddingPythia8Hadronizer_cfi.py",
            step="HLT:2022v14",
            mc=True,
            beamspot="Realistic25ns13p6TeVEarly2022Collision",
            geometry="DB:Extended",
            era="Run3",
            conditions="124X_mcRun3_2022_realistic_postEE_v1",
            eventcontent="RAWSIM",
            datatier="RAWSIM",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseGenerator_HLT",
            customise_commands="'process.source.bypassVersionCheck = cms.untracked.bool(True);'",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class RecoSimTaskMuMu2022postEEWOInitialRecoCorrectionWOFSR(EmbeddingTask):
    
    cmssw_version = luigi.Parameter(
        default="CMSSW_12_4_23",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """
    The needed CMSSW version for this task.
    Taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 
    from this chain https://cms-pdmv-prod.web.cern.ch/mcm/chained_requests?prepid=EGM-chain_Run3Summer22EEwmLHEGS_flowRun3Summer22EEDRPremix_flowRun3Summer22EEMiniAODv4_flowRun3Summer22EENanoAODv12-00001&page=0&shown=15
    """
    
    cmssw_branch = luigi.Parameter(
        default="embedding_backport_CMSSW_12_4_X",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc10",
        description="The CMSSW scram arch.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    RequiredTask = HLTSimTaskMuMu2022postEEWOInitialRecoCorrectionWOFSR

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMu_wo_InitialRecoCorrectionWOFSR/recosim/{self.branch}_recosim.root")

    def run(self):
        """Run the reco cmsdriver command"""
        self.run_cms_driver(
            "TauAnalysis/MCEmbeddingTools/python/EmbeddingPythia8Hadronizer_cfi.py",
            step="RAW2DIGI,L1Reco,RECO,RECOSIM",
            mc=True,
            beamspot="Realistic25ns13p6TeVEarly2022Collision",
            geometry="DB:Extended",
            era="Run3",
            conditions="124X_mcRun3_2022_realistic_postEE_v1",
            eventcontent="RAWRECOSIMHLT",
            datatier="RAW-RECO-SIM",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseGenerator_postHLT",
            customise_commands="'process.source.bypassVersionCheck = cms.untracked.bool(True);'",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class MergingTaskMuMu2022postEEWOInitialRecoCorrectionWOFSR(EmbeddingTask):
    
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
    
    RequiredTask = RecoSimTaskMuMu2022postEEWOInitialRecoCorrectionWOFSR

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMu_wo_InitialRecoCorrectionWOFSR/merging/{self.branch}_merging.root")

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            "PAT",
            step="PAT",
            data=True,
            conditions="auto:run3_data",
            era="Run3",
            eventcontent="MINIAODSIM",
            datatier="USER",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseMerging",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )

class NanoAODTaskMuMu2022postEEWOInitialRecoCorrectionWOFSR(EmbeddingTask):
    
    emb_files_per_job = luigi.IntParameter(
        default=20,
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
    
    RequiredTask = MergingTaskMuMu2022postEEWOInitialRecoCorrectionWOFSR

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMu_wo_InitialRecoCorrectionWOFSR/nanoaod/{self.branch}_nanoaod.root")

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            "",
            step="NANO",
            data=True,
            conditions="auto:run3_data",
            era="Run3",
            eventcontent="NANOAODSIM",
            datatier="NANOAODSIM",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseNanoAOD",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )
