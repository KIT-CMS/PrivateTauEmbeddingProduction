
import law
import luigi
from tasks.EmpeddingTasks import EmbeddingTask
from tasks.EmpeddingTasks.era2022postEE.select_and_clean import (
    CleaningTaskTauTau2022postEE,
)

logger = law.logger.get_logger(__name__)

class GenSimTaskMuTau2022postEE(EmbeddingTask):
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_17",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    cmssw_branch = luigi.Parameter(
        default="embedding_backport_CMSSW_13_0_X",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )

    RequiredTask = CleaningTaskTauTau2022postEE

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuTau/gensim/{self.branch}_gensim.root")

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
                """process.generator.HepMCFilter.filterParameters.MuMuCut = cms.string("(Mu.Pt > 18 && Had.Pt > 18 && Mu.Eta < 2.2 && Had.Eta < 2.4)");"""
                """process.generator.HepMCFilter.filterParameters.Final_States = cms.vstring("MuHad");"""
                """process.generator.nAttempts = cms.uint32(1000);"""
                "'"
            ),
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class HLTSimTaskMuTau2022postEE(EmbeddingTask):

    cmssw_version = luigi.Parameter(
        default="CMSSW_12_4_11_patch3",
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
    RequiredTask = GenSimTaskMuTau2022postEE

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuTau/hltsim/{self.branch}_hltsim.root")

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


class RecoSimTaskMuTau2022postEE(EmbeddingTask):
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc10",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_12_4_11_patch3",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    cmssw_branch = luigi.Parameter(
        default="embedding_backport_CMSSW_12_4_X",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    RequiredTask = HLTSimTaskMuTau2022postEE

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuTau/recosim/{self.branch}_recosim.root")

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


class MergingTaskMuTau2022postEE(EmbeddingTask):
    
    emb_files_per_job = luigi.IntParameter(
        default=5,
        description="Number of files to process per job.",
    )
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_17",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    cmssw_branch = luigi.Parameter(
        default="embedding_backport_CMSSW_13_0_X",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )

    RequiredTask = RecoSimTaskMuTau2022postEE

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuTau/merging/{self.branch}_merging.root")

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

class NanoAODTaskMuTau2022postEE(EmbeddingTask):
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_17",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    cmssw_branch = luigi.Parameter(
        default="embedding_backport_CMSSW_13_0_X",
        description="The CMSSW git branch to use with the chosen cmssw version",
    )

    RequiredTask = MergingTaskMuTau2022postEE

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuTau/nanoaod/{self.branch}_nanoaod.root")

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
