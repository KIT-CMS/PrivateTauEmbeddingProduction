
import law
import luigi
from tasks.EmpeddingTasks import EmbeddingTask
from tasks.EmpeddingTasks.era2022postEE.select_and_clean import (
    CleaningTaskMuMu2022postEE,
)

logger = law.logger.get_logger(__name__)



class GenSimTaskMuMu2022postEE(EmbeddingTask):
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_23",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    RequiredTask = CleaningTaskMuMu2022postEE

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMu/gensim/{self.branch}_gensim.root")

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
                "'"
            ),
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class HLTSimTaskMuMu2022postEE(EmbeddingTask):

    cmssw_version = luigi.Parameter(
        default="CMSSW_12_4_23",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """
    The needed CMSSW version for this task.
    Taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 
    from this chain https://cms-pdmv-prod.web.cern.ch/mcm/chained_requests?prepid=EGM-chain_Run3Summer22EEwmLHEGS_flowRun3Summer22EEDRPremix_flowRun3Summer22EEMiniAODv4_flowRun3Summer22EENanoAODv12-00001&page=0&shown=15
    """
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc10",
        description="The CMSSW scram arch.",
    )
    RequiredTask = GenSimTaskMuMu2022postEE

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMu/hltsim/{self.branch}_hltsim.root")

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


class RecoSimTaskMuMu2022postEE(EmbeddingTask):
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_23",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    RequiredTask = HLTSimTaskMuMu2022postEE

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMu/recosim/{self.branch}_recosim.root")

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


class MergingTaskMuMu2022postEE(EmbeddingTask):
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_23",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    RequiredTask = RecoSimTaskMuMu2022postEE

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMu/merging/{self.branch}_merging.root")

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

class NanoAODTaskMuMu2022postEE(EmbeddingTask):
    
    emb_files_per_job = luigi.IntParameter(
        default=5,
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

    RequiredTask = MergingTaskMuMu2022postEE

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022postEE/MuMu/nanoaod/{self.branch}_nanoaod.root")

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
