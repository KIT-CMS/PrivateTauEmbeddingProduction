import os

import law
import luigi
from law.util import interruptable_popen, quote_cmd, rel_path
from luigi.util import inherits

from ..htcondor.cmssw import ETP_CMSSW_HTCondorWorkflow

logger = law.logger.get_logger(__name__)

#
# CMSSW versions:
#  - CMSSW_13_0_17: Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50
#  - CMSSW_12_4_11_patch3: The CMSSW version used in MC production for 2022 DY samples  Taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 from this chain https://cms-pdmv-prod.web.cern.ch/mcm/chained_requests?prepid=EGM-chain_Run3Summer22EEwmLHEGS_flowRun3Summer22EEDRPremix_flowRun3Summer22EEMiniAODv4_flowRun3Summer22EENanoAODv12-00001&page=0&shown=15


class EmbeddingTask(ETP_CMSSW_HTCondorWorkflow, law.LocalWorkflow):
    """This class combines common functionality for embedding Tasks"""

    emb_number_of_events = luigi.Parameter(
        default="100",
        description="Number of events to process. Default is -1, which means all events.",
    )

    emb_files_per_job = luigi.IntParameter(
        default=1,
        description="Number of files to process per job. This has no effect in the PreselectionTask.",
    )

    # cmssw_version = luigi.Parameter(
    #     description="The CMSSW version to use for the cmsdriver command.",
    # )
    
    # cmssw_scram_arch = luigi.Parameter(
    #     description="The CMSSW scram arch.",
    # )

    RequiredTask = None

    exclude_params_req = set(
        [
            "emb_number_of_events",
            "emb_files_per_job",
            "htcondor_walltime",
            "htcondor_request_cpus",
            "htcondor_request_memory",
            "htcondor_request_disk",
        ]
    )
    """These parameters are not overwritten by the BundleCMSSWTask.reqs() method."""

    def create_branch_map(self):
        """Maps `self.emb_files_per_job number` of files to one job using the get_all_branch_chunks method from law."""
        branch_chunks = self.RequiredTask().get_all_branch_chunks(
            self.emb_files_per_job
        )
        return {i: chunk for i, chunk in enumerate(branch_chunks)}

    def workflow_requires(self):
        """Requires the RequiredTask"""
        # exclude the cmssw_version parameter, as we want to set it seperately
        return {"files": self.RequiredTask()}

    def requires(self):
        """What the single jobs require to run, is in this case the RequiredTask jobs which provide the files specified in branch_data"""
        return self.RequiredTask()

    def get_input_files(self):
        """Get the input files from the RequiredTask output"""
        # get the input files from the output of the PreselectionTask
        # this is a bit complicated as the output of a Workflow Task is in a dictionarry with the key 'collection'
        # and the value is a law.SiblingFileCollection
        return [
            wlcg_target.uri()
            for wlcg_target in self.input()["collection"].targets.values()
        ]


class PreselectionTask(ETP_CMSSW_HTCondorWorkflow, law.LocalWorkflow):
    """This class is the first step in the embedding workflow. Therfore can't inherit from EmbeddingTask"""

    emb_number_of_events = luigi.Parameter(
        default="100",
        description="Number of events to process. Default is -1, which means all events.",
    )

    emb_filelist = luigi.Parameter(
        description="List of input files.",
    )

    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_17",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    def create_branch_map(self):
        """This branch map maps one file from the filelist in the filelists folder to one job (branch)"""
        filelist_path = law.util.rel_path(__file__, "filelists", self.emb_filelist)
        with open(filelist_path, "r") as f:
            files = {i.strip() for i in f.readlines() if i.strip()}
        return {i: file for i, file in enumerate(files)}

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(
            f"2022/preselection/{self.branch}_preselection.root"
        )

    def run(self):
        """Run the preselection cmsdriver command"""
        self.run_cms_driver(
            "RECO",
            data=True,
            step="RAW2DIGI,L1Reco,RECO,PAT",
            scenario="pp",
            conditions="auto:run3_data",
            era="Run3",
            eventcontent="RAW",
            datatier="RAW",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseSelecting",
            filein=f"root://cmsdcache-kit-disk.gridka.de:1094/{self.branch_data}",
            number=self.emb_number_of_events,
        )


class SelectionTask(EmbeddingTask):

    RequiredTask = PreselectionTask

    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_17",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""
    
    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022/selection/{self.branch}_selection.root")

    def run(self):
        """Run the selection cmsdriver command"""
        self.run_cms_driver(
            "RECO",
            data=True,
            step="RAW2DIGI,L1Reco,RECO,PAT",
            scenario="pp",
            conditions="auto:run3_data",
            era="Run3",
            eventcontent="RAWRECO",
            datatier="RAWRECO",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseSelecting_Reselect",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class CleaningTask(EmbeddingTask):

    RequiredTask = SelectionTask
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_17",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""
    
    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022/cleaning/{self.branch}_cleaning.root")

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
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseLHEandCleaning_Reselect",
            customise_commands=(  # configs for Mu->Mu embedding
                "'process.externalLHEProducer.particleToEmbed = cms.int32(13)'"
            ),
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class GenSimTask(EmbeddingTask):
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_17",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    RequiredTask = CleaningTask

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022/gensim/{self.branch}_gensim.root")

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
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseGenerator_preHLT_Reselect",
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


class HLTSimTask(EmbeddingTask):

    cmssw_version = luigi.Parameter(
        default="CMSSW_12_4_11_patch3",
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
    RequiredTask = GenSimTask

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022/hltsim/{self.branch}_hltsim.root")

    def run(self):
        """Run the hlt cmsdriver command"""
        # step and conditions taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 (see recomended sample: https://twiki.cern.ch/twiki/bin/viewauth/CMS/MuonRun32022#MC)
        self.run_cms_driver(
            "TauAnalysis/MCEmbeddingTools/python/EmbeddingPythia8Hadronizer_cfi.py",
            step="HLT:2022v14",
            mc=True,
            beamspot="Realistic25ns13TeVEarly2018Collision",
            geometry="DB:Extended",
            era="Run3",
            conditions="124X_mcRun3_2022_realistic_postEE_v1",
            eventcontent="RAWSIM",
            datatier="RAWSIM",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseGenerator_HLT_Reselect",
            customise_commands="'process.source.bypassVersionCheck = cms.untracked.bool(True);'",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class RecoSimTask(EmbeddingTask):
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc10",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_12_4_11_patch3",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    RequiredTask = HLTSimTask

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022/recosim/{self.branch}_recosim.root")

    def run(self):
        """Run the reco cmsdriver command"""
        self.run_cms_driver(
            "TauAnalysis/MCEmbeddingTools/python/EmbeddingPythia8Hadronizer_cfi.py",
            step="RAW2DIGI,L1Reco,RECO,RECOSIM",
            mc=True,
            beamspot="Realistic25ns13TeVEarly2018Collision",
            geometry="DB:Extended",
            era="Run3",
            conditions="124X_mcRun3_2022_realistic_postEE_v1",
            eventcontent="RAWRECOSIMHLT",
            datatier="RAW-RECO-SIM",
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseGenerator_postHLT_Reselect",
            customise_commands="'process.source.bypassVersionCheck = cms.untracked.bool(True);'",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


class MergingTask(EmbeddingTask):
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_17",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    RequiredTask = RecoSimTask

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022/merging/{self.branch}_merging.root")

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
            customise="TauAnalysis/MCEmbeddingTools/customisers.customiseMerging_Reselect",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )

class NanoAODTask(EmbeddingTask):
    
    cmssw_scram_arch = luigi.Parameter(
        default="el8_amd64_gcc11",
        description="The CMSSW scram arch.",
    )
    cmssw_version = luigi.Parameter(
        default="CMSSW_13_0_17",
        description="The CMSSW version to use for the cmsdriver command.",
    )
    """Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50"""

    RequiredTask = MergingTask

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2022/nanoaod/{self.branch}_nanoaod.root")

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
