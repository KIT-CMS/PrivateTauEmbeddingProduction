import law
import luigi
from tasks.EmbeddingTasks import EmbeddingTask
from tasks.EmbeddingTasks.era2024_mc.select_and_clean import (
    CleaningTaskTauTau2024MC,
    default_2024_mc_param,
)
from tasks.htcondor.htcondor import default_param

logger = law.logger.get_logger(__name__)


@default_param(
    htcondor_walltime="3600",
    htcondor_request_cpus="8",
    htcondor_request_memory="8GB",
    htcondor_request_disk="10GB",
    **default_2024_mc_param,
)
class GenSimTaskMuTau2024MC(EmbeddingTask):

    RequiredTask = CleaningTaskTauTau2024MC

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024_mc/MuTau/gensim/{self.branch}_gensim.root")

    def run(self):
        """Run the gen cmsdriver command"""
        self.run_cms_driver(
            "TauAnalysis/MCEmbeddingTools/python/Simulation_GEN_cfi.py",
            step="GEN,SIM,DIGI,L1,DIGI2RAW",
            mc=True,
            beamspot="DBrealistic",
            geometry="DB:Extended",
            era="Run3_2024",
            conditions="auto:phase1_2024_realistic",  # same Global Tag as in HLTSimTask!
            eventcontent="TauEmbeddingSimGen",
            datatier="RAWSIM",
            procModifiers="tau_embedding_sim,tau_embedding_mutauh",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )

@default_param(
    htcondor_walltime="7800",
    htcondor_request_cpus="4",
    htcondor_request_memory="4GB",
    htcondor_request_disk="20GB",
    emb_files_per_job=2,
    **default_2024_mc_param,
)
class HLTSimTaskMuTau2024MC(EmbeddingTask):

    RequiredTask = GenSimTaskMuTau2024MC

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024_mc/MuTau/hltsim/{self.branch}_hltsim.root")

    def run(self):
        """Run the hlt cmsdriver command"""
        # step and conditions taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 (see recomended sample: https://twiki.cern.ch/twiki/bin/viewauth/CMS/MuonRun32022#MC)
        self.run_cms_driver(
            step="HLT:Fake2+TauAnalysis/MCEmbeddingTools/Simulation_HLT_customiser_cff.embeddingHLTCustomiser",
            processName="SIMembeddingHLT",
            mc=True,
            beamspot="DBrealistic",
            geometry="DB:Extended",
            era="Run3_2024",
            conditions="auto:phase1_2024_realistic",
            eventcontent="TauEmbeddingSimHLT",
            customise_commands="\"process.source.bypassVersionCheck = cms.untracked.bool(True)\"",
            datatier="RAWSIM",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


@default_param(
    htcondor_walltime="6700",
    htcondor_request_cpus="2",
    htcondor_request_memory="4GB",
    htcondor_request_disk="20GB",
    **default_2024_mc_param,
)
class RecoSimTaskMuTau2024MC(EmbeddingTask):

    RequiredTask = HLTSimTaskMuTau2024MC

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(
            f"2024_mc/MuTau/recosim/{self.branch}_recosim.root"
        )

    def run(self):
        """Run the reco cmsdriver command"""
        self.run_cms_driver(
            step="RAW2DIGI,L1Reco,RECO,RECOSIM",
            processName="SIMembedding",
            mc=True,
            beamspot="DBrealistic",
            geometry="DB:Extended",
            era="Run3_2024",
            conditions="auto:phase1_2024_realistic",
            eventcontent="TauEmbeddingSimReco",
            datatier="RAW-RECO-SIM",
            procModifiers="tau_embedding_sim",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )


@default_param(
    htcondor_walltime="4200",
    htcondor_request_cpus="1",
    htcondor_request_memory="4GB",
    htcondor_request_disk="300MB",
    emb_files_per_job=2,
    **default_2024_mc_param,
)
class MergingTaskMuTau2024MC(EmbeddingTask):

    RequiredTask = RecoSimTaskMuTau2024MC

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(
            f"2024_mc/MuTau/merging/{self.branch}_merging.root"
        )

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            step="USER:TauAnalysis/MCEmbeddingTools/Merging_USER_MC_cff.merge_step,PAT",
            processName="MERGE",
            mc=True,
            conditions="auto:phase1_2024_realistic",
            era="Run3_2024",
            eventcontent="TauEmbeddingMergeMINIAOD",
            datatier="USER",
            procModifiers="tau_embedding_merging",
            inputCommands="'keep *_*_*_*'",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )

@default_param(
    htcondor_walltime="900",
    htcondor_request_cpus="2",
    htcondor_request_memory="2GB",
    htcondor_request_disk="300MB",
    emb_files_per_job=20,
    **default_2024_mc_param,
)
class NanoAODTaskMuTau2024MC(EmbeddingTask):

    RequiredTask = MergingTaskMuTau2024MC

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(
            f"2024_mc/MuTau/nanoaod/{self.branch}_nanoaod.root"
        )

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            step="NANO:@TauEmbedding",
            mc=True,
            conditions="auto:phase1_2024_realistic",
            era="Run3_2024",
            eventcontent="TauEmbeddingNANOAOD",
            datatier="NANOAODSIM",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )
