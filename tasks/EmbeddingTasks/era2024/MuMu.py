
import law
import luigi
from tasks.EmbeddingTasks import EmbeddingTask
from tasks.EmbeddingTasks.era2024.select_and_clean import (
    SelectionTask2024,
    condor_2024_param,
    cmssw_2024_param_HLT,
    cmssw_2024_param_15
)
from tasks.htcondor.htcondor import default_param

logger = law.logger.get_logger(__name__)

@default_param(
    htcondor_walltime="8400",
    htcondor_request_cpus="8",
    htcondor_request_memory="6GB",
    htcondor_request_disk="14GB",
    emb_files_per_job=3,
    **condor_2024_param,
    **cmssw_2024_param_HLT,
)
class CleaningTaskMuMu2024(EmbeddingTask):

    RequiredTask = SelectionTask2024
    
    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuMu/cleaning/{self.branch}_cleaning_{self.output_file_suffix()}.root")

    def run(self):
        """Run the cleaning cmsdriver command"""
        self.run_cms_driver(
            step="USER:TauAnalysis/MCEmbeddingTools/LHE_USER_cff.embeddingLHEProducerTask,RAW2DIGI,RECO",
            processName="LHEembeddingCLEAN",
            data=True,
            scenario="pp",
            conditions="140X_dataRun3_v20",
            era="Run3_2024",
            eventcontent="TauEmbeddingCleaning",
            datatier="RAWRECO",
            procModifiers="tau_embedding_cleaning,tau_embedding_mu_to_mu",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )
        
@default_param(
    htcondor_walltime="4800",
    htcondor_request_cpus="8",
    htcondor_request_memory="8GB",
    htcondor_request_disk="10GB",
    emb_files_per_job=3,
    **condor_2024_param,
    **cmssw_2024_param_HLT,
)
class GenSimTaskMuMu2024(EmbeddingTask):
    
    RequiredTask = CleaningTaskMuMu2024

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuMu/gensim/{self.branch}_gensim_{self.output_file_suffix()}.root")

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
            conditions="140X_mcRun3_2024_realistic_v26", # same Global Tag as in HLTSimTask!
            eventcontent="TauEmbeddingSimGen",
            datatier="RAWSIM",
            procModifiers="tau_embedding_sim,tau_embedding_mu_to_mu",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )

@default_param(
    htcondor_walltime="7800",
    htcondor_request_cpus="4",
    htcondor_request_memory="4GB",
    htcondor_request_disk="20GB",
    **condor_2024_param,
    **cmssw_2024_param_HLT,
)
class HLTSimTaskMuMu2024(EmbeddingTask):

    RequiredTask = GenSimTaskMuMu2024

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuMu/hltsim/{self.branch}_hltsim_{self.output_file_suffix()}.root")

    def run(self):
        """Run the hlt cmsdriver command"""
        # step and conditions taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 (see recomended sample: https://twiki.cern.ch/twiki/bin/viewauth/CMS/MuonRun32022#MC)
        self.run_cms_driver(
            step="HLT:2024v14+TauAnalysis/MCEmbeddingTools/Simulation_HLT_customiser_cff.embeddingHLTCustomiser",
            processName="SIMembeddingHLT",
            mc=True,
            beamspot="DBrealistic",
            geometry="DB:Extended",
            era="Run3_2024",
            conditions="140X_mcRun3_2024_realistic_v26",
            eventcontent="TauEmbeddingSimHLT",
            datatier="RAWSIM",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )

@default_param(
    htcondor_walltime="6700",
    htcondor_request_cpus="2",
    htcondor_request_memory="4GB",
    htcondor_request_disk="20GB",
    **condor_2024_param,
    **cmssw_2024_param_HLT,
)
class RecoSimTaskMuMu2024(EmbeddingTask):
    
    RequiredTask = HLTSimTaskMuMu2024

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuMu/recosim/{self.branch}_recosim_{self.output_file_suffix()}.root")

    def run(self):
        """Run the reco cmsdriver command"""
        self.run_cms_driver(
            step="RAW2DIGI,L1Reco,RECO,RECOSIM",
            processName="SIMembedding",
            mc=True,
            beamspot="DBrealistic",
            geometry="DB:Extended",
            era="Run3_2024",
            conditions="140X_mcRun3_2024_realistic_v26",
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
    htcondor_request_disk="1GB",
    emb_files_per_job=5,
    **condor_2024_param,
    **cmssw_2024_param_15,
)
class MergingTaskMuMu2024(EmbeddingTask):
    
    RequiredTask = RecoSimTaskMuMu2024

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuMu/merging/{self.branch}_merging_{self.output_file_suffix()}.root")

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            step="USER:TauAnalysis/MCEmbeddingTools/Merging_USER_cff.merge_step,PAT",
            processName="MERGE",
            data=True,
            conditions="140X_dataRun3_v17",
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
    emb_files_per_job=2,
    **condor_2024_param,
    **cmssw_2024_param_15,
)
class NanoAODTaskMuMu2024(EmbeddingTask):
    
    RequiredTask = MergingTaskMuMu2024

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024/MuMu/nanoaod/{self.branch}_nanoaod_{self.output_file_suffix()}.root")

    def run(self):
        """Run the merging cmsdriver command"""
        self.run_cms_driver(
            step="NANO:@TauEmbedding",
            data=True,
            conditions="140X_dataRun3_v20",
            era="Run3_2024",
            eventcontent="TauEmbeddingNANOAOD",
            datatier="NANOAODSIM",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )
