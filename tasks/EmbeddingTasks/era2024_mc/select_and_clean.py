import law
import luigi
from tasks.EmbeddingTasks import EmbeddingTask
from tasks.htcondor.cmssw import ETP_CMSSW_HTCondorWorkflow
from tasks.htcondor.htcondor import default_param

logger = law.logger.get_logger(__name__)

#
# CMSSW versions:
#  - CMSSW_13_0_17: Use the CMSSW version used in the ReReco campaign: https://cms-pdmv-prod.web.cern.ch/rereco/requests?input_dataset=/Muon/Run2022G-v1/RAW&shown=127&page=0&limit=50
#  - CMSSW_12_4_11_patch3: The CMSSW version used in MC production for 2022 DY samples  Taken from https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup/EGM-Run3Summer22EEDRPremix-00004 from this chain https://cms-pdmv-prod.web.cern.ch/mcm/chained_requests?prepid=EGM-chain_Run3Summer22EEwmLHEGS_flowRun3Summer22EEDRPremix_flowRun3Summer22EEMiniAODv4_flowRun3Summer22EENanoAODv12-00001&page=0&shown=15
default_2024_mc_param = {
    "htcondor_accounting_group": "cms.higgs",
    "htcondor_container_image": "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/cms:rhel8-m",
    "lcg_stack": "/cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/setup.sh",
    "retries": 3,
    "git_cmssw_hash": "2103a99ae6a",
    "cmssw_version": "CMSSW_15_1_0_pre6",
    "cmssw_branch": "embedding_mc_CMSSW_15_1_X",
    "cmssw_scram_arch": "el8_amd64_gcc12",
}

@default_param(
    htcondor_walltime="6200", # = 1h 43min
    htcondor_request_cpus="8",
    htcondor_request_memory="6GB",
    htcondor_request_disk="2GB",
    emb_filelist="artur_2024_mc_prod.filelist",
    emb_number_of_events=-1,
    **default_2024_mc_param,
)
class SelectionTask2024MC(ETP_CMSSW_HTCondorWorkflow, law.LocalWorkflow):
    """This class is the first step in the embedding workflow. Therfore can't inherit from EmbeddingTask"""

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
            step="RAW2DIGI,L1Reco,RECO,PAT,FILTER:TauAnalysis/MCEmbeddingTools/Selection_FILTER_cff.makePatMuonsZmumuSelection",
            processName="SELECT",
            mc=True,
            geometry="DB:Extended",
            conditions="auto:phase1_2024_realistic",
            era="Run3_2024",
            eventcontent="TauEmbeddingSelection",
            datatier="RAWRECO",
            filein=f"root://cmsdcache-kit-disk.gridka.de:1094/{self.branch_data}",
            number=self.emb_number_of_events,
        )


@default_param(
    htcondor_walltime="4800",
    htcondor_request_cpus="8",
    htcondor_request_memory="6GB",
    htcondor_request_disk="10GB",
    **default_2024_mc_param,
)
class CleaningTaskTauTau2024MC(EmbeddingTask):

    RequiredTask = SelectionTask2024MC

    def output(self):
        """The path to the files the cmsdriver command is going to create"""
        return law.wlcg.WLCGFileTarget(f"2024_mc/cleaning/{self.branch}_cleaning.root")

    def run(self):
        """Run the cleaning cmsdriver command"""
        self.run_cms_driver(
            step="USER:TauAnalysis/MCEmbeddingTools/LHE_USER_cff.embeddingLHEProducerTask,RAW2DIGI,RECO",
            processName="LHEembeddingCLEAN",
            mc=True,
            geometry="DB:Extended",
            conditions="auto:phase1_2024_realistic",
            era="Run3_2024",
            eventcontent="TauEmbeddingCleaning",
            datatier="RAWRECO",
            procModifiers="tau_embedding_cleaning",
            filein=",".join(self.get_input_files()),
            number=self.emb_number_of_events,
        )
