import law
import luigi

from ..htcondor.cmssw import ETP_CMSSW_HTCondorWorkflow


class EmbeddingTask(ETP_CMSSW_HTCondorWorkflow, law.LocalWorkflow):
    """This class combines common functionality for embedding Tasks"""

    emb_number_of_events = luigi.Parameter(
        default="-1",
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
    """These parameters are not overwritten by the reqs() method."""

    def create_branch_map(self):
        """Maps `self.emb_files_per_job number` of files to one job using the get_all_branch_chunks method from law."""
        branch_chunks = self.RequiredTask().get_all_branch_chunks(
            self.emb_files_per_job
        )
        return {i: chunk for i, chunk in enumerate(branch_chunks)}

    def workflow_requires(self):
        """Requires the RequiredTask"""
        return {"files": self.RequiredTask()}

    def requires(self):
        """What the single jobs require to run, is in this case the RequiredTask jobs which provide the files specified in branch_data"""
        return self.RequiredTask()

    def get_input_files(self):
        """Get the input files from the RequiredTask output"""
        # get the input files from the output of the PreselectionTask
        # this is a bit complicated as the output of a Workflow Task is in a dictionarry with the key 'collection'
        # and the value is a law.SiblingFileCollection
        all_files = [
            wlcg_target.uri()
            for wlcg_target in self.input()["collection"].targets.values()
        ]
        return [all_files[i] for i in self.branch_data]

