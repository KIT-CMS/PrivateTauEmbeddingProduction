import os

import law
import luigi
from law.util import interruptable_popen, quote_cmd, rel_path


law.contrib.load("tasks", "singularity", "cms", "wlcg", "git")

logger = law.logger.get_logger(__name__)

class BundleCMSSWTask(
    law.SandboxTask, law.cms.BundleCMSSW, law.tasks.TransferLocalFile
):
    """ """
    sandbox = luigi.Parameter(
        default="",
        description="path to a sandbox file to be used for the job; default: ''",
    )
    singularity_args = lambda x: ["-B", "/cvmfs", "-B", os.getcwd(), "-B", "/home/cwinter/.globus/x509up"]
    def sandbox_post_setup_cmds(self):
        bootstrap_file =  law.util.rel_path(__file__, "setup_cmssw.sh")
        return [
            "export X509_USER_PROXY=/home/cwinter/.globus/x509up",
            f"source {bootstrap_file} {self.cmssw_version} {self.cmssw_branch} {self.cmssw_scram_arch} {self.n_compile_cores}"
        ]

    # replicas = luigi.IntParameter(
    #     default=1,
    #     description="number of replicas to generate; default: 1",
    # )
    # """ number of replicas to generate for the bundle to allow concurrent access """
    cmssw_version = luigi.Parameter(
        description="CMSSW version to bundle.",
    )

    cmssw_branch = luigi.Parameter(
        description="The CMSSW git branch to use with the chosen cmssw version",
    )

    cmssw_scram_arch = luigi.Parameter(
        description="CMSSW scram architecture.",
    )
    n_compile_cores = luigi.IntParameter(
        default=4,
        description="Number of cores to use for compiling CMSSW.",
    )
    
    # Don't know what this is for
    # Set it to None as in the example
    version = None
    task_namespace = None

    exclude_params_req = set(
        [
            "replicas",
            "htcondor_walltime",
            "htcondor_request_cpus",
            "htcondor_request_memory",
            "htcondor_request_disk",
        ]
    )
    """These parameters are not overwritten by the BundleCMSSWTask.reqs() method."""

    exclude = "^src/tmp"
    """ Regular expression for excluding files or directories relative to CMSSW_BASE. """

    def htcondor_bootstrap_file(self):
        """Path to the bootstrap file thats used to setup the environment in the docker containers."""
        bootstrap_file = law.util.rel_path(__file__, "setup_cmssw.sh")
        return law.JobInputFile(bootstrap_file, share=True, render_job=True)

    def get_cmssw_path(self):
        """Returns the path to the CMSSW directory."""
        return os.path.join(os.environ["BASE_DIR"], self.cmssw_version)

    def single_output(self):
        """
        Defines the filename of one of the output `.tgz` file.
        It needs the checksum of the repository to be unique.
        It is also used to infer names and locations of replicas
        This is required by the TransferLocalFile task.
        """

        repo_base = os.path.basename(self.get_cmssw_path())
        path = os.path.join("bundles", f"{repo_base}.{self.checksum}.tgz")
        return law.wlcg.WLCGFileTarget(path)

    def get_file_pattern(self):
        """
        Get the pattern for the path to the different replicas.
        Output is a str in the form of `/path/to/bundles/repo.*tgz`.
        This is used in the HTCondorWorkflow task to calculate the repo_pattern and cmssw_pattern.
        """
        path = os.path.expandvars(os.path.expanduser(self.single_output().abspath))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        """
        Both parent classes define this method,
        so we need to set it manually to the TransferLocalFile output function.
        """
        output = law.tasks.TransferLocalFile.output(self)
        return output

    def create_branch_map(self):
        return {0: 0}

    def run(self):
        """
        create the bundle
        if no `path=` parameter is given
        `is_tmp` is a flag to create a temporary file with `.tgz` as file extension
        the location of the file is determined by the tmp_dir parameter or in the config
        https://law.readthedocs.io/en/stable/config.html#target
        """
        # setup cmssw
        # cmd = [rel_path(__file__, "setup_cmssw.sh"), self.cmssw_version]
        # logger.warning(f"contents of {rel_path(__file__)}: {os.listdir(rel_path(__file__))}")
        # logger.warning(f"running {cmd}")
        # logger.warning(f"Base Path {os.environ['BASE_DIR']} contents: {os.listdir(os.environ['BASE_DIR'])}")
        # code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash", stdout=subprocess.PIPE)
        # if code != 0:
        #     logger.error(f"output and code: {out}, {code}")
        #     logger.warning(f"contents of {rel_path(__file__)}: {os.listdir(rel_path(__file__))}")
        #     logger.warning(f"running {cmd}")
        #     logger.warning(f"Base Path {os.environ['BASE_DIR']} contents: {os.listdir(os.environ['BASE_DIR'])}")
        #     raise Exception("setup of cmssw failed: {}".format(out))
        # create the bundle
        bundle = law.LocalFileTarget(is_tmp="tgz")
        self.bundle(bundle)

        # log the size
        self.publish_message(
            "bundled CMSSW archive, size is {}".format(
                law.util.human_bytes(bundle.stat().st_size, fmt=True),
            )
        )

        # transfer the bundle
        self.transfer(bundle)
