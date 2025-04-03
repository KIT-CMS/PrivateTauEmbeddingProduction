import os
import subprocess

import law
import luigi
from law.util import interruptable_popen, quote_cmd, rel_path

from .bundle_files import BundleRepo
from .htcondor import ETP_HTCondorWorkflow

law.contrib.load("tasks", "cms", "wlcg", "git")

logger = law.logger.get_logger(__name__)


class BundleCMSSWTask(
    ETP_HTCondorWorkflow, law.cms.BundleCMSSW, law.tasks.TransferLocalFile
):
    """ """

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
    # htcondor_walltime = luigi.Parameter(
    #     description="Requested walltime for the jobs.",
    #     default="1200",
    # )
    # htcondor_request_cpus = luigi.Parameter(
    #     description="Number of CPU cores to request for each job.",
    #     default="4",
    # )
    # htcondor_request_memory = luigi.Parameter(
    #     description="Amount of memory to request for each job.",
    #     default="5000",
    # )
    # htcondor_request_disk = luigi.Parameter(
    #     description="Amount of disk scratch space to request for each job.",
    #     default="1000",
    # )
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

    def htcondor_job_config(self, config, job_num, branches):
        config = super().htcondor_job_config(config, job_num, branches)
        config.render_variables["cmssw_version"] = self.cmssw_version
        config.render_variables["cmssw_branch"] = self.cmssw_branch
        config.render_variables["cmssw_scram_arch"] = self.cmssw_scram_arch
        config.render_variables["n_compile_cores"] = self.htcondor_request_cpus
        return config


class ETP_CMSSW_HTCondorWorkflow(ETP_HTCondorWorkflow):

    # we need to set the checksum manually as the CMSSW repo is not there yet when the checksum is calculated.
    # Disableing the checksumming leads to other problems, as the checksum is used to create the bundle file name.
    git_cmssw_hash = luigi.Parameter(
        default="",
        description="git hash of the cmssw repository. This is only used to set a unique name for the bundle file.",
    )
    cmssw_version = luigi.Parameter(
        description="CMSSW version to bundle.",
    )

    cmssw_branch = luigi.Parameter(
        description="The CMSSW git branch to use with the chosen cmssw version",
    )
    
    cmssw_scram_arch = luigi.Parameter(
        description="CMSSW scram architecture.",
    )
    tolerance = luigi.FloatParameter(
        default=0.5,
        description="number of failed tasks to still consider the task successful; relative "
        "fraction (<= 1) or absolute value (> 1); default: 0.0",
    )
    
    exclude_params_req = set(
        [
            "emb_files_per_job",
            "retries",
            "htcondor_walltime",
            "htcondor_request_cpus",
            "htcondor_request_memory",
            "htcondor_request_disk",
        ]
    )
    """These parameters are not overwritten by the reqs() method."""

    def htcondor_workflow_requires(self):
        """Adds the repo and software bundling as requirements"""
        reqs = super().htcondor_workflow_requires()
        
        custom_tag=self.cmssw_branch
        if self.git_cmssw_hash:
            custom_tag += f".{self.git_cmssw_hash}"
            
        # add repo and software bundling as requirements when getenv is not requested
        reqs["cmssw"] = BundleCMSSWTask.req(
            self,
            custom_checksum=custom_tag,
        )
        return reqs

    def htcondor_job_config(self, plain_config, job_num, branches):
        """
        Add cmssw related variables like the version and the path to the htcondor job config
        which should be rendered in the bootrap script
        """
        config = super().htcondor_job_config(plain_config, job_num, branches)

        reqs = self.htcondor_workflow_requires()

        # add cmssw bundle variables:
        # the path to the bundled CMSSW directory archives e.g.: 'root://cmsdcache-kit-disk.gridka.de:1094//store/user/<user>/run3_embedding/bundles'
        config.render_variables["cmssw_uris"] = ",".join(
            reqs["cmssw"].single_output().parent.uri(return_all=True)
        )
        # the pattern for the filenames with a star as placeholder e.g.: CMSSW_14_2_0_pre3.7e6ac64.*.tgz
        config.render_variables["cmssw_pattern"] = os.path.basename(
            reqs["cmssw"].get_file_pattern()
        )
        config.render_variables["cmssw_version"] = self.cmssw_version
        config.render_variables["cmssw_scram_arch"] = self.cmssw_scram_arch

        return config

    def run_cmssw_shell(self, cmd):
        """Run a shell command in the CMSSW environment."""
        # load and parse env file created in bootstrap script
        env_file = os.path.join(os.environ["BASE_DIR"], "cmssw.env")
        env = {}
        with open(env_file, "r") as f:
            previous_line_key = ""
            for line in f:
                # some env variables (like BASH_FUNC_cmsrel and BASH_FUNC_cmsenv) span multiple lines
                # and therefore cause problems when parsing the env file line by line.
                if line.startswith("}"):
                    # add a ";" to end the bash command in the brevious line
                    env[previous_line_key] = env[previous_line_key] + "; " + line
                    continue
                key, value = line.strip().split("=", 1)
                env[key] = value
                previous_line_key = key

        if type(cmd) == str:
            cmd = [cmd]
        # run the command in the CMSSW environment
        code, out, err = interruptable_popen(
            cmd, shell=True, executable="/bin/bash", stdout=subprocess.PIPE, env=env
        )

        if code != 0:
            logger.error(f"output: {out}\ncode: {code}\nerror: {err}")
            raise Exception(f"executing the cmsdriver command failed: {cmd}")
        return code, out, err

    def run_cms_driver(self, process_name, **kwargs):
        """Run a cmsDriver.py command in the CMSSW environment and copy the output file to the remote location specified in output().

        :param process_name: Name of the process to run or the python configuration file to run.
        :type process_name: str
        :param kwargs: Arguments to pass to the cmsDriver.py command. Either the data or mc argument must be set to True.
            - If the data argument is set to True, the allowed arguments are:
                - step
                - data
                - scenario
                - conditions
                - era
                - eventcontent
                - datatier
                - customise
                - filein
                - fileout
                - number.
            - If the mc argument is set to True, the allowed arguments are:
                - step
                - mc
                - beamspot
                - geometry
                - era
                - conditions
                - eventcontent
                - datatier
                - customise
                - customise_commands
                - filein
                - fileout
                - number.

            The fileout argument is ignored and the path specified in the output() method is used.
        """
        # define allowed arguments for the data and mc cmsDriver.py commands
        allowed_data_args = [
            "step",
            "data",
            "scenario",
            "conditions",
            "era",
            "eventcontent",
            "datatier",
            "customise",
            "customise_commands",
            "filein",
            "fileout",
            "number",
        ]
        allowed_mc_args = [
            "step",
            "mc",
            "beamspot",
            "geometry",
            "era",
            "conditions",
            "eventcontent",
            "datatier",
            "customise",
            "customise_commands",
            "filein",
            "fileout",
            "number",
        ]
        # some checks that mc and data are not set at the same time
        if kwargs.get("data") == None and kwargs.get("mc") == None:
            raise ValueError("Either the data or mc argument must be set to True.")

        if kwargs.get("data") == True and kwargs.get("mc") == True:
            raise ValueError("Only one of the data or mc argument can be set to True.")

        if kwargs.get("fileout") != None:
            logger.warning(
                "The fileout argument is ignored and the path specified in the output() method is used."
            )
            del kwargs["fileout"]

        # build the cmsDriver.py command and check depending of the data or mc argument if the arguments are allowed
        cmd = f"cmsDriver.py {process_name}"
        if kwargs.get("data") == True:
            allowed_args = allowed_data_args
            cmd += " --data"
            kwargs.pop("data")
        elif kwargs.get("mc") == True:
            allowed_args = allowed_mc_args
            cmd += " --mc"
            kwargs.pop("mc")
            
        for key, value in kwargs.items():
            if key in allowed_args:
                cmd += f" --{key} {value}"
            else:
                raise ValueError(
                    f"Argument {key} is not allowed or implemented in the cmsdriver command."
                )
        # define a temporary local output file
        local_output = os.path.join(os.environ["BASE_DIR"], "cmsdriver_output.root")
        cmd += f" --fileout file:{local_output}"
        
        # Specify the output python configuration file. This is then called by cmsRun.
        # This is needed, because we want to run multithreaded and the --nThreads argument in cmsDriver.py is not working.
        cmd += f" --python_filename cmsdriver_config.py --no_exec"
        
        logger.info(f"Running cmsDriver.py command: {cmd}")
        code, out, err = self.run_cmssw_shell(cmd)
        logger.info(f"cmsDriver.py output:\n{out}")
        
        # run the cmsRun command with the output python configuration file
        logger.info(f"Running cmsRun with {self.htcondor_request_cpus} cores")
        code, out, err = self.run_cmssw_shell(f"cmsRun -n {self.htcondor_request_cpus} cmsdriver_config.py")
        logger.info(f"cmsRun output:\n{out}")
        
        logger.info("Copying output file to the remote location specified in output()")
        # try to copy to the remote location and retry 10 times while waiting 5min between.
        # see https://github.com/riga/law/blob/0f619e5b556e04119e843a5c149afbd74d5d9d54/law/contrib/gfal/target.py#L332
        # and https://github.com/riga/law/blob/0f619e5b556e04119e843a5c149afbd74d5d9d54/law/target/remote/interface.py#L84
        self.output().copy_from_local(local_output, retries=10, retry_delay=5 * 60)
        logger.info(f"Output file copied")
