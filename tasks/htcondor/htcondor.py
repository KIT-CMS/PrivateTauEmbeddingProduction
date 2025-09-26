import os
from abc import abstractmethod

import law
import luigi

from .bundle_files import BundleRepo

law.contrib.load("htcondor")
logger = law.logger.get_logger(__name__)

class ETP_HTCondorWorkflow(law.htcondor.HTCondorWorkflow):
    """
    Abstract class for ETP HTCondor workflows. It extends the HTCondorWorkflow class from law.
    The following parameters need to be set in the derived class:
    
    htcondor_accounting_group: 
        ETP HTCondor accounting group jobs are submitted to.
    htcondor_container_image: 
        Docker image to use for running docker jobs.
    htcondor_walltime: 
        Requested walltime for the jobs.
    htcondor_request_cpus: 
        Number of CPU cores to request for each job.
    htcondor_request_memory: 
        Amount of memory to request for each job.
    htcondor_request_disk: 
        Amount of disk scratch space to request for each job.
    lcg_stack: 
        LCG stack to use for the job.
    
    The following parameters have a default value and are not expected to be changed for most workflows:
    
    htcondor_requirements: 
        Additional requirements on e.g. the target machines to run the jobs.
        default="(TARGET.ProvidesCPU)&&(TARGET.ProvidesIO)"
    htcondor_remote_job: 
        ETP HTCondor specific flag to allow jobs to run on remote resources (NEMO, TOPAS).
        default="True"
        
    The following methods need to be implemented in the derived class:
    
    
    """
    # These can adjusted to the needs of the specific workflow
    htcondor_accounting_group = luigi.Parameter(
        description="ETP HTCondor accounting group jobs are submitted to.",
    )
    htcondor_container_image = luigi.Parameter(
        description="Docker image to use for running docker jobs.",
    )
    htcondor_walltime = luigi.Parameter(
        description="Requested walltime for the jobs.",
    )
    htcondor_request_cpus = luigi.Parameter(
        description="Number of CPU cores to request for each job.",
    )
    htcondor_request_memory = luigi.Parameter(
        description="Amount of memory to request for each job.",
    )
    htcondor_request_disk = luigi.Parameter(
        description="Amount of disk scratch space to request for each job.",
    )
    lcg_stack = luigi.Parameter(
        description="LCG stack to use for the job.",
    )
    # These htcondor parameters are not expected to be changed (default for ETP HTCondor)
    htcondor_requirements = luigi.Parameter(
        default='(TARGET.ProvidesCPU)&&(TARGET.ProvidesIO)',
        significant=False, # makes it not show up in the task representation
        description="Additional requirements on e.g. the target machines to run the jobs.",
    )
    htcondor_remote_job = luigi.Parameter(
        default="True",
        # significant=False, # makes it not show up in the task representation
        description="ETP HTCondor specific flag to allow jobs to run on remote resources (NEMO, TOPAS).",
    )
    
    # set Law options
    # output_collection_cls = law.SiblingFileCollection # is this needed? It's not used by law at all
    create_branch_map_before_repr = True
    """The branch map should be created before the task representation is created via :py:meth:`repr`."""

    def htcondor_output_directory(self):
        """location of submission output files, such as the json files containing job data"""
        job_dir = law.config.get_expanded("job", "job_file_dir")
        return law.LocalDirectoryTarget(f"{job_dir}/{self.task_id}/")
    
    def htcondor_create_job_file_factory(self):
        path = self.htcondor_log_directory().abspath
        factory = super().htcondor_create_job_file_factory(dir=path, mkdtemp=False)
        logger.warn(f"HTCondor job directory is: {path}")
        return factory
    
    def htcondor_log_directory(self):
        """Creates a log directory in the output directory, else the logs would be in some tmp directory with a random name"""
        log_path = os.path.join(self.htcondor_output_directory().abspath, "logs")
        return law.LocalDirectoryTarget(log_path)

    def htcondor_bootstrap_file(self):
        """Path to the bootstrap file thats used to setup the environment in the docker containers."""
        bootstrap_file = law.util.rel_path(__file__, "bootstrap.sh")
        return law.JobInputFile(bootstrap_file, share=True, render_job=True)

    def htcondor_workflow_requires(self):
        """Adds the repo and software bundling as requirements """
        reqs = super().htcondor_workflow_requires()

        # add repo and software bundling as requirements when getenv is not requested
        reqs["repo"] = BundleRepo.req(self, _exclude=["custom_checksum"]) # exclude custom_checksum, which is used for cmssw bundles
        return reqs

    def htcondor_job_config(self, config, job_num, branches):
        """"""
        # Names of the log, stdout and stderr files
        config.log = os.path.join(self.htcondor_log_directory().abspath, "Log.txt")
        config.stdout = os.path.join(self.htcondor_log_directory().abspath, "Output.txt")
        config.stderr = os.path.join(self.htcondor_log_directory().abspath, "Error.txt")
        config.universe = "container"
        
        config.custom_content = [
            ("accounting_group", self.htcondor_accounting_group),
            ("stream_error", "True"),  # Remove before commit,
            ("stream_output", "True"),  #,
            ("Requirements", self.htcondor_requirements),
            ("+RemoteJob", self.htcondor_remote_job),
            ("container_image", self.htcondor_container_image),
            ("+RequestWalltime", self.htcondor_walltime),
            ("x509userproxy", law.wlcg.get_vomsproxy_file()),
            ("request_cpus", self.htcondor_request_cpus),
            ("RequestMemory", self.htcondor_request_memory),
            ("RequestDisk", self.htcondor_request_disk),
            ("JobBatchName", self.task_id),
        ]

        # include the wlcg specific tools script in the input sandbox
        config.input_files["wlcg_tools"] = law.JobInputFile(
            law.util.law_src_path("contrib/wlcg/scripts/law_wlcg_tools.sh"),
            share=True,
            render=False
        )

        # add render variables to render the bootstrap script with the correct paths
        
        def get_bundle_info(task):
            """Extracts the filepath and filepattern of the bundle files from the bundle tasks.
            Taken from https://github.com/columnflow/columnflow/blob/master/columnflow/tasks/framework/remote.py#L388
            """
            # the path to the bundled CMSSW directory archives e.g.: 
            # 'root://cmsdcache-kit-disk.gridka.de:1094//store/user/<user>/run3_embedding/bundles'
            uris = task.output().dir.uri(return_all=True)
            # the pattern for the filenames with a star as placeholder e.g.: CMSSW_14_2_0_pre3.7e6ac64.*.tgz
            pattern = os.path.basename(task.get_file_pattern())
            return ",".join(uris), pattern
        
        reqs = self.htcondor_workflow_requires()

        # add repo bundle variables
        uris, pattern = get_bundle_info(reqs["repo"])
        config.render_variables["repo_uris"] = uris
        config.render_variables["repo_pattern"] = pattern
        config.render_variables["user"] = os.environ["USER"]
        config.render_variables["lcg_stack"] = self.lcg_stack

        return config
