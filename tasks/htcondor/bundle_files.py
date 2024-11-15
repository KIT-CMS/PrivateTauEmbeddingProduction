import os

import law
import luigi

law.contrib.load("tasks", "cms", "wlcg", "git")


class BundleRepo(law.git.BundleGitRepository, law.tasks.TransferLocalFile):
    """
    Bundle a git repository and transfer it to a remote location.
    Therefore it inherits from the BundleGitRepository and TransferLocalFile tasks.
    """
    
    replicas = luigi.IntParameter(
        default=1,
        description="number of replicas to generate; default: 1 ",
    )
    """ number of replicas to generate for the bundle to allow concurrent access """
    
    exclude_files = ["tmp", "*~", "*.pyc", ".vscode/"]
    """ list of files to exclude from the bundle, additional to the ones in the .gitignore file """
    
    # Don't know what this is for
    # Set it to None as in the example
    version = None
    task_namespace = None

    def get_repo_path(self):
        """
        Path to the git repository that should be bundled.
        This is required by the BundleGitRepository task.
        """
        return os.environ["BASE_DIR"]

    def single_output(self):
        """
        Defines the filename of one of the output `.tgz` file.
        It needs the checksum of the repository to be unique.
        It is also used to infer names and locations of replicas
        This is required by the TransferLocalFile task.
        """
        repo_base = os.path.basename(self.get_repo_path())
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
        return law.tasks.TransferLocalFile.output(self)

    @law.decorator.safe_output
    def run(self):
        """
        create the bundle
        if no `path=` parameter is given 
        `is_tmp` is a flag to create a temporary file with `.tgz` as file extension
        the location of the file is determined by the tmp_dir parameter or in the config
        https://law.readthedocs.io/en/stable/config.html#target
        """
        
        bundle = law.LocalFileTarget(is_tmp="tgz")
        self.bundle(bundle)

        # log the size
        self.publish_message(
            "bundled repository archive, size is {}".format(
                law.util.human_bytes(bundle.stat().st_size, fmt=True),
            )
        )

        # transfer the bundle
        self.transfer(bundle)


class BundleCMSSW(law.cms.BundleCMSSW, law.tasks.TransferLocalFile):
    """
    
    """
    
    replicas = luigi.IntParameter(
        default=1,
        description="number of replicas to generate; default: 1",
    )
    """ number of replicas to generate for the bundle to allow concurrent access """

    # Don't know what this is for
    # Set it to None as in the example
    version = None
    task_namespace = None
    
    
    exclude = "^src/tmp"
    """ Regular expression for excluding files or directories relative to CMSSW_BASE. """

    def get_cmssw_path(self):
        """ Returns the path to the CMSSW directory."""
        return os.environ["CMSSW_DIR"]

    def single_output(self):
        """
        Defines the filename of one of the output `.tgz` file.
        It needs the checksum of the repository to be unique.
        It is also used to infer names and locations of replicas
        This is required by the TransferLocalFile task.
        """
        path = "{}.{}.tgz".format(
            os.path.basename(self.get_cmssw_path()), self.checksum
        )
        return law.wlcg.WLCGFileTarget(os.path.join("bundles", path))

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
        return law.tasks.TransferLocalFile.output(self)

    def run(self):
        """
        create the bundle
        if no `path=` parameter is given 
        `is_tmp` is a flag to create a temporary file with `.tgz` as file extension
        the location of the file is determined by the tmp_dir parameter or in the config
        https://law.readthedocs.io/en/stable/config.html#target
        """
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
