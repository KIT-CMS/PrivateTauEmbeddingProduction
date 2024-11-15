import math

import law
import luigi
from luigi.util import inherits

from ..htcondor.htcondor import ETPHTCondorWorkflow

# logger = law.logger.get_logger(__name__) 

class CreateNumberFiles(ETPHTCondorWorkflow, law.LocalWorkflow):
    """
    Creates <number> files with a the <number> in them. 
    for more info on law workflows see: https://law.readthedocs.io/en/latest/workflows.html
    """
     
    number = luigi.IntParameter()

    def create_branch_map(self):
        return {i: i for i in range(self.number)}

    def output(self):
        return law.wlcg.WLCGFileTarget(f"test_{self.branch}.txt")

    def run(self):
        self.output().dump(f"{self.branch + 1}")


@inherits(CreateNumberFiles)
class CollectNumbers(ETPHTCondorWorkflow, law.LocalWorkflow):
    """
    Sum up the numbers created in the CreateNumberFiles tasks.
    Depending on the number of branches specified in num_branches, the numbers are summed up in chunks.
    """
    num_branches = luigi.IntParameter(default=2)

    def workflow_requires(self):
        """
        Specifies the requirements for this Workflow task in general.
        The requirements for a single branch are specified in the requires method.
        """
        return {"files": CreateNumberFiles.req(self)}

    def requires(self):
        """ 
        Which tasks are required to run this task
        Of course we need to set the branch parameter to -1 as we require all branches
        This branch only depends on branches specified in branch_data (either [0, 1, 2, 3, 4] or [5, 6, 7, 8, 9]).
        """
        return CreateNumberFiles.req(self, branch=-1, branches=self.branch_data)

    def create_branch_map(self):
        """ Creates a branch_map that showes the number of branches needed for this task and maps the branch_data to the branches """
        # if number=10 and num_branches=2 returns: [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
        branch_chunks = CreateNumberFiles.req(self).get_all_branch_chunks(math.ceil(self.number/self.num_branches))
        # This branch_map sets the branch_data for each branch (either [0, 1, 2, 3, 4] or [5, 6, 7, 8, 9])
        return {i: chunk for i, chunk in enumerate(branch_chunks)}
    
    def output(self):
        """
        This is the file target the job creates.
        This depends on the branch of the workflow task.
        """
        return law.wlcg.WLCGFileTarget(f"sum_{self.branch}.txt")

    def run(self):
        """Creates the sum of all the numbers in the files this Workflow branch depends on."""
        sum = 0
        # self.input()["collection"] returns the FileCollection of the FileTargets created by the CreateNumberFiles tasks which are required by this task.
        # self.input()["collection"].targets returns an OrderedDict with the branch as key and the output FileTarget as value.
        # So we loop over all the values of the OrderedDict to get the FileTargets and sum up the content of the files.
        for f in self.input()["collection"].targets.values():
            sum += int(f.load())
        self.output().dump(str(sum))
