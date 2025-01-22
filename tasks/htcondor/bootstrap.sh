#!/usr/bin/env bash

# Bootstrap file that is executed by remote jobs submitted by law to set up the environment.
# So-called render variables, denoted by "{{name}}", are replaced with variables
# configured in the remote workflow tasks, e.g. in HTCondorWorkflow.htcondor_job_config(), upon job

######## set env variables ########
# set executing user as this is often used to specify the output directory (e.g. in the law.cfg) 
export USER="{{user}}"
# set the base directory where the repo and cmssw are stored
export JOB_DIR="${PWD}"
export BASE_DIR="${PWD}/repo"

# load the repo bundle
(
    mkdir -p "${BASE_DIR}"
    cd "${BASE_DIR}" || exit "$?"
    (
        # source the law wlcg tools, mainly for law_wlcg_get_file
        source "${JOB_DIR}/{{wlcg_tools}}" || return "$?"

        # to get gfal-* executables if not available, source the lcg dir
        # if ! law_wlcg_check_executable "gfal-ls"; then
        #     source "{{grid_stack}}" || return "$?"
        # fi
        # download the repo bundle
        law_wlcg_get_file "{{repo_uris}}" '{{repo_pattern}}' "${BASE_DIR}/repo.tgz" || return "$?"
    )
    # extract the repo bundle
    tar -xzf "repo.tgz" || return "$?"
    rm -f "repo.tgz"
    echo "Analysis repo setup done."
)
# load the cmssw bundle
(
    source /cvmfs/cms.cern.ch/cmsset_default.sh
    mkdir -p "${BASE_DIR}"
    cd "${BASE_DIR}" || exit "$?"
    export SCRAM_ARCH="{{cmssw_scram_arch}}"
    cmsrel "{{cmssw_version}}"
    cd "{{cmssw_version}}" || exit "$?"
    (
        # source the law wlcg tools, mainly for law_wlcg_get_file
        source "${JOB_DIR}/{{wlcg_tools}}" || return "$?"

        # to get gfal-* executables if not available, source the lcg dir
        # if ! law_wlcg_check_executable "gfal-ls"; then
        #     source "{{grid_stack}}" || return "$?"
        # fi
        # download the cmssw bundle
        law_wlcg_get_file "{{cmssw_uris}}" '{{cmssw_pattern}}' "${PWD}/cmssw.tgz" || return "$?"
    )
    # extract the cmssw bundle
    tar -xzf "cmssw.tgz" || return "$?"
    rm -f "cmssw.tgz"
    cd "src" || return "$?"
    eval "$( scramv1 runtime -sh )"
    scram b
    echo "CMSSW setup done."
    echo "export CMSSW env"
    env > "${BASE_DIR}/cmssw.env"
)

cd "${BASE_DIR}" || return "$?"
# source some lcg stack to get the right python version
echo "Sourcing the lcg stack"
source "{{lcg_stack}}" || return "$?"
# source the repo setup
echo "Sourcing the setup.sh"
source "${BASE_DIR}/setup.sh" || return "$?"
return "0"
