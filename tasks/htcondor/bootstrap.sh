#!/usr/bin/env bash

# Bootstrap file that is executed by remote jobs submitted by law to set up the environment.
# So-called render variables, denoted by "{{name}}", are replaced with variables
# configured in the remote workflow tasks, e.g. in HTCondorWorkflow.htcondor_job_config(), upon job

######## set env variables ########
# set executing user as this is often used to specify the output directory (e.g. in the law.cfg) 
export USER="{{user}}"
# set the base directory where the repo and cmssw are stored
export BASE_DIR="${PWD}/repo"
# export BASE_DIR="${BASE_DIR}/repo"
# export CMSSW_BASE="${BASE_DIR}/cmssw"
# export CMSSW_VERSION="CMSSW_14_2_0_pre3"
# export SCRAM_ARCH="el8_amd64_gcc12"

# source the law wlcg tools, mainly for law_wlcg_get_file
source "{{wlcg_tools}}" "" || return "$?"

# to get gfal-* executables, source the lcg dir
source "{{grid_stack}}" || return "$?"

# load the repo bundle
(
    mkdir -p "${BASE_DIR}"
    cd "${BASE_DIR}" || exit "$?"
    # download the repo bundle
    law_wlcg_get_file "{{repo_uris}}" '{{repo_pattern}}' "${BASE_DIR}/repo.tgz" || return "$?"
    # extract the repo bundle
    tar -xzf "repo.tgz" || return "$?"
    rm -f "repo.tgz"
    echo "Analysis repo setup done."
)


cd "${BASE_DIR}" || return "$?"
# source some lcg stack to get the right python version
source "{{lcg_stack}}" || return "$?"
# source the repo setup
source "${BASE_DIR}/setup.sh" || return "$?"
return "0"
