#!/usr/bin/env bash

######## set env variables ########
# set executing user as this is used to specify the output directory (e.g. in the law.cfg) 
export USER="{{user}}"
# set the base directory where the repo and cmssw are stored
export BASE_DIR="${PWD}/repo"
# set CMSSW branch
export cmssw_branch="{{cmssw_branch}}"

# source the law wlcg tools, mainly for law_wlcg_get_file
source "{{wlcg_tools}}" "" || return "$?"
# load the repo bundle
(
    mkdir -p "${BASE_DIR}"
    cd "${BASE_DIR}" || return "$?"
    # download the repo bundle
    law_wlcg_get_file "{{repo_uris}}" '{{repo_pattern}}' "${BASE_DIR}/repo.tgz" || return "$?"
    # extract the repo bundle
    tar -xzf "repo.tgz" || return "$?"
    rm -f "repo.tgz"
    echo "Analysis repo setup done."
) || return "$?"

# setup cmssw
(
    source /cvmfs/cms.cern.ch/cmsset_default.sh || return $?
    cd "${BASE_DIR}" || return $?
    export SCRAM_ARCH="{{cmssw_scram_arch}}"
    # check if the cmssw version is already set up else set it up
    if [ -d "{{cmssw_version}}" ]; then
        echo "################ {{cmssw_version}} is already set up ################"
    else
        echo "################ Setting up {{cmssw_version}} ################"
        cmsrel "{{cmssw_version}}" || return $?
    fi

    cd "{{cmssw_version}}/src" || return $?
    cmsenv || return $?

    # git init itself does not work. It needs to be done with git cms-init
    git cms-init --upstream-only # --upstream-only is used to avoid checking for a personal cmmsw fork and the personal git name/email etc are not needed

    # CMSSW folders which contain special code changes in the embedding $cmssw_branch
    git sparse-checkout set TauAnalysis/MCEmbeddingTools DataFormats/GsfTrackReco RecoEgamma/EgammaPhotonAlgos RecoEgamma/EgammaElectronProducers || return $?


    echo "################ Get dev changes form KIT-CMS ################"
    git remote add kit-cms https://github.com/KIT-CMS/cmssw.git || return $?
    git fetch kit-cms $cmssw_branch || return $?
    git switch $cmssw_branch || return $?

    echo "################ Compiling with {{n_compile_cores}} cores################"
    scram b -j {{n_compile_cores}} || return $?
) || return "$?"

cd "${BASE_DIR}" || return "$?"
echo "Sourcing the lcg stack"
source "{{lcg_stack}}" || return "$?"
echo "Sourcing the grid stack"
# source "{{grid_stack}}" || return "$?"
# source the repo setup
echo "Sourcing the setup.sh"
source "${BASE_DIR}/setup.sh" || return "$?"