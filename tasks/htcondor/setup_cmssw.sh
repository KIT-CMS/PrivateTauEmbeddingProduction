#!/usr/bin/env bash
#####################################################################################################################
################# This is not maintained anymore, use singularity_sandbox/setup_cmssw.sh instead!!! #################
#####################################################################################################################

echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "WARNING: This script is not maintained anymore, use singularity_sandbox/setup_cmssw.sh instead!!!"
echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

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

    # only do the following if the cmssw branch is not master, otherwise we can just use the master branch of the cms-sw repo which is already set up by git cms-init
    if [ "$cmssw_branch" != "master" ]; then
        echo "################ Setting up sparse checkout for branch $cmssw_branch ################"

        # needded for Run2 UL analyses: git cms-addpkg TauAnalysis/MCEmbeddingTools DataFormats/GsfTrackReco RecoEgamma/EgammaPhotonAlgos RecoEgamma/EgammaElectronProducers Configuration/ProcessModifiers Configuration/EventContent Configuration/Applications || return $?
        # needed for Run3 analyeses, when there was no Pull request merged yet: git cms-addpkg Configuration/Applications Configuration/EventContent Configuration/Eras Configuration/ProcessModifiers Configuration/PyReleaseValidation Configuration/StandardSequences IOMC/EventVertexGenerators PhysicsTools/NanoAOD RecoLocalCalo/Configuration RecoLocalCalo/EcalRecProducers RecoLocalCalo/HcalRecProducers RecoLocalMuon/CSCRecHitD RecoLocalMuon/CSCSegment RecoLocalMuon/DTRecHit RecoLocalMuon/DTSegment RecoLocalMuon/RPCRecHit RecoLocalTracker/SiStripClusterizer RecoLuminosity/LumiProducer RecoTracker/IterativeTracking RecoVertex/Configuration SimGeneral/MixingModule TauAnalysis/MCEmbeddingTools
        git cms-addpkg TauAnalysis/MCEmbeddingTools

        echo "################ Get dev changes form KIT-CMS ################"
        git remote add kit-cms https://github.com/KIT-CMS/cmssw.git || return $?
        git fetch kit-cms $cmssw_branch || return $?
        git switch $cmssw_branch || return $?

        echo "################ Compiling with {{n_compile_cores}} cores################"
        scram b -j {{n_compile_cores}} || return $?
    fi
) || return "$?"

cd "${BASE_DIR}" || return "$?"
echo "Sourcing the lcg stack"
source "{{lcg_stack}}" || return "$?"
echo "Sourcing the grid stack"
# source "{{grid_stack}}" || return "$?"
# source the repo setup
echo "Sourcing the setup.sh"
source "${BASE_DIR}/setup.sh" || return "$?"
