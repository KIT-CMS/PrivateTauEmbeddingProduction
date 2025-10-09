#!/usr/bin/env bash

set -e # Exit immediately if a command exits with a non-zero status
set -o pipefail # Return the exit status of the last command in the pipeline that failed

######## set env variables ########
# set the base directory where the repo and cmssw are stored
export BASE_DIR="${PWD}/repo"
# set CMSSW branch
cmssw_version="$1"
cmssw_branch="$2"
export SCRAM_ARCH="$3"
n_compile_cores="$4"

mkdir -p "${BASE_DIR}"
# setup cmssw

source /cvmfs/cms.cern.ch/cmsset_default.sh
cd "${BASE_DIR}"
# check if the cmssw version is already set up else set it up
if [ -d "$cmssw_version" ]; then
    echo "################ $cmssw_version is already set up ################"
else
    echo "################ Setting up $cmssw_version ################"
    cmsrel "$cmssw_version"

    cd "$cmssw_version/src"
    cmsenv

    # git init itself does not work. It needs to be done with git cms-init
    git cms-init --upstream-only # --upstream-only is used to avoid checking for a personal cmmsw fork and the personal git name/email etc are not needed

    # CMSSW folders which contain special code changes in the embedding $cmssw_branch
    #git sparse-checkout set TauAnalysis/MCEmbeddingTools DataFormats/GsfTrackReco RecoEgamma/EgammaPhotonAlgos RecoEgamma/EgammaElectronProducers Configuration/ProcessModifiers Configuration/EventContent Configuration/Applications
    git cms-addpkg Configuration/Applications Configuration/EventContent Configuration/Eras Configuration/ProcessModifiers Configuration/PyReleaseValidation Configuration/StandardSequences IOMC/EventVertexGenerators PhysicsTools/NanoAOD RecoLocalCalo/Configuration RecoLocalCalo/EcalRecProducers RecoLocalCalo/HcalRecProducers RecoLocalMuon/CSCRecHitD RecoLocalMuon/CSCSegment RecoLocalMuon/DTRecHit RecoLocalMuon/DTSegment RecoLocalMuon/RPCRecHit RecoLocalTracker/SiStripClusterizer RecoLuminosity/LumiProducer RecoTracker/IterativeTracking RecoVertex/Configuration SimGeneral/MixingModule TauAnalysis/MCEmbeddingTools

    echo "################ Get dev changes form KIT-CMS ################"
    git remote add kit-cms https://github.com/KIT-CMS/cmssw.git
    git fetch kit-cms $cmssw_branch
    git switch $cmssw_branch

    echo "################ Compiling with $n_compile_cores cores################"
    scram b -j $n_compile_cores
    cd -
fi

cd ..