#!/bin/bash

# check if there is a parameter that specifies the cmssw version
if [ $# -eq 1 ]; then
	cmssw_ver=$1
else
	cmssw_ver=CMSSW_14_2_0_pre3
fi

source /cvmfs/cms.cern.ch/cmsset_default.sh

# check if the cmssw version is already set up else set it up
if [ -d $cmssw_ver ]; then
	echo "################ $cmssw_ver is already set up ################"
else
	echo "################ Setting up $cmssw_ver ################"
	cmsrel $cmssw_ver
fi

cd ${cmssw_ver}/src || exit $?
cmsenv

# init git if not already done
if [ ! -d .git ]; then
	git cms-init
	# git clone --no-checkout --depth 1 --sparse -b embedding_update_for_run3 git@github.com:KIT-CMS/cmssw.git .
fi

# needed_packages=(TauAnalysis/MCEmbeddingTools DataFormats/GsfTrackReco)
git sparse-checkout set TauAnalysis/MCEmbeddingTools DataFormats/GsfTrackReco


echo "################ Get dev changes form KIT-CMS ################"
git remote add kit-cms git@github.com:KIT-CMS/cmssw.git
git fetch kit-cms embedding_update_for_run3
git switch embedding_update_for_run3

echo "################ Compiling ################"
scram b -j 10 || exit $?

exit 0
