#! /bin/bash

conda create -n nlp python=3.11
conda activate nlp
conda install pytorch
conda install transformers
conda install pandas
conda install numpy
conda install scikit-learn
conda install datasets
conda install sentencepiece
pip install 'accelerate>=0.26.0'