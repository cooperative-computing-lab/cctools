#! /bin/bash

conda env create --name cctools-build --yes --quiet --channel conda-forge --strict-channel-priority --experimental-solver=libmamba --file environment.yml
