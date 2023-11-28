source ~/.zshrc
cd makeflow/test
conda activate cctools-build
./TR_makeflow_001_dirs_01.sh check_needed
./TR_makeflow_001_dirs_01.sh prepare
./TR_makeflow_001_dirs_01.sh run
./TR_makeflow_001_dirs_01.sh clean

exit 1