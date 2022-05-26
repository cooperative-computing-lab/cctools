./expand.sh.1 > example.mf
resource_monitor -O resource_output-$scale -- "$makeflow"  example.mf 


resource_monitor -O resource_output_recovery-$scale -- "$makeflow"  example.mf 
