#!/bin/bash
java -jar ~/bin/yesworkflow-0.2.1.2.jar graph main.py \
 -c graph.layout=tb -c graph.edgelabels=show -c graph.portlayout=group -c graph.programlabel=both -c graph.datalabel=both -c graph.view=combined \
 | dot -Tpng -o workflow.png
java -jar ~/bin/yesworkflow-0.2.1.2.jar graph main.py \
 -c graph.layout=tb -c graph.edgelabels=show -c graph.portlayout=group -c graph.programlabel=both -c graph.datalabel=both -c graph.view=combined \
 > workflow.gv
java -jar ~/bin/yesworkflow-0.2.1.2.jar extract main.py -c extract.listfile > workflow.yw
