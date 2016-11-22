#!/bin/bash
for i in {1..20}
do
   python ./get_synpuf_files.py . $i &
done
