#!/usr/bin/env bash
# usage: abm.sh A B N out ; median sessionize lap + total
cd /tmp/claude-0/-home-user/7093f2af-7756-5848-9566-55372d396f64/scratchpad/ws
export GLIBC_TUNABLES=glibc.malloc.hugetlb=1:glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=17179869184
t(){ rm -rf out/ab; $1 data/p20m out/ab 2>&1 | awk '/regrouped/{r=$1}/sessionized/{z=$1}/written/{w=$1}END{gsub("s","",r);gsub("s","",z);gsub("s","",w);printf "%.3f %.3f\n", z-r, w}'; }
for i in $(seq $3); do echo "A $(t $1)"; echo "B $(t $2)"; done > $4
python3 - $4 <<'P'
import statistics as st,sys
L=[l.split() for l in open(sys.argv[1])]
A=[(float(a),float(b)) for k,a,b in L if k=='A'];B=[(float(a),float(b)) for k,a,b in L if k=='B']
for n,X in (('A',A),('B',B)): print(n,'sess med %.3f total med %.3f best %.3f'%(st.median(x[0] for x in X),st.median(x[1] for x in X),min(x[1] for x in X)))
print('pairs B faster', sum(b[1]<a[1] for a,b in zip(A,B)),'/',len(A))
P
