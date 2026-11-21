#!/usr/bin/env bash
# CPU-time A/B: usage abc.sh A B N ; median user+sys and wall per binary, interleaved
cd "$(dirname "$0")"
export GLIBC_TUNABLES=glibc.malloc.hugetlb=1:glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=17179869184
t(){ rm -rf out/ab; ( TIMEFORMAT='%R %U %S'; time $1 data/p20m out/ab >/dev/null 2>&1 ) 2>&1 | tail -1; }
for i in $(seq $3); do echo "A $(t $1)"; echo "B $(t $2)"; done | python3 -c "
import sys,statistics as st
L=[l.split() for l in sys.stdin]
A=[(float(w),float(u)+float(s)) for k,w,u,s in L if k=='A']; B=[(float(w),float(u)+float(s)) for k,w,u,s in L if k=='B']
for n,X in (('A',A),('B',B)): print(n,'wall med %.3f  cpu med %.3f'%(st.median(x[0] for x in X),st.median(x[1] for x in X)))
print('cpu pairs B lower %d/%d, wall pairs B lower %d/%d'%(sum(b[1]<a[1] for a,b in zip(A,B)),len(A),sum(b[0]<a[0] for a,b in zip(A,B)),len(A)))"
