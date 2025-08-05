import sys
import pickle
import random
import time
import os
import json

json_fn, stream_fn, N = sys.argv[1:]

N = int(N)

with open(json_fn, 'r') as f:
    pikle =json.load(f)
#pkl = '3us_0.75uJ_light.pickle'
#stream_fn = '/sf/cristallina/data/p21736/work/nico/streams_final/3us_0.75uJ_light.stream'
#N = int(sys.argv[1])

#with open(pkl,'rb') as f:
#  pikle = pickle.load(f)

stream = open(stream_fn)
out = open(f'dark_{N:03d}.stream','w')

# Get CrystFEL stream header and print it to file
head = pikle['header']
out.write(head)

keys=list(pikle.keys())
total = list(range(len(keys)-1))
sele = random.choices(total,k=len(total))

print(len(sele))

t0=time.time()
for n,i in enumerate(sorted(sele)):
  if n % 100 == 0:
    t=time.time() 
    print(n, t-t0)
    t0=t
  start = pikle[str(i)]['begin_chunk']
  stop = pikle[str(i)]['end_peaks']
  stream.seek(start)
  out.write(stream.read(stop-start))

  start = pikle[str(i)]['begin_crystal']
  stop = pikle[str(i)]['end_crystal']
  stream.seek(start)
  out.write(stream.read(stop-start))

  stream.seek(pikle[str(i)]['end_chunk'])
  out.write(stream.readline())


out.close()
#root = '3us'
#pdb='/sf/cristallina/data/p21736/work/nico/streams_final/carh_3us.pdb'
#cmd=f'sbatch -p day -N 1 -c 20 --wrap="../merging_SwissFEL.sh {root}_{N:03d} {pdb} 2.9 mmm"'
#os.system(cmd)
