import sys
import json
import argparse
import numpy as np
import os


parser = argparse.ArgumentParser(description= 'Given a CrystFEL stream filePlot peakevolution as a function of indexed images. You will need to change the hardcoded arguments: p_files, crystals, plotnames, residues. If you want to fit the data with a sqrt(n) function, add the --fit argument.')
    parser.add_argument('--fit', action='store_true' , help='Fit the data with a sqrt(n) function')
    args = parser.parse_args()



stream1 = sys.argv[1]
#stream2 = sys.argv[2]
root = os.path.basename(stream1.split('.stream')[0])


def buff_stream(stream):
    f = open(stream, 'r')
    buff_positions = {}

    offset = 0
    crystals_in = []
    crystals_out = []
    resolution_cut = []
    HEADER=True
    header = ''
    i=0
    N = 0
    end_peaks = 0
    TO_PROCESS = []
    while True:
 
    #line = f.readline()
      while HEADER:
        line = f.readline()
        header+=line
        offset+=len(line)
        if 'Begin chunk' in line:
            HEADER = False
            begin_chunk = offset
            buff_positions['header'] = header

      line = f.readline()
      if not line:
        break
    #if 'Begin geom' in line:
    #    geom_start = offset
    #elif 'End geom' in line:
    #    buff_positions['geom'] = {'start': geom_start,
    #                              'end': offset+len(line)}
      elif 'Begin chunk' in line:
        begin_chunk = offset
      elif 'filename' in line:
        filename = line.split()[2]
      elif 'Event' in line:
        event = line.split()[1][:-2]
      elif 'Begin crystal' in line:
        if i == 0: 
          end_peaks = offset
          i = 1
        crystals_in.append(offset)
      elif 'End crystal' in line:
        crystals_out.append(offset+len(line))
        TO_PROCESS.append(N)
        N+=1
      elif 'n_indexing_tries' in line:
        tries = int(line.split()[2])
      elif 'peak_resolution' in line:
        resolution_cut.append(float(line.split()[5]))
      elif 'End chunk' in line:
        if len(TO_PROCESS) > 0:
         for n,crystal_n in enumerate(TO_PROCESS):
          key=str(crystal_n)
          buff_positions[key] = {'begin_chunk': begin_chunk,
                                    'end_chunk': offset,
                                    'filename': filename,
                                    'begin_crystal': crystals_in[n],
                                    'end_crystal': crystals_out[n],
                                     #'n_tries': tries,
                                    'resolution_limit': resolution_cut,
                                    'end_peaks': end_peaks,
                                    'event': event}
            #print(begin_chunk, offset, len(crystals_in), len(crystals_out), filename, event)
            #assert len(crystals_in) == len(crystals_out)
        #  else:
        #    print(f'Error with {filename} -- Event {event}')   #erint(begin_chunk, offset, len(crystals_in))
        crystals_in = []
        crystals_out = []
        resolution_cut = []
        TO_PROCESS = []
        i = 0

      offset += len(line)
    f.seek(0)
    f.close()
    return buff_positions

buff=buff_stream(stream1)

with open(f'{root}.json', 'wb') as handle:
    pickle.dump(buff, handle, protocol=pickle.HIGHEST_PROTOCOL)

tf = time.time()
print(f"Time: {tf-t0:4.2f}")
