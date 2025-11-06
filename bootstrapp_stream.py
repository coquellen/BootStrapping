import argparse
import random
import time
import os
import json


def buffer_stream(stream):
    f = open(stream, 'r')
    buff_positions = {}

    offset = 0
    crystals_in = []
    crystals_out = []
    resolution_cut = []
    HEADER = True
    header = ''
    i = 0
    N = 0
    end_peaks = 0
    TO_PROCESS = []
    # if not multi events h5
    event = None
    while True:

        # line = f.readline()
        while HEADER:
            line = f.readline()
            header += line
            offset += len(line)
            if 'Begin chunk' in line:
                HEADER = False
                begin_chunk = offset
                buff_positions['header'] = header

        line = f.readline()
        if not line:
            break
        # if 'Begin geom' in line:
        #    geom_start = offset
        # elif 'End geom' in line:
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
            crystals_out.append(offset + len(line))
            TO_PROCESS.append(N)
            N += 1
        elif 'n_indexing_tries' in line:
            tries = int(line.split()[2])
        elif 'peak_resolution' in line:
            resolution_cut.append(float(line.split()[5]))
        elif 'End chunk' in line:
            if len(TO_PROCESS) > 0:
                for n, crystal_n in enumerate(TO_PROCESS):
                    key = str(crystal_n)
                    buff_positions[key] = {'begin_chunk': begin_chunk,
                                           'end_chunk': offset,
                                           'filename': filename,
                                           'begin_crystal': crystals_in[n],
                                           'end_crystal': crystals_out[n],
                                           # 'n_tries': tries,
                                           'resolution_limit': resolution_cut,
                                           'end_peaks': end_peaks,
                                           'event': event}
                    # print(begin_chunk, offset, len(crystals_in), len(crystals_out), filename, event)
                    # assert len(crystals_in) == len(crystals_out)
            #  else:
            #    print(f'Error with {filename} -- Event {event}')   #erint(begin_chunk, offset, len(crystals_in))
            crystals_in = []
            crystals_out = []
            resolution_cut = []
            TO_PROCESS = []
            i = 0
            event = None

        offset += len(line)
    f.seek(0)
    f.close()
    return buff_positions


def bootstrap_stream(stream_fn, output, dict, N):

    keys = list(dict.keys())
    total = list(range(len(keys) - 1))
    sele = random.choices(total, k=len(total))

    stream = open(stream_fn)
    out = open(f'{output}_{N:03d}.stream','w')

    # Get CrystFEL stream header and print it to file
    head = dict['header']
    out.write(head)

    for n,i in enumerate(sorted(sele)):
      start = dict[str(i)]['begin_chunk']
      stop = dict[str(i)]['end_peaks']
      stream.seek(start)
      out.write(stream.read(stop-start))
    
      start = dict[str(i)]['begin_crystal']
      stop = dict[str(i)]['end_crystal']
      stream.seek(start)
      out.write(stream.read(stop-start))
    
      stream.seek(dict[str(i)]['end_chunk'])
      out.write(stream.readline())

    out.close()
    stream.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Given a CrystFEL stream, n bootstrapped streams will be generated. '
                    'If this script has already been used, the generated json can be passed as an argument to avoid parsing the CrystFEL stream file another time')
    parser.add_argument('--stream', '-s', help='Input crystFEL stream file')
    parser.add_argument('-n', help='Number of bootstrapped streamfiles to create (defaut=1)',default=1, type=int)
    parser.add_argument('--output', '-o', help='Basename for bootstrapped streams. Trailing numbers will be added')
    parser.add_argument('--json', '-j', help='Optional json file')

    args = parser.parse_args()

    stream_fn = args.stream
    json_fn = args.json
    n = args.n
    output = args.output

    if output is None:
        output = os.path.basename(stream_fn.split('.stream')[0])
    else:
        output = os.path.basename(output.split('.stream')[0])
    print(output)
    #exit()
    if json_fn is not None:
        with open(json_fn, 'r') as f:
            dict = json.load(f)
    else:
        dict = buffer_stream(stream_fn)
        with open(f'{output}.json', 'w') as fp:
            json.dump(dict, fp)

    for i in range(n):
        bootstrap_stream(stream_fn, output, dict, i)
