from mpi4py import MPI
import numpy as np
from pathlib import Path

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
alphabet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

'''
def sort_dict(d):
    temp = {}
    for item in d:
        for letter in alphabet:
            if item.key() == 
'''
def sort_list_dicts(l):
    temp = []
    for i in range(0, len(alphabet)):
        for j in range(0, len(l)):
            if list(l[j])[0][0] == alphabet[i]:
                temp.append(l[i])
    return temp


class Worker:
    def __init__(self, rank):
        self.rank = rank
        self.status = "liber"
        self.filenames = None
        self.files = []
        self.nr_files = 0
        self.nr_splits = 0

    def MapStep(self):
        self.status = "ocupat"
        x = 26 // (size-1)
        self.nr_splits = 26 // x
        self.nr_files = comm.recv(source=0, tag=1)
        self.filenames = comm.recv(source=0, tag=2)
        for filename in self.filenames:
            self.files.append(open(filename, "r+"))
        
       
        for i in range(0, self.nr_splits):
            with open("./temp-files/proces{0}split{1}.txt".format(self.rank, i), "w+") as f:
                f.truncate(0)
        
        map_phase_map = []

        for file in self.files:
            try:
                words = file.read().split()
                for word in words:
                    map_phase_map.append({word: Path(file.name).stem})
            except:
                pass # sunt multe.
        
        # ar fi bine de sortat lista de elemente cheie: valoare in functie de cheia elementului
        #map_phase_map = sorted(map_phase_map, key=...)
        map_phase_map = sort_list_dicts(map_phase_map)
        last_slice = 0
        contor = 1
        for i in range(0, self.nr_splits):
            # aici trebuie sa ma ocup de scrierea in fisierul temporar corespunzator literei
            with open("./temp-files/proces{0}split{1}.txt".format(self.rank, i), "w+") as f:
                f.write(str(map_phase_map[last_slice:contor*len(map_phase_map)//self.nr_splits]))
                last_slice = contor*len(map_phase_map)//self.nr_splits
                contor = contor + 1



if(rank == 0):
    last_slice = 0
    contor = 1
    files = np.array([open("./test-files/{}.txt".format(x), "r+").name for x in range(1, 26)])
    files_per_proces = (len(files) // (size-1)) + 1
    for i in range(1, size):
        comm.send(files_per_proces, dest=i, tag=1)
        comm.send(files[last_slice:files_per_proces*contor], dest=i, tag=2)
        last_slice = files_per_proces*contor
        contor = contor + 1
    
else:
    worker = Worker(rank)
    worker.MapStep()
