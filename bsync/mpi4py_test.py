#!/usr/bin/env python3

from mpi4py import MPI
comm = MPI.COMM_WORLD;
mpi_rank = comm.Get_rank();
mpi_size = comm.Get_size();

print(mpi_rank,mpi_size);


