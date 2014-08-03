TIME = time
PYTHON = python
MPI = mpirun

default: sequential

sequential:
	$(PYTHON) sequential_clustering.py -d dna -c 10 -t 5 -i dna.txt -o results.txt

parallel:
	$(MPI) -np 11 $(PYTHON) parallel_clustering.py -d dna -t 5 -i dna.txt -o results.txt

sequential_time:
	$(TIME) $(PYTHON) sequential_clustering.py -d dna -c 10 -t 5 -i dna.txt -o results.txt

parallel_time:
	$(TIME) $(MPI) -np 11 $(PYTHON) parallel_clustering.py -d dna -t 5 -i dna.txt -o results.txt

gen_point:
	$(PYTHON) generaterawdata.py -c 10 -p 100 -o point.txt

gen_dna:
	$(PYTHON) generaterawdata_dna.py -c 10 -d 100 -b 10 -o dna.txt

clean:
	$(RM) -r *.txt
