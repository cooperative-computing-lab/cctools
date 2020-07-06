#!/usr/bin/python


def create_splits(query):

    #num_reads = 200000
    num_reads = 2000000

    read_count = 0
    q = open(query, "r")
    
    num_outputs = 0
    out = open(query+"."+str(num_outputs), "w+")

    line_count = 0

    for line in q:
        line = line.strip()

        if line.startswith("@") and line_count % 4 == 0:
            if read_count == num_reads:
                out.close()
                num_outputs += 1
                read_count = 0
                out = open(query+"."+str(num_outputs), "w+")
            else:
                read_count += 1

        out.write(line+"\n")

        line_count += 1

    q.close()

    return num_outputs

if __name__ == "__main__":
    create_splits("query.fastq")
