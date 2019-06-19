# readcompressability
Utility for measuring the sequence complexity of reads in a fastq file using zlib compression ratio.

    usage: readcompressability [options] unaligned_1.fq.gz unaligned_2.fq.gz
      -check
        	Check that the read names match (for PE data)
      -limit int
        	limit the number of reads to consider (default = 0 = unlimited)
      -out string
        	output filename (default = stdout)
