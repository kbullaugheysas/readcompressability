package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

/* This program takes on one or two (in the case of paried end data) fq files
 * and computes the zlib compression ratio (contanenating reads in the case of PE data). */

type Args struct {
	OutFilename string
	Limit       int
	CheckNames  bool
}

var args = Args{}

func init() {
	log.SetFlags(0)
	flag.StringVar(&args.OutFilename, "out", "", "output filename (default = stdout)")
	flag.IntVar(&args.Limit, "limit", 0, "limit the number of reads to consider (default = 0 = unlimited)")
	flag.BoolVar(&args.CheckNames, "check", false, "Check that the read names match (for PE data)")

	flag.Usage = func() {
		log.Println("usage: readcompressability [options] unaligned_1.fq.gz unaligned_2.fq.gz")
		flag.PrintDefaults()
	}
}

/* Provide an ambidexterous interface to files to read that may be gzipped */
type AmbiReader struct {
	fp *os.File
	gz *gzip.Reader
	r  io.Reader
}

func (a AmbiReader) Read(b []byte) (n int, err error) {
	return a.r.Read(b)
}

func (a *AmbiReader) Open(fn string) error {
	if a.r != nil {
		return fmt.Errorf("AmbiReader already open")
	}
	var err error
	// If no filename is given, then read from stdin
	if fn == "" {
		a.r = os.Stdin
		return nil
	}
	a.fp, err = os.Open(fn)
	if err != nil {
		return err
	}
	if strings.HasSuffix(fn, ".gz") {
		a.gz, err = gzip.NewReader(a.fp)
		if err != nil {
			return err
		}
		a.r = a.gz
	} else {
		a.r = a.fp
	}
	return nil
}

func (a *AmbiReader) Close() error {
	if a.gz != nil {
		if err := a.gz.Close(); err != nil {
			return err
		}
	}
	if err := a.fp.Close(); err != nil {
		return err
	}
	return nil
}

/* Provide an ambidexterous interface to files to write that may be gzipped */
type AmbiWriter struct {
	fp *os.File
	gz *gzip.Writer
	r  io.Writer
}

func (a AmbiWriter) Write(b []byte) (n int, err error) {
	return a.r.Write(b)
}

func (a *AmbiWriter) Close() error {
	if a.gz != nil {
		if err := a.gz.Close(); err != nil {
			return err
		}
	}
	if err := a.fp.Close(); err != nil {
		return err
	}
	return nil
}

func (a *AmbiWriter) Open(fn string) error {
	if a.r != nil {
		return fmt.Errorf("AmbiWriter already open")
	}
	var err error
	// If no filename is given, then read from stdin
	if fn == "" {
		a.r = os.Stdout
		return nil
	}
	a.fp, err = os.Create(fn)
	if err != nil {
		return err
	}
	if strings.HasSuffix(fn, ".gz") {
		a.gz = gzip.NewWriter(a.fp)
		a.r = a.gz
	} else {
		a.r = a.fp
	}
	return nil
}

func (a *AmbiWriter) Stdout() {
	a.r = os.Stdout
}

func main() {
	flag.Parse()
	fq := flag.Args()

	// Open the inputs
	inputs := make([]AmbiReader, len(fq))
	for i, fn := range fq {
		if err := inputs[i].Open(fn); err != nil {
			log.Fatalf("Failed to open %s: %v\n", fn, err)
		}
		defer inputs[i].Close()
	}

	// Prepare the output writers
	output := AmbiWriter{}
	if args.OutFilename == "" {
		output.Stdout()
	} else {
		if err := output.Open(args.OutFilename); err != nil {
			log.Fatalf("Failed to open %s for writing: %v\n", args.OutFilename, err)
		}
		defer output.Close()
	}

	// Iterate over the inputs in sync
	inputScanners := make([]*bufio.Scanner, len(fq))
	for i := 0; i < len(fq); i++ {
		inputScanners[i] = bufio.NewScanner(inputs[i])
	}
	line_num := 0
	read_count := 0
	err := func() error {
		var read_name string
		var read_buf bytes.Buffer
		var zwriter *zlib.Writer
		for {
			if args.Limit > 0 && read_count >= args.Limit {
				return nil
			}
			full_len := 0
			for i := 0; i < len(fq); i++ {
				if inputScanners[i].Scan() {
					line := inputScanners[i].Text()
					if line_num%4 == 0 {
						if !strings.HasPrefix(line, "@") {
							return fmt.Errorf("Line %d should be a fastq header line, got: %s\n", line_num, line)
						}
						name := strings.Split(line[1:len(line)], " ")[0]
						if i == 0 {
							read_name = name
							read_buf.Reset()
							zwriter = zlib.NewWriter(&read_buf)
						} else if args.CheckNames && name != read_name {
							return fmt.Errorf("expecting read %s on line %d in input %d, got %s",
								read_name, line_num, i+1, name)
						}
					} else if line_num%4 == 1 {
						full_len += len(line)
						fmt.Fprintln(zwriter, line)
					}
				} else {
					if i == 0 {
						return nil
					} else {
						return fmt.Errorf("Expecting scanner %d to be able to scan\n", i)
					}
				}
			}
			if line_num%4 == 1 {
				zwriter.Close()
				compressed_len := read_buf.Len()
				r := float64(full_len) / float64(compressed_len)
				fmt.Fprintf(output, "%s\t%d\t%d\t%0.4f\n", read_name, full_len, compressed_len, r)
				read_count++
			}
			line_num++
		}
	}()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("processed", line_num, "lines")
}
