package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nikhilkumar1612/zsync-bench/internal/dataset"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "usage:")
		fmt.Fprintln(flag.CommandLine.Output(), "  zsync-dataset validate <dataset-directory>")
		fmt.Fprintln(flag.CommandLine.Output(), "  zsync-dataset events <dataset-directory>")
		fmt.Fprintln(flag.CommandLine.Output(), "  zsync-dataset fixture <linear|reorg> <output-directory>")
	}
	flag.Parse()
	if flag.NArg() == 3 && flag.Arg(0) == "fixture" {
		if err := dataset.WriteFixture(flag.Arg(2), flag.Arg(1)); err != nil {
			fmt.Fprintf(os.Stderr, "write fixture: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if flag.NArg() == 2 && flag.Arg(0) == "events" {
		file, err := os.Open(filepath.Join(flag.Arg(1), "events.bin"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "open event log: %v\n", err)
			os.Exit(1)
		}
		defer file.Close()
		_, _, err = dataset.ScanFrames(file, dataset.ScanOptions{MaxPayloadBytes: 16 << 20}, func(event dataset.EventEnvelope) error {
			fmt.Printf("%d %s %s\n", event.Sequence, event.Kind, event.PayloadSHA256)
			return nil
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "read events: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if flag.NArg() != 2 || flag.Arg(0) != "validate" {
		flag.Usage()
		os.Exit(2)
	}
	report, err := dataset.Validate(flag.Arg(1))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dataset validation failed: %v\n", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintf(os.Stderr, "encode report: %v\n", err)
		os.Exit(1)
	}
}
