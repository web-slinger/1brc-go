package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
)

const (
	measurements10In        string = "measurements_ten.txt"
	measurements10Out       string = "{Adelaide=15.0/15.0/15.0, Cabo San Lucas=14.9/14.9/14.9, Dodoma=22.2/22.2/22.2, Halifax=12.9/12.9/12.9, Karachi=15.4/15.4/15.4, Pittsburgh=9.7/9.7/9.7, Ségou=25.7/25.7/25.7, Tauranga=38.2/38.2/38.2, Xi'an=24.2/24.2/24.2, Zagreb=12.2/12.2/12.2}"
	measurementsRoundingIn  string = "measurements_rounding.txt"
	measurementsRoundingOut string = "{ham=14.6/25.5/33.6, jel=-9.0/18.0/46.5}"
)

func TestRun(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		fileName  string
		expOutput string
	}{
		{
			fileName:  measurements10In,
			expOutput: measurements10Out,
		},
		{
			fileName:  measurementsRoundingIn,
			expOutput: measurementsRoundingOut,
		},
	}

	for _, tc := range tests {
		t.Run(tc.fileName, func(t *testing.T) {

			ctx := context.Background()
			// with concurrency
			output, err := run(ctx, wd+"/"+tc.fileName, true)
			if err != nil {
				t.Fatal(err)
			}

			if tc.expOutput != output {
				t.Errorf("(concurrency) expected %+v but got %+v", tc.expOutput, output)
			}

			// without concurrency
			output, err = run(ctx, wd+"/"+tc.fileName, false)
			if err != nil {
				t.Fatal(err)
			}

			if tc.expOutput != output {
				t.Errorf("expected %+v but got %+v", tc.expOutput, output)
			}
		})
	}
}

func BenchmarkRun(b *testing.B) {
	ctx := context.Background()

	wd, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	slog.SetDefault(logger)

	for i := 0; i < b.N; i++ {
		_, err := run(ctx, wd+"\\measurements_million.txt", true)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	fmt.Print("\n")

	b.ReportAllocs()
}
