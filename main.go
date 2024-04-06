package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	chunkSize = 1024 * 80
)

type Location struct {
	Min   int64
	Max   int64
	Total int64
	Count int64
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timeStart := time.Now()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if len(os.Args) < 2 {
		slog.ErrorContext(ctx, "need to supply file")
		os.Exit(1)
	}

	// get file name no ext
	fileName := filepath.Base(os.Args[1])
	fileName = strings.TrimSuffix(fileName, filepath.Ext(fileName))

	// create file for profile
	f, err := os.Create(fmt.Sprintf("%s-profile.pb.gz", fileName))
	if err != nil {
		slog.ErrorContext(ctx, "unable to create file for cpu pprof")
		os.Exit(1)
	}
	defer f.Close()

	// start CPU profiling
	if err := pprof.StartCPUProfile(f); err != nil {
		slog.ErrorContext(ctx, err.Error())
		os.Exit(1)
	}
	defer pprof.StopCPUProfile()

	_, err = run(ctx, os.Args[1], true)
	if err != nil {
		slog.ErrorContext(ctx, err.Error())
		os.Exit(1)
	}
	slog.InfoContext(ctx, "success", slog.Float64("durationSeconds", time.Since(timeStart).Seconds()))
}

func run(ctx context.Context, filePath string, concurrency bool) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if concurrency {
		locations, locationMap, err := parseFileWithConcurrency(ctx, f)
		if err != nil {
			return "", err
		}
		return createResult(locations, locationMap)
	}

	locations, locationMap, err := parseFile(ctx, f)
	if err != nil {
		return "", err
	}
	return createResult(locations, locationMap)
}

func parseFile(ctx context.Context, file *os.File) ([]string, map[string]Location, error) {
	locations := []string{}
	locationMap := map[string]Location{}

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		// avoid using strings.Split from CPU profiling
		splitIndex := strings.Index(line, ";")
		if splitIndex == -1 {
			//slog.WarnContext(ctx, "line does not have ; present", slog.String("line", line))
			continue
		}

		locationName := line[0:splitIndex]
		temperature := parseNumber(line[splitIndex+1:])

		loc, ok := locationMap[locationName]
		if !ok {
			// add to locations slice for ordered location printing at end
			locations = append(locations, locationName)

			// add location to map using name as key
			locationMap[locationName] = Location{
				Min:   temperature,
				Max:   temperature,
				Total: temperature,
				Count: 1,
			}
			continue
		}

		// if location exists in map increase Count, Total, Max and Min
		loc.Count++
		loc.Total += temperature
		if loc.Max < temperature {
			loc.Max = temperature
		}
		if loc.Min > temperature {
			loc.Min = temperature
		}
		locationMap[locationName] = loc
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	return locations, locationMap, nil
}

func createResult(locations []string, locationMap map[string]Location) (string, error) {
	buffer := bytes.Buffer{}
	buffer.WriteRune('{')

	// ensure alpha order
	sort.Strings(locations)

	for i := range locations {
		details, ok := locationMap[locations[i]]
		if !ok {
			return "", fmt.Errorf("location '%s' found in locations but not in map", locations[i])
		}

		// fmt.Println(locations[i])
		// fmt.Printf("%+v\n", details)

		if i > 0 {
			buffer.WriteRune(',')
			buffer.WriteRune(' ')
		}

		buffer.WriteString(locations[i])
		buffer.WriteRune('=')
		buffer.WriteString(strconv.FormatFloat(float64(details.Min)/10, 'f', 1, 64))
		buffer.WriteRune('/')
		average := math.Round(float64(details.Total) / float64(details.Count))
		buffer.WriteString(strconv.FormatFloat(average/10, 'f', 1, 64))
		buffer.WriteRune('/')
		buffer.WriteString(strconv.FormatFloat(float64(details.Max)/10, 'f', 1, 64))
	}
	buffer.WriteRune('}')
	return buffer.String(), nil
}

func parseNumber(temperature string) int64 {
	// avoid split string due to CPU profile
	negative := temperature[0] == '-'
	if negative {
		temperature = temperature[1:]
	}

	var val int64
	if temperature[1] == '.' {
		// 1.2
		val = int64(temperature[2]) + int64(temperature[0])*10 - '0'*(11)
	} else {
		// 12.3
		val = int64(temperature[3]) + int64(temperature[1])*10 + int64(temperature[0])*100 - '0'*(111)
	}

	if negative {
		val = -val
	}

	return val
}

// concurrency funcs
func lineOrchestrator(file *os.File, results chan<- map[string]Location) error {
	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	start := int64(0)
	end := int64(0)
	for start < fileSize {
		// Increment the wait group counter
		wg.Add(1)

		end = start + chunkSize
		// end could be greater than fileSize due to chunking
		if end > fileSize {
			end = fileSize
		}
		go func(start, end int64) {
			defer wg.Done()

			chunk := make([]byte, end-start)
			_, err := file.ReadAt(chunk, start)
			if err == io.EOF {
				return
			}
			if err != nil {
				fmt.Println("Error reading chunk:", err)
				return
			}

			results <- processChunk(chunk)
		}(start, end)

		// Move the start position to the next complete line boundary
		start = findNextLineBoundary(file, end)
	}

	slog.Info("file",
		slog.Int64("fileSize", fileSize),
		slog.Int64("bytesRead", end))

	wg.Wait()
	return nil
}

func processChunk(input []byte) map[string]Location {
	locationMap := map[string]Location{}

	data := string(input)

	lines := strings.Split(data, "\n")

	// Process each line
	for _, line := range lines {
		locationName, location := processLine(line)
		if location != nil {
			loc, exists := locationMap[locationName]
			if !exists {
				locationMap[locationName] = *location
				continue
			}
			loc.Count++
			loc.Total += location.Total
			if loc.Max < location.Max {
				loc.Max = location.Max
			}
			if loc.Min > location.Min {
				loc.Min = location.Min
			}

			// update location in map
			locationMap[locationName] = loc
		}
	}

	return locationMap
}

func processLine(line string) (string, *Location) {
	if strings.Trim(line, "") == "" {
		//slog.Warn("line empty")
		return "", nil
	}
	splitIndex := strings.Index(line, ";")
	if splitIndex == -1 {
		//slog.Warn("line does not have ; present", slog.String("line", line))
		return "", nil
	}

	locationName := line[0:splitIndex]
	val := line[splitIndex+1:]

	if len(val) < 3 || val[len(val)-2] != '.' {
		//slog.Warn("line is not complete", slog.String("line", line))
		return "", nil
	}
	temperature := parseNumber(val)

	return locationName, &Location{
		Min:   temperature,
		Max:   temperature,
		Total: temperature,
		Count: 1,
	}
}

func parseFileWithConcurrency(ctx context.Context, file *os.File) ([]string, map[string]Location, error) {
	locations := []string{}
	locationMap := map[string]Location{}
	//mapLock := sync.Mutex{}

	// Channel to communicate processed data
	results := make(chan map[string]Location)
	done := make(chan bool)

	go func() {
		defer func() {
			done <- true
		}()
		err := lineOrchestrator(file, results)
		if err != nil {
			return
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return locations, locationMap, fmt.Errorf("cancelled due to context")
		case <-done:
			return locations, locationMap, nil
		case miniLocationMap := <-results:
			//mapLock.Lock()
			for key, location := range miniLocationMap {
				loc, exists := locationMap[key]
				if !exists {
					locations = append(locations, key)
					locationMap[key] = location
					continue
				}
				loc.Count += location.Count
				loc.Total += location.Total
				if loc.Max < location.Max {
					loc.Max = location.Max
				}
				if loc.Min > location.Min {
					loc.Min = location.Min
				}

				// update location in map
				locationMap[key] = loc
			}
			//mapLock.Unlock()
		}
	}
}

func findNextLineBoundary(file *os.File, start int64) int64 {
	buffer := make([]byte, 1)
	for {
		_, err := file.ReadAt(buffer, start)
		if err != nil || buffer[0] == '\n' {
			return start
		}
		start--
	}
}
