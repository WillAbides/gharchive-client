package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	jsoniter "github.com/json-iterator/go"
	"github.com/willabides/gharchive-client"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

var cli struct {
	Start           string   `kong:"arg,help='start time formatted as YYYY-MM-DD, or as an RFC3339 date'"`
	End             string   `kong:"arg,optional,help='end time formatted as YYYY-MM-DD, or as an RFC3339 date. default is an hour past start'"`
	IncludeType     []string `kong:"name=type,help='include only these event types'"`
	ExcludeType     []string `kong:"name=not-type,help='exclude these event types'"`
	StrictCreatedAt bool     `kong:"help='only output events with a created_at between start and end'"`
	NoEmptyLines    bool     `kong:"help='skip empty lines'"`
	OnlyValidJSON   bool     `kong:"help='skip lines that aren not valid json objects'"`
	PreserveOrder   bool     `kong:"help='ensure that events are output in the same order they exist on data.gharchive.org'"`
	Concurrency     int      `kong:"help='max number of concurrent downloads to run. Ignored if --preserve-order is set. Default is the number of cpus available.'"`
	Debug           bool     `kong:"help='output debug logs'"`
}

func parseTimeString(st string) (tm time.Time, err error) {
	tm, err = time.Parse(time.RFC3339, st)
	if err == nil {
		return tm, nil
	}
	tm, err = time.ParseInLocation(`2006-01-02`, st, time.UTC)
	if err != nil {
		return tm, fmt.Errorf("invalid time")
	}
	return tm, nil
}

func main() {
	k := kong.Parse(&cli)
	start, err := parseTimeString(cli.Start)
	k.FatalIfErrorf(err, "invalid start time")
	debugLog := log.New(ioutil.Discard, "DEBUG ", log.LstdFlags)
	if cli.Debug {
		debugLog.SetOutput(os.Stderr)
	}
	var end time.Time
	if cli.End != "" {
		end, err = parseTimeString(cli.End)
		k.FatalIfErrorf(err, "invalid end time. must be either 'YYYY-MM-DD' or 'YYYY-MM-DDThh:mm:ssZ' (RFC 3339")
	}
	if end.IsZero() {
		end = start.AddDate(0, 0, 1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var validators []gharchive.Validator
	if cli.NoEmptyLines {
		validators = append(validators, gharchive.ValidateNotEmpty())
	}
	if cli.OnlyValidJSON {
		validators = append(validators, func(line []byte) bool {
			return jsoniter.ConfigFastest.Valid(line)
		})
	}
	var fieldValidators []gharchive.JSONFieldValidator
	if cli.StrictCreatedAt {
		fieldValidators = append(fieldValidators, gharchive.JSONFieldValidator{
			Field: "created_at",
			Validator: gharchive.TimeValueValidator(func(val time.Time) bool {
				if val.After(end) {
					cancel()
					return false
				}
				if val.Equal(start) || val.Equal(end) {
					return true
				}
				return val.After(start) && val.Before(end)
			}),
		})
	}
	if len(cli.IncludeType) > 0 {
		for i, s := range cli.IncludeType {
			if !strings.HasSuffix(strings.ToLower(s), "event") {
				cli.IncludeType[i] = s + "Event"
			}
		}
		fieldValidators = append(fieldValidators, gharchive.JSONFieldValidator{
			Field: "type",
			Validator: gharchive.StringValueValidator(func(val string) bool {
				for _, s := range cli.IncludeType {
					if strings.EqualFold(s, val) {
						return true
					}
				}
				return false
			}),
		})
	}
	if len(cli.ExcludeType) > 0 {
		for i, s := range cli.ExcludeType {
			if !strings.HasSuffix(strings.ToLower(s), "event") {
				cli.ExcludeType[i] = s + "Event"
			}
		}
		fieldValidators = append(fieldValidators, gharchive.JSONFieldValidator{
			Field: "type",
			Validator: gharchive.StringValueValidator(func(val string) bool {
				for _, s := range cli.ExcludeType {
					if strings.EqualFold(s, val) {
						return false
					}
				}
				return true
			}),
		})
	}
	if len(fieldValidators) > 0 {
		validators = append(validators, gharchive.ValidateJSONFields(fieldValidators))
	}
	if cli.Concurrency == 0 {
		cli.Concurrency = runtime.NumCPU()
	}
	if cli.PreserveOrder {
		cli.Concurrency = 1
	}
	debugLog.Printf("concurrency=%d", cli.Concurrency)
	debugLog.Printf("start=%s", start.Format(time.RFC3339))
	debugLog.Printf("end=%s", end.Format(time.RFC3339))
	sc, err := gharchive.New(ctx, start, &gharchive.Options{
		Validators:    validators,
		Concurrency:   cli.Concurrency,
		PreserveOrder: cli.PreserveOrder,
		EndTime:       end,
	})
	k.FatalIfErrorf(err, "error creating scanner")
	defer func() {
		_ = sc.Close() //nolint:errcheck // nothing to do with this error
	}()
	var lineCount int
	scanStartTime := time.Now()
	for sc.Scan(ctx) {
		lineCount++
		fmt.Print(string(sc.Bytes()))
	}
	scanDuration := time.Since(scanStartTime)
	linesPerSecond := int64(float64(lineCount) / scanDuration.Seconds())
	debugLog.Println("done")
	debugLog.Printf("output %s lines", message.NewPrinter(language.English).Sprintf("%d", lineCount))
	debugLog.Printf("took %0.2f seconds", scanDuration.Seconds())
	debugLog.Printf("output %s lines per second", message.NewPrinter(language.English).Sprintf("%d", linesPerSecond))

	err = sc.Err()
	if err == io.EOF || err == context.Canceled {
		err = nil
	}
	k.FatalIfErrorf(err, "error streaming from gharchive")
}
