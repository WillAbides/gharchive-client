package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	jsoniter "github.com/json-iterator/go"
	"github.com/willabides/gharchive-client"
)

var cli struct {
	Start           string   `kong:"arg,help='start time formatted as YYYY-MM-DD, or as an RFC3339 date'"`
	End             string   `kong:"arg,optional,help='end time formatted as YYYY-MM-DD, or as an RFC3339 date. default is a day past start'"`
	IncludeType     []string `kong:"name=type,help='include only these event types'"`
	ExcludeType     []string `kong:"name=not-type,help='exclude these event types'"`
	StrictCreatedAt bool     `kong:"help='only output events with a created_at between start and end'"`
	NoEmptyLines    bool     `kong:"help='skip empty lines'"`
	OnlyValidJSON   bool     `kong:"help='skip lines that aren not valid json objects'"`
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
	var end time.Time
	if cli.End != "" {
		end, err = parseTimeString(cli.End)
		k.FatalIfErrorf(err, "invalid end time")
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
	sc, err := gharchive.New(ctx, start, end, &gharchive.Options{Validators: validators})
	k.FatalIfErrorf(err, "error creating scanner")
	defer func() {
		_ = sc.Close() //nolint:errcheck // nothing to do with this error
	}()
	for {
		line, err := sc.Next(ctx)
		if err == io.EOF || err == context.Canceled {
			break
		}
		k.FatalIfErrorf(err, "error streaming from gharchive")
		fmt.Print(string(line))
	}
}
