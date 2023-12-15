package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apstndb/adcplus"
	"github.com/apstndb/adcplus/tokensource"
	"github.com/jessevdk/go-flags"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type genericIterator[T any] interface {
	Next() (*T, error)
}

func getN[T any](iter genericIterator[T], max int) ([]*T, error) {
	var n int
	var result []*T
	for {
		if n >= max {
			break
		}
		elem, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		result = append(result, elem)
		n++
	}
	return result, nil
}

func getAll[T any](iter genericIterator[T]) ([]*T, error) {
	return getN[T](iter, math.MaxInt)
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatalln(err)
	}
}

type queryOpts struct {
}

type opts struct {
	/*
		Positional struct {
			Command string `positional-arg-name:"command" description:"(required) Command." required:"true"`
			Query   string `positional-arg-name:"query" description:"(required) Query." required:"false"`
		} `positional-args:"yes"`
	*/
	QueryOptions struct {
		Positional struct {
			Query string `positional-arg-name:"query"`
		} `positional-args:"yes"`
		Parameter []string `long:"parameter"`
		Sync      bool     `long:"sync"`
		JobId     string   `long:"job_id"`
	} `command:"query"`
	LsOptions struct {
		Jobs        bool   `long:"jobs"`
		ParentJobId string `long:"parent_job_id"`
		MaxResults  int    `long:"max_results" short:"n" default:"50"`
		All         bool   `long:"all" short:"a"`
	} `command:"ls"`
	CancelOptions struct {
		Positional struct {
			JobId string `positional-arg-name:"JOB_ID"`
		} `positional-args:"yes"`
		SynchronousMode *bool `long:"synchronous_mode"`
	} `command:"cancel"`

	ProjectId                 string `long:"project_id" required:"true"`
	ImpersonateServiceAccount string `long:"impersonate-service-account"`
	Location                  string `long:"location"`
	DatasetId                 string `long:"dataset_id"`
}

func processFlags() (command string, o opts, err error) {
	// var qo queryOpts
	flagParser := flags.NewParser(&o, flags.Default)
	// cmd, err := flagParser.AddCommand("query", "", "", &qo)
	// if err != nil {
	// 	return o, err
	// }
	_, err = flagParser.Parse()
	if err != nil {
		return "", o, err
	}
	/*
		var subcommand string
		for _, s := range []string{"ls", "query", "show"} {
			if flagParser.Command.Find(s) == flagParser.Active {
				subcommand = s
				break
			}
		}
	*/
	return flagParser.Active.Name, o, nil
}

func run(ctx context.Context) error {
	command, o, err := processFlags()
	if err != nil {
		return err
	}
	fmt.Println("command:", command)

	var copts []option.ClientOption
	ts, err := tokensource.SmartAccessTokenSource(ctx, adcplus.WithTargetPrincipal(o.ImpersonateServiceAccount))
	copts = append(copts, option.WithTokenSource(ts))
	bqCli, err := bigquery.NewClient(ctx, o.ProjectId, copts...)
	if err != nil {
		return err
	}
	defer bqCli.Close()

	switch command {
	case "ls":
		if o.LsOptions.Jobs {
			iter := bqCli.Jobs(ctx)
			iter.AllUsers = o.LsOptions.All
			iter.ParentJobID = o.LsOptions.ParentJobId
			jobs, err := getN[bigquery.Job](iter, o.LsOptions.MaxResults)
			if err != nil {
				return err
			}
			for _, job := range jobs {
				config, err := job.Config()
				if err != nil {
					return err
				}
				_ = config
				lastStats := job.LastStatus().Statistics

				qc := config.(*bigquery.QueryConfig)
				var trimmedQ string
				if len(qc.Q) > 50 {
					trimmedQ = qc.Q[0:50] + "..."
				} else {
					trimmedQ = qc.Q
				}
				fmt.Println(job.ID(), jobType(config), stateToString(job.LastStatus()), job.Email(), lastStats.StartTime.Format(time.RFC3339), lastStats.EndTime.Sub(lastStats.StartTime), strconv.Quote(trimmedQ), formatQueryParameters(qc.Parameters))
			}
			return nil

		}
		if o.DatasetId == "" {
			datasets, err := getN[bigquery.Dataset](bqCli.Datasets(ctx), o.LsOptions.MaxResults)
			if err != nil {
				return err
			}
			for _, dataset := range datasets {
				fmt.Println(dataset.DatasetID)
			}
		} else {
			tables, err := getN[bigquery.Table](bqCli.Dataset(o.DatasetId).Tables(ctx), o.LsOptions.MaxResults)
			if err != nil {
				return err
			}
			for _, table := range tables {
				metadata, err := table.Metadata(ctx)
				if err != nil {
					return err
				}
				dontExpire := metadata.ExpirationTime.IsZero()
				var expires string
				if !dontExpire {
					expires = metadata.ExpirationTime.Format(time.RFC3339)
				}
				fmt.Println(table.TableID, metadata.NumRows, expires, metadata.TimePartitioning, metadata.Clustering, metadata.NumBytes, metadata.NumLongTermBytes, metadata.Labels, metadata.Type, metadata.Clustering)
			}
		}
	case "cancel":
		j, err := bqCli.JobFromIDLocation(ctx, o.CancelOptions.Positional.JobId, o.Location)
		if o.CancelOptions.SynchronousMode != nil && !*o.CancelOptions.SynchronousMode {
			return err
		}
		js, err := j.Wait(ctx)
		_ = js
		return err
	case "query":
		q := bqCli.Query(o.QueryOptions.Positional.Query)
		var parameters []bigquery.QueryParameter
		for _, param := range o.QueryOptions.Parameter {
			splits := strings.SplitN(param, ":", 3)
			if len(splits) != 3 {
				return fmt.Errorf("parameter format is not valid %s", param)
			}
			key, typ, valueStr := splits[0], splits[1], splits[2]
			var value interface{}
			switch typ {
			case "", "STRING":
				value = valueStr
			case "TIMESTAMP":
				value, err = time.Parse(time.RFC3339Nano, valueStr)
				if err != nil {
					return err
				}
			}
			parameters = append(parameters, bigquery.QueryParameter{
				Name:  key,
				Value: value,
			})
		}
		q.Parameters = parameters
		q.Location = o.Location
		if o.QueryOptions.JobId != "" {
			q.JobID = o.QueryOptions.JobId
		} else {
			q.JobID = "gobqjob"
			q.AddJobIDSuffix = true
		}
		fmt.Println("query:", o.QueryOptions.Positional.Query)
		fmt.Println("sync:", o.QueryOptions.Sync)

		j, err := q.Run(ctx)
		if err != nil {
			return err
		}
		fmt.Println(j.ID())
		if o.QueryOptions.Sync {
			js, err := j.Wait(ctx)
			if err != nil {
				return err
			}
			_ = js
			ri, err := j.Read(ctx)
			if err != nil {
				return err
			}

			// var rows [][]bigquery.Value
			for {
				var row []bigquery.Value
				err = ri.Next(&row)
				if err == iterator.Done {
					break
				}
				if err != nil {
					return err
				}
				// TODO: table format
				fmt.Println(row)
				// rows = append(rows, row)
			}
		}
	default:
		return fmt.Errorf("unknown cmd: %s", command)
	}
	return nil
}

func stateToString(state *bigquery.JobStatus) string {
	if state.Err() != nil {
		return "Failure"
	}
	switch state.State {
	case bigquery.StateUnspecified:
		return "Unspecified"
	case bigquery.Running:
		return "Running"
	case bigquery.Done:
		return "Success"
	case bigquery.Pending:
		return "Pending"
	default:
		panic("unknown status")
	}
}

func formatQueryParameters(params []bigquery.QueryParameter) string {
	var paramStrs []string
	for _, p := range params {
		paramStrs = append(paramStrs, fmt.Sprintf("%v=%v", p.Name, p.Value))
	}
	return strings.Join(paramStrs, ",")
}

func jobType(job bigquery.JobConfig) string {
	switch job.(type) {
	case *bigquery.QueryConfig:
		return "query"
	case *bigquery.LoadConfig:
		return "load"
	case *bigquery.ExtractConfig:
		return "extract"
	case *bigquery.CopyConfig:
		return "copy"
	default:
		return "unknown"
	}
}
