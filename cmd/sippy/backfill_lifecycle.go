package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/civil"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bqcachedclient "github.com/openshift/sippy/pkg/bigquery"
	"github.com/openshift/sippy/pkg/db/lifecyclebackfill"
	"github.com/openshift/sippy/pkg/flags"
)

type BackfillLifecycleFlags struct {
	DBFlags          *flags.PostgresFlags
	BigQueryFlags    *flags.BigQueryFlags
	GoogleCloudFlags *flags.GoogleCloudFlags
	StartDate        string
	EndDate          string
	DryRun           bool
}

func NewBackfillLifecycleFlags() *BackfillLifecycleFlags {
	return &BackfillLifecycleFlags{
		DBFlags:          flags.NewPostgresDatabaseFlags(),
		BigQueryFlags:    flags.NewBigQueryFlags(),
		GoogleCloudFlags: flags.NewGoogleCloudFlags(),
	}
}

func (f *BackfillLifecycleFlags) BindFlags(fs *pflag.FlagSet) {
	f.DBFlags.BindFlags(fs)
	f.BigQueryFlags.BindFlags(fs)
	f.GoogleCloudFlags.BindFlags(fs)
	fs.StringVar(&f.StartDate, "start-date", "", "Start date (YYYY-MM-DD)")
	fs.StringVar(&f.EndDate, "end-date", "", "End date (YYYY-MM-DD)")
	fs.BoolVar(&f.DryRun, "dry-run", false, "Log counts without updating")
}

func NewBackfillLifecycleCommand() *cobra.Command {
	f := NewBackfillLifecycleFlags()

	cmd := &cobra.Command{
		Use:   "backfill-lifecycle",
		Short: "Backfill the lifecycle column on prow_job_run_tests from BigQuery",
		RunE: func(cmd *cobra.Command, args []string) error {
			if f.StartDate == "" {
				return fmt.Errorf("--start-date is required")
			}
			startDate, err := civil.ParseDate(f.StartDate)
			if err != nil {
				return fmt.Errorf("invalid --start-date %q: %w", f.StartDate, err)
			}

			endDate := civil.DateOf(time.Now().UTC())
			if f.EndDate != "" {
				endDate, err = civil.ParseDate(f.EndDate)
				if err != nil {
					return fmt.Errorf("invalid --end-date %q: %w", f.EndDate, err)
				}
			}

			if startDate.After(endDate) {
				return fmt.Errorf("--start-date (%s) is after --end-date (%s)", startDate, endDate)
			}

			dbc, err := f.DBFlags.GetDBClient()
			if err != nil {
				return fmt.Errorf("getting db client: %w", err)
			}

			ctx := context.Background()
			opCtx, ctx := bqcachedclient.OpCtxForCronEnv(ctx, "backfill-lifecycle")
			bqClient, err := f.BigQueryFlags.GetBigQueryClient(
				ctx, opCtx, nil,
				f.GoogleCloudFlags.ServiceAccountCredentialFile,
			)
			if err != nil {
				return fmt.Errorf("getting bigquery client: %w", err)
			}

			log.WithFields(log.Fields{
				"start":   startDate,
				"end":     endDate,
				"dry_run": f.DryRun,
			}).Info("starting lifecycle backfill")

			return lifecyclebackfill.Backfill(ctx, dbc, bqClient, startDate, endDate, f.DryRun)
		},
	}

	f.BindFlags(cmd.Flags())

	return cmd
}
