package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	// "strconv"
	"time"

	"cloud.google.com/go/spanner"
	// "google.golang.org/api/iterator"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

type command func(ctx context.Context, w io.Writer, client *spanner.Client) error
type adminCommand func(ctx context.Context, w io.Writer, adminClient *database.DatabaseAdminClient, database string) error

var (
	commands = map[string]command{
		// "write":               write,
		// "read":                read,
		// "query":               query,
		// "update":              update,
		// "querynewcolumn":      queryNewColumn,
		// "pgquerynewcolumn":    pgQueryNewColumn,
		// "querywithparameter":  queryWithParameter,
		// "pgqueryparameter":    pgQueryParameter,
		"dmlwrite":            writeUsingDML,
		// "pgdmlwrite":          pgWriteUsingDML,
		// "dmlwritetxn":         writeWithTransactionUsingDML,
		// "pgdmlwritetxn":       pgWriteWithTransactionUsingDML,
		// "readindex":           readUsingIndex,
		// "readstoringindex":    readStoringIndex,
		// "readonlytransaction": readOnlyTransaction,
	}

	adminCommands = map[string]adminCommand{
		"createdatabase":    createDatabase,
		// "addnewcolumn":      addNewColumn,
		// "pgaddnewcolumn":    pgAddNewColumn,
		// "addstoringindex":   addStoringIndex,
		// "pgaddstoringindex": pgAddStoringIndex,
		// "pgcreatedatabase":  pgCreateDatabase,
	}
)

func createDatabase(ctx context.Context, w io.Writer, adminClient *database.DatabaseAdminClient, db string) error {
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(db)
	if matches == nil || len(matches) != 3 {
		return fmt.Errorf("Invalid database id %s", db)
	}
	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          matches[1],
		CreateStatement: "CREATE DATABASE `" + matches[2] + "`",
		ExtraStatements: []string{
			`CREATE TABLE Singers (
				SingerId   INT64 NOT NULL,
				FirstName  STRING(1024),
				LastName   STRING(1024),
				SingerInfo BYTES(MAX)
			) PRIMARY KEY (SingerId)`,
			`CREATE TABLE Albums (
				SingerId     INT64 NOT NULL,
				AlbumId      INT64 NOT NULL,
				AlbumTitle   STRING(MAX)
			) PRIMARY KEY (SingerId, AlbumId),
			INTERLEAVE IN PARENT Singers ON DELETE CASCADE`,
		},
	})
	if err != nil {
		return err
	}
	if _, err := op.Wait(ctx); err != nil {
		return err
	}
	fmt.Fprintf(w, "Created database [%s]\n", db)
	return nil
}

func writeUsingDML(w io.Writer, db string) error {
	ctx := context.Background()
	client, err = spanner.NewClient(ctx, db)
	if err != nil {
		return err 
	}
	defer client.Close()

	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: `INSERT Singers (SingerId, FirstName, LastName) VALUES
                                (12, 'Melissa', 'Garcia'),
                                (13, 'Russell', 'Morales'),
                                (14, 'Jacqueline', 'Long'),
                                (15, 'Dylan', 'Shaw')`,
		}
		rowConut, err := txn.Update(ctx, stmt)

		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%d record(s) inserted.\n", rowCount)
		return err
	})
	return err
}

func createClients(ctx context.Context, db string) (*database.DatabaseAdminClient, *spanner.Client) {
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Fatal(err)
	}

	dataClient, err := spanner.NewClient(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	return adminClient, dataClient
}

func run(ctx context.Context, adminClient *database.DatabaseAdminClient, dataClient *spanner.Client, w io.Writer, cmd string, db string) error {
	if adminCmdFn := adminCommands[cmd]; adminCmdFn != nil {
		err := adminCmdFn(ctx, w, adminClient, db)
		if err != nil {
			fmt.Fprintf(w, "%s failed with %v", cmd, err)
		}
		return err
	}

	// Normal mode
	cmdFn := commands[cmd]
	if cmdFn == nil {
		flag.Usage()
		os.Exit(2)
	}
	err := cmdFn(ctx, w, dataClient)
	if err != nil {
		fmt.Fprintf(w, "%s failed with %v", cmd, err)
	}
	return err
}



func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: spanner_snippets <command> <database_name>
	Command can be one of: write, read, query, update, querynewcolumn,
		querywithparameter, dmlwrite, dmlwritetxn, readindex, readstoringindex,
		readonlytransaction, createdatabase, addnewcolumn, addstoringindex,
		pgcreatedatabase, pgqueryparameter, pgdmlwrite, pgaddnewcolumn, pgquerynewcolumn,
		pgdmlwritetxn, pgaddstoringindex
Examples:
	spanner_snippets createdatabase projects/my-project/instances/my-instance/databases/example-db
	spanner_snippets write projects/my-project/instances/my-instance/databases/example-db
`)
	}

	flag.Parse()
	if len(flag.Args()) < 2 {
		flag.Usage()
		os.Exit(2)
	}

	cmd, db := flag.Arg(0), flag.Arg(1)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	adminClient, dataClient := createClients(ctx, db)
	defer adminClient.Close()
	defer dataClient.Close()
	if err := run(ctx, adminClient, dataClient, os.Stdout, cmd, db); err != nil {
		os.Exit(1)
	}
}