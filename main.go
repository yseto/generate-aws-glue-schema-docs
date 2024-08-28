package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

var (
	projectName  string
	dirName      string
	databaseName string
)

//go:embed table.tmpl
var tableTmpl string

type tablePad struct {
	DatabaseName           string
	TableName              string
	PartitionKeys, Columns []types.Column
}

//go:embed toc.tmpl
var tocTmpl string

type tocPad struct {
	ProjectName string
	Tables      []table
}

type table struct {
	Name, Link string
	ColumnNum  int
}

func init() {
	flag.StringVar(&projectName, "projectName", "no name", "project name")
	flag.StringVar(&dirName, "outputDir", ".", "output directory name")
	flag.StringVar(&databaseName, "databaseName", "default", "database name")
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	flag.Parse()

	funcs := template.FuncMap{"hasPrefix": strings.HasPrefix}
	tmpl, err := template.New("tmpl").Funcs(funcs).Parse(tableTmpl)
	if err != nil {
		log.Fatalln(err)
	}

	var tables []table
	err = getTables(ctx, func(t types.Table) error {
		filename := fmt.Sprintf("%s.md", *t.Name)

		wr, err := os.Create(filepath.Join(dirName, filename))
		if err != nil {
			return err
		}
		defer wr.Close()

		tables = append(tables, table{Name: *t.Name, Link: filename, ColumnNum: len(t.StorageDescriptor.Columns)})

		return tmpl.Execute(wr, tablePad{
			DatabaseName:  *t.DatabaseName,
			TableName:     *t.Name,
			PartitionKeys: t.PartitionKeys,
			Columns:       t.StorageDescriptor.Columns,
		})
	})
	if err != nil {
		log.Fatalln(err)
	}

	// table of contents.
	toc, err := template.New("tocTmpl").Parse(tocTmpl)
	if err != nil {
		log.Fatalln(err)
	}
	wr, err := os.Create(filepath.Join(dirName, "README.md"))
	if err != nil {
		log.Fatalln(err)
	}
	defer wr.Close()

	err = toc.Execute(wr, tocPad{
		ProjectName: projectName,
		Tables:      tables,
	})
	if err != nil {
		log.Fatalln(err)
	}
}

func getTables(ctx context.Context, fn func(types.Table) error) error {
	awscfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}

	client := glue.NewFromConfig(awscfg)

	scanPaginator := glue.NewSearchTablesPaginator(client, &glue.SearchTablesInput{
		Filters: []types.PropertyPredicate{
			{
				Key:   aws.String("databaseName"),
				Value: aws.String(databaseName),
			},
		},
	})
	var results []types.Table
	for scanPaginator.HasMorePages() {
		response, err := scanPaginator.NextPage(ctx)
		if err != nil {
			return err
		}

		results = append(results, response.TableList...)
	}

	for i := range results {
		err = fn(results[i])
		if err != nil {
			return err
		}
	}

	return nil
}
