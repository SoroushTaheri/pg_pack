/*
Copyright © 2023 Soroush Taheri soroushtgh@gmail.com
*/
package cmd

import (
	"fmt"
	"log"
	"os"
	"syscall"

	"github.com/spf13/cobra"

	_ "github.com/lib/pq"
	core "github.com/soroushtaheri/pg_pack/pkg"
	"golang.org/x/term"
)

var cmdCreds core.ConnectionCreds
var cmdOpts core.Options
var cmdOutput string

var rootCmd = &cobra.Command{
	Use:   "pg_pack",
	Short: "Pack your PostgreSQL databases fast and easy",
	Long: `pg_pack is a command-line tool for quickly packing PostgreSQL databases,
outperforming traditional methods like pg_dump, enabling faster backups and migrations`,
	Run: func(cmd *cobra.Command, args []string) {
		if cmdCreds.Password == "" {
			fmt.Printf("Password for user %s: ", cmdCreds.Username)
			passB, err := term.ReadPassword(int(syscall.Stdin))
			if err != nil {
				os.Exit(1)
			}
			cmdCreds.Password = string(passB)
			fmt.Println()
		}

		m, err := core.NewManager(&cmdOutput, &cmdCreds, &cmdOpts)

		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}

		if err := m.Pack(); err != nil {
			log.Fatal(err)
		}
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringVarP(&cmdOutput, "output", "o", "", "Output file")

	rootCmd.Flags().StringVar(&cmdCreds.Host, "host", "localhost", "PostgreSQL host")
	rootCmd.Flags().Int16VarP(&cmdCreds.Port, "port", "p", 5432, "PostgreSQL port")
	rootCmd.Flags().StringVarP(&cmdCreds.Username, "user", "u", "postgres", "PostgreSQL username")
	rootCmd.Flags().StringVar(&cmdCreds.Password, "password", "", "PostgreSQL password")
	rootCmd.Flags().StringVarP(&cmdCreds.Database, "database", "d", "", "PostgreSQL database")
	rootCmd.Flags().BoolVarP(&cmdCreds.SSL, "ssl", "s", false, "Enable 'sslmode' when connecting to the database")

	rootCmd.Flags().BoolVarP(&cmdOpts.Compress, "compress", "c", false, "Compress the final package. If enabled, the final file format will be '.pack' otherwise the standard '.sql'")
	rootCmd.Flags().BoolVarP(&cmdOpts.DataOnly, "data-only", "D", false, "Only pack tables' data records (exclude schemas)")
	rootCmd.Flags().StringVar(&cmdOpts.RecordMode, "record-mode", "copy", "How should pg_pack write data records in the package file. Must be either 'INSERT' (safer) or 'COPY' (faster & lighter). Defaults to 'COPY'")

	rootCmd.MarkFlagFilename("output")
	rootCmd.MarkFlagRequired("output")
	rootCmd.MarkFlagRequired("database")
}
