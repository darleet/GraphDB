package app

import (
	"fmt"

	"github.com/spf13/cobra"
)

func initStart() {
	rootCmd.AddCommand(&cobra.Command{
		Use:   "start",
		Short: "Starts server listening",
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Println("Starting server...")
			return nil
		},
	})
}
