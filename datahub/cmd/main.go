package main

import (
	"github.com/containers-ai/karina/datahub/cmd/app"
	"github.com/containers-ai/karina/pkg"
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "datahub",
	Short: pkg.ProjectCodeName + "datahub",
	Long:  "",
}

func init() {
	RootCmd.AddCommand(app.RunCmd)
}

func main() {
	RootCmd.Execute()
}
