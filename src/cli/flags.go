package cli

func (c *RootCommand) initFlags() {
	c.PersistentFlags().StringVarP(
		&c.Options.ConfigPath,
		"config",
		"c",
		"",
		"Path to the .env configuration file",
	)
}
