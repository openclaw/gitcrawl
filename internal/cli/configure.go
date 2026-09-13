package cli

import (
	"errors"
	"flag"
	"io"
	"os"
	"strings"

	"github.com/openclaw/gitcrawl/internal/config"
)

func (a *App) runConfigure(args []string) error {
	fs := flag.NewFlagSet("configure", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	summaryModel := fs.String("summary-model", "", "summary model")
	embedModel := fs.String("embed-model", "", "embedding model")
	embedBaseURLFlag := fs.String("embed-base-url", "", "custom endpoint for the embedding model")
	embeddingBasis := fs.String("embedding-basis", "", "embedding basis")
	jsonOut := fs.Bool("json", false, "write JSON output")
	if err := fs.Parse(normalizeCommandArgs(args, map[string]bool{"summary-model": true, "embed-model": true, "embed-base-url": true, "embedding-basis": true})); err != nil {
		return usageErr(err)
	}
	a.applyCommandJSON(*jsonOut)

	cfg, err := config.Load(a.configPath)
	configExists := true
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		configExists = false
		cfg = config.Default()
	}
	updated := false
	if strings.TrimSpace(*summaryModel) != "" {
		cfg.OpenAI.SummaryModel = strings.TrimSpace(*summaryModel)
		updated = true
	}
	if strings.TrimSpace(*embedModel) != "" {
		cfg.OpenAI.EmbedModel = strings.TrimSpace(*embedModel)
		updated = true
	}
	if flagWasSet(fs, "embed-base-url") {
		cfg.OpenAI.EmbedBaseURL = strings.TrimSpace(*embedBaseURLFlag)
		updated = true
	}
	if strings.TrimSpace(*embeddingBasis) != "" {
		cfg.EmbeddingBasis = strings.TrimSpace(*embeddingBasis)
		updated = true
	}
	if updated || !configExists {
		if err := config.Save(a.configPath, cfg); err != nil {
			return err
		}
	}
	return a.writeOutput("configure", map[string]any{
		"config_path":     config.ResolvePath(a.configPath),
		"updated":         updated || !configExists,
		"summary_model":   cfg.OpenAI.SummaryModel,
		"embed_model":     cfg.OpenAI.EmbedModel,
		"embed_base_url":  embedBaseURL(cfg),
		"embedding_basis": cfg.EmbeddingBasis,
	}, true)
}

func flagWasSet(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(value *flag.Flag) {
		found = found || value.Name == name
	})
	return found
}
