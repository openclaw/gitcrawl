package cli

import "github.com/openclaw/gitcrawl/internal/store"

type scopedSearchHit struct {
	Scope         string  `json:"scope"`
	ThreadID      int64   `json:"thread_id,omitempty"`
	DocumentID    int64   `json:"document_id,omitempty"`
	Number        int     `json:"number,omitempty"`
	Kind          string  `json:"kind"`
	State         string  `json:"state,omitempty"`
	Title         string  `json:"title"`
	Path          string  `json:"path,omitempty"`
	Language      string  `json:"language,omitempty"`
	HTMLURL       string  `json:"html_url,omitempty"`
	AuthorLogin   string  `json:"author_login,omitempty"`
	Snippet       string  `json:"snippet"`
	Score         float64 `json:"score,omitempty"`
	GitSHA        string  `json:"git_sha,omitempty"`
	SourceRoot    string  `json:"source_root,omitempty"`
	WorktreeDirty bool    `json:"worktree_dirty,omitempty"`
}

func mergeScopedSearchHits(threadHits []store.SearchHit, codeHits []store.CodeSearchHit, limit int) []scopedSearchHit {
	if limit <= 0 {
		limit = 20
	}
	out := make([]scopedSearchHit, 0, min(limit, len(threadHits)+len(codeHits)))
	maxLen := max(len(threadHits), len(codeHits))
	for index := 0; index < maxLen && len(out) < limit; index++ {
		if index < len(threadHits) {
			hit := threadHits[index]
			out = append(out, scopedSearchHit{
				Scope:       "threads",
				ThreadID:    hit.ThreadID,
				Number:      hit.Number,
				Kind:        hit.Kind,
				State:       hit.State,
				Title:       hit.Title,
				HTMLURL:     hit.HTMLURL,
				AuthorLogin: hit.AuthorLogin,
				Snippet:     hit.Snippet,
				Score:       hit.Score,
			})
		}
		if index < len(codeHits) && len(out) < limit {
			hit := codeHits[index]
			out = append(out, scopedSearchHit{
				Scope:         "code",
				DocumentID:    hit.DocumentID,
				Kind:          "code",
				Title:         hit.Path,
				Path:          hit.Path,
				Language:      hit.Language,
				Snippet:       hit.Snippet,
				GitSHA:        hit.GitSHA,
				SourceRoot:    hit.SourceRoot,
				WorktreeDirty: hit.WorktreeDirty,
			})
		}
	}
	return out
}
