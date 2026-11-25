package github

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var acquisitionRepository = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)

func appNoRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// AcquisitionToken is a credential, not a diagnostic receipt. Only a trusted
// supervisor may hand the mint result to the unprivileged acquisition process.
type AcquisitionToken struct {
	Token          string            `json:"token"`
	ExpiresAt      time.Time         `json:"expires_at"`
	AppID          int64             `json:"app_id"`
	InstallationID int64             `json:"installation_id"`
	RepositoryID   int64             `json:"repository_id"`
	Repository     string            `json:"repository"`
	Permissions    map[string]string `json:"permissions"`
}

type AcquisitionApp struct {
	AppID, InstallationID, RepositoryID int64
	Repository                          string
}

func acquisitionPermissions() map[string]string {
	return map[string]string{"metadata": "read", "contents": "read", "issues": "read", "pull_requests": "read"}
}

func readPermissions(p map[string]string) bool {
	if len(p) != 4 {
		return false
	}
	for name := range acquisitionPermissions() {
		if p[name] != "read" {
			return false
		}
	}
	return true
}

func validAcquisitionRepository(repo string) bool {
	_, name, ok := strings.Cut(repo, "/")
	return ok && acquisitionRepository.MatchString(repo) && name != "." && name != ".." && !strings.HasPrefix(strings.ToLower(name), "private-")
}

type appRepository struct {
	ID         int64  `json:"id"`
	FullName   string `json:"full_name"`
	Private    *bool  `json:"private"`
	Visibility string `json:"visibility"`
}

func (r appRepository) matches(app AcquisitionApp) bool {
	return r.ID == app.RepositoryID && strings.EqualFold(r.FullName, app.Repository) && r.Private != nil && !*r.Private && r.Visibility == "public"
}

// MintAcquisitionToken performs one non-retried mint. It never stores a key or
// credential. Callers must keep both the input key and output JSON private.
func MintAcquisitionToken(ctx context.Context, app AcquisitionApp, keyPEM []byte) (AcquisitionToken, error) {
	return mintAcquisitionToken(ctx, app, keyPEM, &http.Client{Timeout: 30 * time.Second, CheckRedirect: appNoRedirect})
}

func mintAcquisitionToken(ctx context.Context, app AcquisitionApp, keyPEM []byte, client *http.Client) (AcquisitionToken, error) {
	var out AcquisitionToken
	if app.AppID <= 0 || app.InstallationID <= 0 || app.RepositoryID <= 0 || !validAcquisitionRepository(app.Repository) {
		return out, errors.New("mint requires exact App, installation and public repository identities")
	}
	jwt, err := acquisitionJWT(app.AppID, keyPEM, time.Now())
	if err != nil {
		return out, err
	}
	owner, _, _ := strings.Cut(app.Repository, "/")
	if err = verifyAcquisitionInstallation(ctx, client, jwt, owner, app); err != nil {
		return out, err
	}
	// Public metadata needs no App credential. Refuse private/unavailable or
	// transferred repositories before issuing even a narrowed token.
	var repository appRepository
	if err = appJSON(ctx, client, "", http.MethodGet, "/repos/"+app.Repository, nil, &repository); err != nil {
		return out, err
	}
	if !repository.matches(app) {
		return out, errors.New("repository is not the admitted public identity")
	}
	return mintRepositoryToken(ctx, client, jwt, app)
}

func verifyAcquisitionInstallation(ctx context.Context, client *http.Client, jwt, owner string, app AcquisitionApp) error {
	var installation struct {
		ID          int64                  `json:"id"`
		AppID       int64                  `json:"app_id"`
		SuspendedAt json.RawMessage        `json:"suspended_at"`
		Account     struct{ Login string } `json:"account"`
	}
	if err := appJSON(ctx, client, jwt, http.MethodGet, fmt.Sprintf("/app/installations/%d", app.InstallationID), nil, &installation); err != nil {
		return err
	}
	if installation.ID != app.InstallationID || installation.AppID != app.AppID || !bytes.Equal(installation.SuspendedAt, []byte("null")) || !strings.EqualFold(installation.Account.Login, owner) {
		return errors.New("GitHub App installation identity, owner or suspension mismatch")
	}
	return nil
}

func mintRepositoryToken(ctx context.Context, client *http.Client, jwt string, app AcquisitionApp) (AcquisitionToken, error) {
	var out AcquisitionToken
	var minted struct {
		Token               string            `json:"token"`
		ExpiresAt           time.Time         `json:"expires_at"`
		Permissions         map[string]string `json:"permissions"`
		RepositorySelection string            `json:"repository_selection"`
		Repositories        []appRepository   `json:"repositories"`
	}
	body := map[string]any{"repository_ids": []int64{app.RepositoryID}, "permissions": acquisitionPermissions()}
	if err := appJSON(ctx, client, jwt, http.MethodPost, fmt.Sprintf("/app/installations/%d/access_tokens", app.InstallationID), body, &minted); err != nil {
		return out, err
	}
	if minted.RepositorySelection != "selected" || len(minted.Repositories) != 1 || !minted.Repositories[0].matches(app) || !readPermissions(minted.Permissions) {
		return out, errors.New("minted token did not retain the exact public repository and read permissions")
	}
	out = AcquisitionToken{minted.Token, minted.ExpiresAt, app.AppID, app.InstallationID, app.RepositoryID, app.Repository, minted.Permissions}
	if err := out.validate(app.Repository, time.Now().Add(2*time.Minute)); err != nil {
		return AcquisitionToken{}, err
	}
	return out, nil
}

func acquisitionJWT(appID int64, raw []byte, now time.Time) (string, error) {
	block, rest := pem.Decode(raw)
	if block == nil || len(bytes.TrimSpace(rest)) != 0 {
		return "", errors.New("expected one RSA private key PEM block")
	}
	var key *rsa.PrivateKey
	if parsed, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		key = parsed
	} else if parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		key, _ = parsed.(*rsa.PrivateKey)
	}
	if key == nil || key.N.BitLen() < 2048 || key.Validate() != nil {
		return "", errors.New("expected a valid RSA private key of at least 2048 bits")
	}
	claims, _ := json.Marshal(map[string]any{"iss": strconv.FormatInt(appID, 10), "iat": now.Add(-time.Minute).Unix(), "exp": now.Add(5 * time.Minute).Unix()})
	unsigned := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." + base64.RawURLEncoding.EncodeToString(claims)
	digest := sha256.Sum256([]byte(unsigned))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", errors.New("GitHub App JWT signing failed")
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// Never include response bodies, bearer values or private transport errors in
// mint diagnostics. A failed/ambiguous mint is not retried by this command.
func appJSON(ctx context.Context, client *http.Client, token, method, path string, body, out any) error {
	_, err := appJSONResponse(ctx, client, token, method, path, body, out)
	return err
}

func appJSONResponse(ctx context.Context, client *http.Client, token, method, path string, body, out any) (http.Header, error) {
	var data []byte
	if body != nil {
		data, _ = json.Marshal(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, "https://api.github.com"+path, bytes.NewReader(data))
	if err != nil {
		return nil, errors.New("invalid App authentication request")
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	copyClient := *client
	copyClient.CheckRedirect = appNoRedirect
	response, err := copyClient.Do(req)
	if err != nil {
		return nil, errors.New("GitHub App authentication transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && !(method == http.MethodPost && response.StatusCode == http.StatusCreated) {
		return nil, fmt.Errorf("GitHub App authentication returned HTTP %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 || json.Unmarshal(raw, out) != nil {
		return nil, errors.New("invalid GitHub App authentication response")
	}
	return response.Header, nil
}

// MaxAcquisitionTokenBytes bounds one private credential envelope.
const MaxAcquisitionTokenBytes = 16 << 10

func DecodeAcquisitionToken(raw []byte, repository string, deadline time.Time) (AcquisitionToken, error) {
	var token AcquisitionToken
	if len(raw) > MaxAcquisitionTokenBytes {
		return token, errors.New("installation credential exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&token) != nil || decoder.Decode(new(any)) != io.EOF {
		return AcquisitionToken{}, errors.New("invalid installation credential JSON")
	}
	if err := token.validate(repository, deadline); err != nil {
		return AcquisitionToken{}, err
	}
	return token, nil
}

func (t AcquisitionToken) validate(repository string, deadline time.Time) error {
	if !validAcquisitionRepository(repository) || t.Repository != repository || t.AppID <= 0 || t.InstallationID <= 0 || t.RepositoryID <= 0 || !readPermissions(t.Permissions) || t.Token == "" || strings.ContainsAny(t.Token, " \t\r\n\x00") {
		return errors.New("installation credential identity or read scope mismatch")
	}
	if deadline.IsZero() || !t.ExpiresAt.After(deadline.Add(time.Minute)) || t.ExpiresAt.After(time.Now().Add(65*time.Minute)) {
		return errors.New("installation token cannot cover invocation deadline; mint a fresh token between cycles")
	}
	return nil
}
