package auth

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const (
	defaultCookieName   = "ingitdb_github_token"
	defaultCookieSecure = true
)

var defaultScopes = []string{"repo", "read:org", "read:user"}

// Config configures OAuth and shared auth cookie behavior for HTTP API/MCP servers.
type Config struct {
	GitHubClientID     string
	GitHubClientSecret string
	CallbackURL        string
	Scopes             []string
	CookieDomain       string
	CookieName         string
	CookieSecure       bool
	AuthAPIBaseURL     string

	// tokenExchangeURL overrides the GitHub token-exchange endpoint.
	// Leave empty to use the real GitHub URL. Populated only in tests.
	tokenExchangeURL string

	// defaultHTTPClient is used when ExchangeCodeForToken receives a nil httpClient.
	// Leave nil to fall back to http.DefaultClient. Populated only in tests.
	defaultHTTPClient *http.Client
}

// LoadConfigFromEnv loads authentication settings from environment variables.
func LoadConfigFromEnv() Config {
	scopes := parseScopes(os.Getenv("INGITDB_GITHUB_OAUTH_SCOPES"))
	if len(scopes) == 0 {
		scopes = defaultScopes
	}
	cfg := Config{
		GitHubClientID:     strings.TrimSpace(os.Getenv("INGITDB_GITHUB_OAUTH_CLIENT_ID")),
		GitHubClientSecret: strings.TrimSpace(os.Getenv("INGITDB_GITHUB_OAUTH_CLIENT_SECRET")),
		CallbackURL:        strings.TrimSpace(os.Getenv("INGITDB_GITHUB_OAUTH_CALLBACK_URL")),
		CookieDomain:       strings.TrimSpace(os.Getenv("INGITDB_AUTH_COOKIE_DOMAIN")),
		CookieName:         strings.TrimSpace(os.Getenv("INGITDB_AUTH_COOKIE_NAME")),
		AuthAPIBaseURL:     strings.TrimSpace(os.Getenv("INGITDB_AUTH_API_BASE_URL")),
		Scopes:             scopes,
		CookieSecure:       defaultCookieSecure,
	}
	cookieSecure := strings.TrimSpace(os.Getenv("INGITDB_AUTH_COOKIE_SECURE"))
	if cookieSecure != "" {
		secure, err := strconv.ParseBool(cookieSecure)
		if err == nil {
			cfg.CookieSecure = secure
		}
	}
	if cfg.CookieName == "" {
		cfg.CookieName = defaultCookieName
	}
	return cfg
}

func parseScopes(value string) []string {
	cleanValue := strings.ReplaceAll(value, ",", " ")
	parts := strings.Fields(cleanValue)
	if len(parts) == 0 {
		return nil
	}
	scopes := make([]string, 0, len(parts))
	seen := make(map[string]bool, len(parts))
	for _, part := range parts {
		scope := strings.TrimSpace(part)
		if scope == "" || seen[scope] {
			continue
		}
		seen[scope] = true
		scopes = append(scopes, scope)
	}
	return scopes
}

// ValidateForHTTPMode validates required auth settings before HTTP server startup.
func (c Config) ValidateForHTTPMode() error {
	if c.GitHubClientID == "" {
		return fmt.Errorf("INGITDB_GITHUB_OAUTH_CLIENT_ID is required")
	}
	if c.GitHubClientSecret == "" {
		return fmt.Errorf("INGITDB_GITHUB_OAUTH_CLIENT_SECRET is required")
	}
	if c.CallbackURL == "" {
		return fmt.Errorf("INGITDB_GITHUB_OAUTH_CALLBACK_URL is required")
	}
	if c.CookieDomain == "" {
		return fmt.Errorf("INGITDB_AUTH_COOKIE_DOMAIN is required")
	}
	if c.AuthAPIBaseURL == "" {
		return fmt.Errorf("INGITDB_AUTH_API_BASE_URL is required")
	}
	return nil
}
