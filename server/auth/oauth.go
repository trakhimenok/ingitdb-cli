package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	Error       string `json:"error"`
	Description string `json:"error_description"`
}

// AuthorizeURL returns GitHub OAuth authorize URL.
func (c Config) AuthorizeURL(state string) string {
	values := url.Values{}
	values.Set("client_id", c.GitHubClientID)
	values.Set("redirect_uri", c.CallbackURL)
	values.Set("scope", strings.Join(c.Scopes, " "))
	values.Set("state", state)
	return "https://github.com/login/oauth/authorize?" + values.Encode()
}

// ExchangeCodeForToken exchanges OAuth code for a GitHub access token.
func (c Config) ExchangeCodeForToken(ctx context.Context, code string, httpClient *http.Client) (string, error) {
	if strings.TrimSpace(code) == "" {
		return "", fmt.Errorf("code is required")
	}
	values := url.Values{}
	values.Set("client_id", c.GitHubClientID)
	values.Set("client_secret", c.GitHubClientSecret)
	values.Set("code", code)
	values.Set("redirect_uri", c.CallbackURL)

	exchangeURL := c.tokenExchangeURL
	if exchangeURL == "" {
		exchangeURL = "https://github.com/login/oauth/access_token"
	}

	requestBody := strings.NewReader(values.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, exchangeURL, requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to build token request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := httpClient
	if client == nil {
		if c.defaultHTTPClient != nil {
			client = c.defaultHTTPClient
		} else {
			client = http.DefaultClient
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("token exchange request failed: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	var payload tokenResponse
	if err = json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("failed to decode token exchange response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token exchange failed with status %d", resp.StatusCode)
	}
	if payload.Error != "" {
		return "", fmt.Errorf("token exchange failed: %s (%s)", payload.Error, payload.Description)
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return "", fmt.Errorf("token exchange response did not include access_token")
	}
	return payload.AccessToken, nil
}
