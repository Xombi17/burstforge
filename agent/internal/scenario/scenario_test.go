package scenario

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const valid = `
name: t
target: { base_url: "http://localhost:9999", environment: local }
workflow:
  steps:
    - { name: login, method: POST, path: /login, capture: { token: $.token } }
    - name: act
      method: POST
      path: /act
      headers: { Authorization: "Bearer {{token}}", X-Id: "{{logical_id}}" }
      body: { who: "u{{n}}" }
      retry: { max_attempts: 3, backoff: exponential, base_delay: 100ms, jitter: full }
traffic: { profile: burst, arrivals: 10, window: 1s }
`

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "s.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadAppliesDefaults(t *testing.T) {
	sc, err := Load(write(t, valid))
	if err != nil {
		t.Fatal(err)
	}
	st := sc.Workflow.Steps[0]
	if st.Timeout.Seconds() != 10 || st.Retry.MaxAttempts != 1 || len(st.Retry.On) != 4 {
		t.Errorf("defaults not applied: %+v", st)
	}
	if sc.Traffic.MaxInflight != 5000 {
		t.Errorf("max_inflight default: %d", sc.Traffic.MaxInflight)
	}
}

func TestValidationReportsAllProblems(t *testing.T) {
	bad := `
target: { base_url: "ftp://x", environment: production }
workflow:
  steps:
    - { name: a, path: nope, retry: { max_attempts: 99 } }
    - { name: a, path: /x, headers: { H: "{{undefined}}" } }
traffic: { profile: burst, arrivals: 0, double_submit: 2 }
`
	_, err := Load(write(t, bad))
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{
		"base_url must be an http(s) URL",
		"production targets are not supported",
		"path must start with /",
		"max_attempts must be 1..10",
		"duplicated",
		"{{undefined}}",
		"arrivals must be > 0",
		"double_submit must be between 0 and 1",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in:\n%v", want, err)
		}
	}
}

func TestPathsCannotChangeTheHost(t *testing.T) {
	bad := valid + `
hooks:
  before: [{ path: "@169.254.169.254/latest" }]
  after: [{ path: "x" }]
observe: { url: "http://elsewhere/stats", counters: [a] }
invariants:
  - { name: i, path: "@evil/x", json: $.a }
`
	_, err := Load(write(t, bad))
	for _, want := range []string{"hooks.before[0].path", "hooks.after[0].path", "observe.url", "invariants[0].path"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in: %v", want, err)
		}
	}
	creds := strings.Replace(valid, "http://localhost:9999", "http://u:p@localhost:9999", 1)
	if _, err := Load(write(t, creds)); err == nil || !strings.Contains(err.Error(), "credentials") {
		t.Errorf("credentials in base_url: %v", err)
	}
}

func TestUnknownFieldsRejected(t *testing.T) {
	if _, err := Load(write(t, valid+"\nrate_limit: 5\n")); err == nil {
		t.Fatal("typos in field names must not be silently ignored")
	}
}

func TestCheckTarget(t *testing.T) {
	cases := []struct {
		url   string
		allow []string
		ok    bool
	}{
		{"http://localhost:8080", nil, true},
		{"http://127.0.0.1:8080", nil, true},
		{"http://10.1.2.3", nil, true},
		{"http://192.168.1.5", nil, true},
		{"https://staging.example.com", nil, false},
		{"https://staging.example.com", []string{"staging.example.com"}, true},
		{"https://api.example.com", []string{"staging.example.com"}, false},
		{"http://8.8.8.8", nil, false},
	}
	for _, c := range cases {
		s := &Scenario{Target: Target{BaseURL: c.url, AllowHosts: c.allow}}
		if err := s.CheckTarget(); (err == nil) != c.ok {
			t.Errorf("%s allow=%v: err=%v, want ok=%v", c.url, c.allow, err, c.ok)
		}
	}
}

func TestRender(t *testing.T) {
	vars := map[string]string{"n": "7", "token": "abc"}
	if got := Render("u{{n}}-{{ token }}-{{missing}}", vars); got != "u7-abc-{{missing}}" {
		t.Errorf("got %q", got)
	}
	in := map[string]any{"a": []any{"{{n}}", map[string]any{"b": "{{token}}"}}, "c": 5}
	out := RenderValue(in, vars).(map[string]any)
	if out["a"].([]any)[0] != "7" || out["a"].([]any)[1].(map[string]any)["b"] != "abc" || out["c"] != 5 {
		t.Errorf("deep render failed: %v", out)
	}
	if in["a"].([]any)[0] != "{{n}}" {
		t.Error("RenderValue must not mutate its input")
	}
}
