package settings

import (
	"os"
)

func getEnv(env string) string {
	val, ok := os.LookupEnv(env)
	if !ok {
		return ""
	}
	return val
}

func Env() string {
	env := getEnv("ENV")
	if env == "" {
		return "production"
	}
	return env
}

func InProduction() bool {
	return Env() == "production"
}

func SecretKey() string {
	return getEnv("SECRET_KEY")
}

func DagsterWebserverUrl() string {
	return getEnv("DAGSTER_WEBSERVER_URL")
}

func DagsterWebserverReadOnlyUrl() string {
	return getEnv("DAGSTER_WEBSERVER_READONLY_URL")
}

func AzureTenantId() string {
	return getEnv("AZURE_TENANT_ID")
}

func AzureClientId() string {
	return getEnv("AZURE_CLIENT_ID")
}

func AzureClientSecret() string {
	return getEnv("AZURE_CLIENT_SECRET")
}

func AzureRedirectUri() string {
	return getEnv("AZURE_REDIRECT_URI")
}

func AzureTenantName() string {
	return getEnv("AZURE_TENANT_NAME")
}

func AzureAuthPolicyName() string {
	return getEnv("AZURE_AUTH_POLICY_NAME")
}

func AzureLogoutRedirectUri() string {
	return getEnv("AZURE_LOGOUT_REDIRECT_URI")
}
