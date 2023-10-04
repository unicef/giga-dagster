package main

import (
	"authproxy/pkg/settings"
	"github.com/gofiber/fiber/v2/middleware/session"
	"github.com/gofiber/storage/memory/v2"
	"time"
)

func GetStore() *session.Store {
	storage := memory.New()
	store := session.New(session.Config{
		Expiration:     1 * time.Hour,
		KeyLookup:      "cookie:session",
		CookiePath:     "/",
		CookieSecure:   settings.InProduction(),
		CookieHTTPOnly: true,
		CookieSameSite: "Strict",
		KeyGenerator:   settings.SecretKey,
		Storage:        storage,
	})
	return store
}
