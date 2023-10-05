package main

import (
	"authproxy/pkg/settings"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/proxy"
	"github.com/gofiber/template/django/v3"
	_ "github.com/joho/godotenv/autoload"
	"log"
	"net/http"
	"time"
)

func main() {
	engine := django.New("./templates", ".html")
	app := fiber.New(fiber.Config{
		ReadTimeout:             5 * time.Second,
		WriteTimeout:            5 * time.Second,
		AppName:                 "Dagster Auth Proxy",
		StreamRequestBody:       true,
		EnableTrustedProxyCheck: false,
		TrustedProxies:          nil,
		EnablePrintRoutes:       true,
		Views:                   engine,
	})
	store := GetStore()

	app.Use(logger.New())

	app.Static("/static", "./static")
	RegisterRoutes(app)

	proxyConfig := proxy.Config{
		Servers: []string{settings.DagsterWebserverUrl()},
	}

	proxyHandler := func(ctx *fiber.Ctx) error {
		sess, err := store.Get(ctx)
		if err != nil {
			return err
		}
		if sess.Get("user") == nil {
			return ctx.Redirect("/login", http.StatusSeeOther)
		}

		return proxy.Balancer(proxyConfig)(ctx)
	}

	app.Get("*", proxyHandler)
	app.Post("*", proxyHandler)
	app.Put("*", proxyHandler)
	app.Patch("*", proxyHandler)
	app.Delete("*", proxyHandler)
	app.Head("*", proxyHandler)
	app.Options("*", proxyHandler)

	log.Fatal(app.Listen("0.0.0.0:3001"))
}
