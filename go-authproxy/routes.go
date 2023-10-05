package main

import (
	"authproxy/pkg/settings"
	"context"
	"github.com/AzureAD/microsoft-authentication-library-for-go/apps/public"
	"github.com/gofiber/fiber/v2"
)

func RegisterRoutes(app *fiber.App) {
	app.Get("/login", func(ctx *fiber.Ctx) error {
		var err error

		var client public.Client
		client, err = public.New(settings.AzureClientId())
		if err != nil {
			return err
		}

		var redirectUri string
		redirectUri, err = client.AuthCodeURL(
			context.TODO(),
			settings.AzureClientId(),
			settings.AzureRedirectUri(),
			[]string{},
		)

		return ctx.Render("login", fiber.Map{
			"authUri": redirectUri,
		})
	})

	app.Get("/callback", func(ctx *fiber.Ctx) error {
		return nil
	})
}
