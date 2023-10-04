package main

import (
	"authproxy/pkg/settings"
	"context"
	msal "github.com/AzureAD/microsoft-authentication-library-for-go/apps/public"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/session"
)

func RegisterRoutes(app *fiber.App) {
	store := GetStore()

	app.Get("/login", func(ctx *fiber.Ctx) error {
		var sess *session.Session
		var err error
		sess, err = store.Get(ctx)
		if err != nil {
			return err
		}

		var client msal.Client
		client, err = msal.New(settings.AzureClientId())
		if err != nil {
			return err
		}

		var accounts []msal.Account
		accounts, err = client.Accounts(context.TODO())
		if err != nil {
			return err
		}

		var result msal.AuthResult
		if len(accounts) > 0 {
			result, err = client.AcquireTokenSilent(context.TODO(), []string{}, msal.WithSilentAccount(accounts[0]))
		}
		if err != nil || len(accounts) == 0 {
			result, err = client.AcquireTokenInteractive(context.TODO(), []string{})
			if err != nil {
				return err
			}
		}

		sess.Set("account", result.Account)
		sess.Set("accessToken", result.AccessToken)
		err = sess.Save()
		if err != nil {
			return err
		}

		return ctx.Render("login", fiber.Map{
			"authUri": "/auth/login",
		})
	})

	app.Get("/callback", func(ctx *fiber.Ctx) error {
		return nil
	})
}
