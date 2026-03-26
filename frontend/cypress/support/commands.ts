/// <reference types="cypress" />

type ClerkOtpLoginOptions = {
  clerkOrigin: string;
  email: string;
  otp: string;
};

const APP_LOAD_TIMEOUT_MS = 30_000;
const LOCAL_AUTH_TOKEN_MIN_LENGTH = 50;

function getEnv(name: string, fallback?: string): string {
  const value = Cypress.env(name) as string | undefined;
  if (value) return value;
  if (fallback !== undefined) return fallback;
  throw new Error(
    `Missing Cypress env var ${name}. ` +
      `Set it via CYPRESS_${name}=... in CI/local before running Clerk login tests.`,
  );
}

function clerkOriginFromPublishableKey(): string {
  const key = getEnv("NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY");

  const m = /^pk_(?:test|live)_(.+)$/.exec(key);
  if (!m) throw new Error(`Unexpected Clerk publishable key format: ${key}`);

  const decoded = atob(m[1]);
  const domain = decoded.replace(/\$$/, "");
  const normalized = domain.replace(".clerk.accounts.dev", ".accounts.dev");
  return `https://${normalized}`;
}

function normalizeOrigin(value: string): string {
  try {
    const url = new URL(value);
    return url.origin;
  } catch {
    return value.replace(/\/$/, "");
  }
}

Cypress.Commands.add("waitForAppLoaded", () => {
  cy.get("[data-cy='route-loader']", {
    timeout: APP_LOAD_TIMEOUT_MS,
  }).should("not.exist");

  cy.get("body", { timeout: APP_LOAD_TIMEOUT_MS }).then(($body) => {
    const globalLoader = $body.find("[data-cy='global-loader']");
    if (globalLoader.length > 0) {
      cy.wrap(globalLoader).should("have.attr", "aria-hidden", "true");
    }
  });
});

Cypress.Commands.add("loginWithLocalToken", (token?: string, path = "/control-center/budget-e2e") => {
  const resolvedToken = token ?? getEnv("LOCAL_AUTH_TOKEN", "x".repeat(64));
  if (resolvedToken.length < LOCAL_AUTH_TOKEN_MIN_LENGTH) {
    throw new Error(`LOCAL_AUTH_TOKEN must be at least ${LOCAL_AUTH_TOKEN_MIN_LENGTH} characters.`);
  }

  cy.visit(path, {
    onBeforeLoad(win) {
      win.sessionStorage.setItem("mc_local_auth_token", resolvedToken);
      win.localStorage.setItem("mc_local_auth_token", resolvedToken);
    },
  });

  cy.get("body", { timeout: APP_LOAD_TIMEOUT_MS }).then(($body) => {
    if ($body.text().includes("Local Authentication")) {
      cy.window().then((win) => {
        win.sessionStorage.setItem("mc_local_auth_token", resolvedToken);
        win.localStorage.setItem("mc_local_auth_token", resolvedToken);
      });
      cy.get("input").first().clear().type(resolvedToken, { log: false });
      cy.contains("button", "Enter Mission Control").click();
    }
  });

  cy.location("pathname", { timeout: APP_LOAD_TIMEOUT_MS }).should("eq", path);
});

Cypress.Commands.add("waitForBudgetWorkspaceLoaded", () => {
  cy.waitForAppLoaded();
  cy.get("[data-cy='budget-workspace-root']", {
    timeout: APP_LOAD_TIMEOUT_MS,
  }).should("be.visible");
  cy.get("body", { timeout: APP_LOAD_TIMEOUT_MS }).should(($body) => {
    const hasReview = $body.text().includes("Needs Attention");
    const hasOverview = $body.text().includes("Financial picture at a glance");
    const hasSections = $body.find("[data-cy='budget-section']").length > 0;
    expect(hasReview || hasOverview || hasSections, "budget workspace ready").to.equal(true);
  });
});

Cypress.Commands.add("loginWithClerkOtp", () => {
  const clerkOrigin = normalizeOrigin(
    getEnv("CLERK_ORIGIN", clerkOriginFromPublishableKey()),
  );
  const email = getEnv("CLERK_TEST_EMAIL", "jane+clerk_test@example.com");
  const otp = getEnv("CLERK_TEST_OTP", "424242");

  const opts: ClerkOtpLoginOptions = { clerkOrigin, email, otp };

  cy.visit("/sign-in");

  const emailSelector =
    'input[type="email"], input[name="identifier"], input[autocomplete="email"]';
  const otpSelector =
    'input[autocomplete="one-time-code"], input[name*="code"], input[name^="code"], input[name^="code."], input[inputmode="numeric"]';
  const continueSelector = 'button[type="submit"], button';
  const methodSelector = /email|code|otp|send code|verification|verify|use email/i;

  const fillEmailStep = (email: string) => {
    cy.get(emailSelector, { timeout: 20_000 })
      .first()
      .clear()
      .type(email, { delay: 10 });

    cy.contains(continueSelector, /continue|sign in|send|next/i, { timeout: 20_000 })
      .should("be.visible")
      .click({ force: true });
  };

  const maybeSelectEmailCodeMethod = () => {
    cy.get("body").then(($body) => {
      const hasOtp = $body.find(otpSelector).length > 0;
      if (hasOtp) return;

      const candidates = $body
        .find("button,a")
        .toArray()
        .filter((el) => methodSelector.test((el.textContent || "").trim()));

      if (candidates.length > 0) {
        cy.wrap(candidates[0]).click({ force: true });
      }
    });
  };

  const waitForOtpOrMethod = () => {
    cy.get("body", { timeout: 60_000 }).should(($body) => {
      const hasOtp = $body.find(otpSelector).length > 0;
      const hasMethod = $body
        .find("button,a")
        .toArray()
        .some((el) => methodSelector.test((el.textContent || "").trim()));
      expect(
        hasOtp || hasMethod,
        "waiting for OTP input or verification method UI",
      ).to.equal(true);
    });
  };

  fillEmailStep(opts.email);

  cy.location("origin", { timeout: 60_000 }).then((origin) => {
    const current = normalizeOrigin(origin);
    if (current === opts.clerkOrigin) {
      cy.origin(
        opts.clerkOrigin,
        { args: { otp: opts.otp } },
        ({ otp }) => {
          const otpSelector =
            'input[autocomplete="one-time-code"], input[name*="code"], input[name^="code"], input[name^="code."], input[inputmode="numeric"]';
          const continueSelector = 'button[type="submit"], button';
          const methodSelector = /email|code|otp|send code|verification|verify|use email/i;

          const maybeSelectEmailCodeMethod = () => {
            cy.get("body").then(($body) => {
              const hasOtp = $body.find(otpSelector).length > 0;
              if (hasOtp) return;

              const candidates = $body
                .find("button,a")
                .toArray()
                .filter((el) => methodSelector.test((el.textContent || "").trim()));

              if (candidates.length > 0) {
                cy.wrap(candidates[0]).click({ force: true });
              }
            });
          };

          const waitForOtpOrMethod = () => {
            cy.get("body", { timeout: 60_000 }).should(($body) => {
              const hasOtp = $body.find(otpSelector).length > 0;
              const hasMethod = $body
                .find("button,a")
                .toArray()
                .some((el) => methodSelector.test((el.textContent || "").trim()));
              expect(
                hasOtp || hasMethod,
                "waiting for OTP input or verification method UI",
              ).to.equal(true);
            });
          };

          waitForOtpOrMethod();
          maybeSelectEmailCodeMethod();

          cy.get(otpSelector, { timeout: 60_000 }).first().clear().type(otp, { delay: 10 });

          cy.get("body").then(($body) => {
            const hasSubmit = $body
              .find(continueSelector)
              .toArray()
              .some((el) => /verify|continue|sign in|confirm/i.test(el.textContent || ""));
            if (hasSubmit) {
              cy.contains(continueSelector, /verify|continue|sign in|confirm/i, { timeout: 20_000 })
                .should("be.visible")
                .click({ force: true });
            }
          });
        },
      );
      return;
    }

    const fillOtpAndSubmit = (otp: string) => {
      waitForOtpOrMethod();
      maybeSelectEmailCodeMethod();

      cy.get(otpSelector, { timeout: 60_000 }).first().clear().type(otp, { delay: 10 });

      cy.get("body").then(($body) => {
        const hasSubmit = $body
          .find(continueSelector)
          .toArray()
          .some((el) => /verify|continue|sign in|confirm/i.test(el.textContent || ""));
        if (hasSubmit) {
          cy.contains(continueSelector, /verify|continue|sign in|confirm/i, { timeout: 20_000 })
            .should("be.visible")
            .click({ force: true });
        }
      });
    };

    fillOtpAndSubmit(opts.otp);
  });

  cy.waitForAppLoaded();
});

declare global {
  namespace Cypress {
    interface Chainable {
      waitForAppLoaded(): Chainable<void>;
      loginWithLocalToken(token?: string, path?: string): Chainable<void>;
      waitForBudgetWorkspaceLoaded(): Chainable<void>;
      loginWithClerkOtp(): Chainable<void>;
    }
  }
}

export {};
