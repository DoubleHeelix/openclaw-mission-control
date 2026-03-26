/// <reference types="cypress" />

function liveMutationEnabled(): boolean {
  return Cypress.env("LIVE_BUDGET_MUTATION") === 1 || Cypress.env("LIVE_BUDGET_MUTATION") === "1";
}

function parseReviewCount(text: string): number {
  const match = text.match(/(\d+)\s+items/i);
  return match ? Number(match[1]) : 0;
}

describe("Live budget review actions", () => {
  it("removes a resolvable review card from Needs Attention after a live click", function () {
    if (!liveMutationEnabled()) {
      cy.log("Skipping live mutation review test. Set CYPRESS_LIVE_BUDGET_MUTATION=1 to enable.");
      return;
    }

    cy.loginWithLocalToken(undefined, "/control-center/budget");
    cy.waitForBudgetWorkspaceLoaded();

    cy.contains("button", "Items needing confirmation", { timeout: 30000 }).then(($toggle) => {
      const expanded = $toggle.attr("aria-expanded");
      if (expanded === "false") {
        cy.wrap($toggle).click();
      }
    });

    cy.contains("button", "Items needing confirmation", { timeout: 30000 })
      .invoke("text")
      .then((text) => {
        const initialCount = parseReviewCount(text);
        if (initialCount === 0) {
          cy.log("No live review items available for mutation test.");
          return;
        }

        cy.get("[data-cy='budget-review-card']", { timeout: 30000 }).then(($cards) => {
          const candidate = [...$cards].find((card) => {
            const textContent = card.textContent || "";
            const hasKeepDiscretionary = textContent.includes("Keep in variable spending");
            const hasResolvableReason =
              textContent.includes("Low confidence") ||
              textContent.includes("salary like single occurrence") ||
              textContent.includes("large debit unclassified") ||
              textContent.includes("Material cadence ambiguity");
            const hasHardBlocker =
              textContent.includes("Possible duplicate group") ||
              textContent.includes("Parser anomaly") ||
              textContent.includes("Parser leakage") ||
              textContent.includes("Transfer detection needs confirmation");
            return hasKeepDiscretionary && hasResolvableReason && !hasHardBlocker;
          });

          expect(candidate, "live review card candidate").to.exist;

          cy.wrap(candidate!)
            .as("candidateCard")
            .invoke("text")
            .then((cardText) => {
              const merchantLine = cardText
                .split("\n")
                .map((line) => line.trim())
                .find((line) => line && !line.includes("Needs review"));
              expect(merchantLine, "candidate merchant label").to.be.a("string").and.not.be.empty;

              cy.get("@candidateCard").within(() => {
                cy.contains("button", "Keep in variable spending").click();
              });

              cy.contains("button", "Items needing confirmation", { timeout: 30000 })
                .invoke("text")
                .should((updatedText) => {
                  const updatedCount = parseReviewCount(updatedText);
                  expect(updatedCount).to.equal(initialCount - 1);
                });

              cy.get("body").should("not.contain.text", merchantLine as string);
            });
        });
      });
  });
});
