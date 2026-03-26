/// <reference types="cypress" />

describe("Live budget smoke", () => {
  it("loads the hosted budget page without the application error shell", () => {
    cy.loginWithLocalToken(undefined, "/control-center/budget");
    cy.waitForBudgetWorkspaceLoaded();

    cy.contains("Financial picture at a glance", { timeout: 30000 }).should("be.visible");
    cy.get("[data-cy='budget-summary-card'][data-card-key='income']").should("be.visible");
    cy.get("[data-cy='budget-section'][data-section-key='income']").should("be.visible");
    cy.get("body").should("not.contain.text", "Application error");
    cy.get("body").should("not.contain.text", "Failed to load chunk");
  });
});
