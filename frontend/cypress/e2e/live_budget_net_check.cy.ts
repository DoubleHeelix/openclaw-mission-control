/// <reference types="cypress" />

describe('Live budget net check', () => {
  it('shows non-zero net cards on the live budget page', () => {
    cy.loginWithLocalToken(undefined, '/control-center/budget');
    cy.waitForBudgetWorkspaceLoaded();

    cy.get("[data-cy='budget-summary-card'][data-card-key='core-net']")
      .should('be.visible')
      .should(($card) => {
        const text = $card.text().replace(/\s+/g, ' ').trim();
        expect(text).to.not.match(/CORE NET\s*0\.00/i);
      });

    cy.get("[data-cy='budget-summary-card'][data-card-key='observed-net']")
      .should('be.visible')
      .should(($card) => {
        const text = $card.text().replace(/\s+/g, ' ').trim();
        expect(text).to.not.match(/OBSERVED NET\s*0\.00/i);
      });
  });
});
