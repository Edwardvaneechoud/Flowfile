/* Search + category filtering for the formula function reference
   (docs/users/formulas/functions.md). No-op on every other page. */

(function () {
  function init() {
    var search = document.querySelector(".fn-search");
    if (!search || search.dataset.bound) return;
    search.dataset.bound = "1";

    var chips = Array.prototype.slice.call(document.querySelectorAll(".fn-chip"));
    var sections = Array.prototype.slice.call(document.querySelectorAll(".fn-category"));
    var noResults = document.querySelector(".fn-no-results");
    var activeCat = "all";

    function apply() {
      var terms = search.value.trim().toLowerCase().split(/\s+/).filter(Boolean);
      var anyVisible = false;
      sections.forEach(function (section) {
        var matchesCat = activeCat === "all" || section.dataset.cat === activeCat;
        var visibleCards = 0;
        section.querySelectorAll(".fn-card").forEach(function (card) {
          var text = card.dataset.text;
          var show =
            matchesCat &&
            terms.every(function (term) {
              return text.indexOf(term) !== -1;
            });
          card.hidden = !show;
          if (show) visibleCards++;
        });
        section.hidden = visibleCards === 0;
        if (visibleCards > 0) anyVisible = true;
      });
      if (noResults) noResults.hidden = anyVisible;
    }

    chips.forEach(function (chip) {
      chip.addEventListener("click", function () {
        chips.forEach(function (other) {
          other.classList.remove("active");
        });
        chip.classList.add("active");
        activeCat = chip.dataset.cat;
        apply();
      });
    });
    search.addEventListener("input", apply);
  }

  if (window.document$) {
    window.document$.subscribe(init);
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
